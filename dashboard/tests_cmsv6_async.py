import datetime
import urllib.error
import re
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, Mock, patch

from django.test import SimpleTestCase, TestCase

from dashboard.models import Camion, EstadoVideo, Turno, Video
from dashboard.services import cmsv6_downloader
from dashboard.services.mdvr_weekly_status import resumen_semana_mdvr
from dashboard.tasks import _rango_semanal_mdvr, importar_videos_mdvr_task


class _CMSV6ConfigStub:
    device_id = "4462510196"
    output_dir = ""
    small_response_bytes = 8
    day_download_max_attempts = 3
    day_download_retry_wait_secs = 0

    def validate(self):
        return None


class _CMSV6RangeTestServer:
    def __init__(self, data: bytes, *, ignore_range=False):
        self.data = data
        self.ignore_range = ignore_range
        self.requests = []

    def __enter__(self):
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                range_header = self.headers.get("Range")
                parent.requests.append({"query": parsed.query, "range": range_header})

                if range_header and not parent.ignore_range:
                    match = re.match(r"bytes=(\d+)-", range_header)
                    start = int(match.group(1)) if match else 0
                    body = parent.data[start:]
                    self.send_response(206)
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Content-Range", f"bytes {start}-{len(parent.data) - 1}/{len(parent.data)}")
                else:
                    query = parse_qs(parsed.query)
                    file_len = int(query.get("FLENGTH", [len(parent.data)])[0])
                    body = parent.data[:file_len]
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))

                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                return None

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.url = f"http://{host}:{port}/download?DownType=3&FOFFSET=0&FLENGTH={len(self.data)}&jsession=old"
        return self

    def __exit__(self, *_exc):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()


class CMSV6AsyncPipelineTests(SimpleTestCase):
    def test_login_conserva_sesion_api_si_login_web_responde_404(self):
        config = cmsv6_downloader.CMSV6Config(
            base_url="http://cmsv6.test",
            account="cuenta",
            password="clave",
            device_id="4462510196",
            output_dir="",
        )
        session = cmsv6_downloader.CMSV6Session(config)
        api_response = MagicMock()
        api_response.read.return_value = b'{"result":0,"jsession":"api-session"}'
        api_response.__enter__.return_value = api_response
        web_error = urllib.error.HTTPError(
            "http://cmsv6.test/808gps/login.html",
            404,
            "Not Found",
            {},
            None,
        )
        session.opener.open = Mock(side_effect=[api_response, web_error])

        session.login()

        self.assertEqual(session.jsession, "api-session")
        self.assertIsNone(session.sid)
        self.assertFalse(session.web_login_ok)
        self.assertEqual(session.web_login_response["http_status"], 404)
        self.assertEqual(
            session.web_login_response["fase"],
            "login_web_secundario",
        )

    def test_rango_semanal_mdvr_usa_solo_semana_actual(self):
        with patch(
            "dashboard.tasks.timezone.localdate",
            return_value=datetime.date(2026, 5, 13),
        ):
            self.assertEqual(
                _rango_semanal_mdvr(),
                (datetime.date(2026, 5, 11), datetime.date(2026, 5, 13)),
            )
            self.assertEqual(
                _rango_semanal_mdvr(incluir_futuro=True),
                (datetime.date(2026, 5, 11), datetime.date(2026, 5, 17)),
            )

    def test_ejecutar_rango_notifica_cada_dia_completado(self):
        session = Mock()
        session.get_gps.return_value = []
        session.get_track.return_value = []
        session.get_alarms.return_value = []
        session.get_video_files.return_value = []

        resumen_dia = {"descargados": 1, "omitidos": 2, "errores": 0, "total": 3}
        callbacks = []

        with tempfile.TemporaryDirectory() as tmp_dir, patch(
            "dashboard.services.cmsv6_downloader.CMSV6Session",
            return_value=session,
        ), patch(
            "dashboard.services.cmsv6_downloader._descargar_videos_dia",
            return_value=resumen_dia,
        ):
            cmsv6_downloader.ejecutar_rango(
                tmp_dir,
                datetime.datetime(2026, 5, 4, 0, 0),
                datetime.datetime(2026, 5, 4, 23, 59),
                lambda _mensaje: None,
                lambda _progress, _mensaje: None,
                opts={"excel_ruta": False, "excel_alarmas": False, "videos": True},
                config=_CMSV6ConfigStub(),
                on_day_complete=lambda dia, resumen: callbacks.append((dia, resumen)),
            )

        self.assertEqual(
            callbacks,
            [(datetime.date(2026, 5, 4), resumen_dia)],
        )

    def test_ejecutar_rango_reintenta_mismo_dia_antes_de_avanzar(self):
        session = Mock()
        session.get_gps.return_value = []
        session.get_track.return_value = []
        session.get_alarms.return_value = []
        session.get_video_files.return_value = [{"file": "uno.h264"}]

        config = _CMSV6ConfigStub()
        resumen_fallido = {"descargados": 0, "omitidos": 0, "errores": 1, "total": 1}
        resumen_ok = {"descargados": 1, "omitidos": 0, "errores": 0, "total": 1}

        with tempfile.TemporaryDirectory() as tmp_dir, patch(
            "dashboard.services.cmsv6_downloader.CMSV6Session",
            return_value=session,
        ), patch(
            "dashboard.services.cmsv6_downloader._descargar_videos_dia",
            side_effect=[resumen_fallido, resumen_ok],
        ) as descargar:
            resultado = cmsv6_downloader.ejecutar_rango(
                tmp_dir,
                datetime.datetime(2026, 5, 4, 0, 0),
                datetime.datetime(2026, 5, 4, 23, 59),
                lambda _mensaje: None,
                lambda _progress, _mensaje: None,
                opts={"excel_ruta": False, "excel_alarmas": False, "videos": True},
                config=config,
            )

        self.assertEqual(descargar.call_count, 2)
        self.assertEqual(resultado["videos_descargados"], 1)
        self.assertEqual(resultado["videos_errores"], 0)

    def test_ejecutar_rango_no_avanza_si_dia_no_completa(self):
        session = Mock()
        session.get_gps.return_value = []
        session.get_track.return_value = []
        session.get_alarms.return_value = []
        session.get_video_files.return_value = [{"file": "uno.h264"}]

        class Config(_CMSV6ConfigStub):
            day_download_max_attempts = 2

        resumen_fallido = {"descargados": 0, "omitidos": 0, "errores": 1, "total": 1}

        with tempfile.TemporaryDirectory() as tmp_dir, patch(
            "dashboard.services.cmsv6_downloader.CMSV6Session",
            return_value=session,
        ), patch(
            "dashboard.services.cmsv6_downloader._descargar_videos_dia",
            return_value=resumen_fallido,
        ) as descargar, self.assertRaises(RuntimeError):
            cmsv6_downloader.ejecutar_rango(
                tmp_dir,
                datetime.datetime(2026, 5, 4, 0, 0),
                datetime.datetime(2026, 5, 5, 23, 59),
                lambda _mensaje: None,
                lambda _progress, _mensaje: None,
                opts={"excel_ruta": False, "excel_alarmas": False, "videos": True},
                config=Config(),
            )

        self.assertEqual(descargar.call_count, 2)
        session.get_video_files.assert_called_once()

    def test_importar_videos_mdvr_task_permite_base_dir(self):
        with patch(
            "dashboard.tasks.importar_videos_mdvr",
            return_value={"ok": True},
        ) as importar:
            resultado = importar_videos_mdvr_task.run(
                importar_velocidades=False,
                fecha_objetivo="2026-05-04",
                base_dir="/tmp/cmsv6_output",
            )

        self.assertEqual(resultado, {"ok": True})
        importar.assert_called_once_with(
            base_dir="/tmp/cmsv6_output",
            importar_velocidades=False,
            fecha_objetivo="2026-05-04",
        )

    def test_importar_videos_mdvr_periodica_se_omite_si_monitor_activo(self):
        monitor = {"id": "cmsv6-monitor-mdvr-semanal", "origen": "active"}
        with patch(
            "dashboard.tasks._monitor_mdvr_activo_en_workers",
            return_value=monitor,
        ), patch("dashboard.tasks.importar_videos_mdvr") as importar:
            resultado = importar_videos_mdvr_task.run(
                omitir_si_monitor_activo=True,
            )

        self.assertTrue(resultado["skipped"])
        self.assertEqual(resultado["reason"], "monitor_mdvr_semanal_activo")
        self.assertEqual(resultado["worker_task"], monitor)
        importar.assert_not_called()

    def test_download_file_reanuda_con_http_range_sin_usar_foffset(self):
        data = bytes(index % 251 for index in range(180_000))
        offset = 70_000
        config = cmsv6_downloader.CMSV6Config(
            base_url="",
            account="",
            password="",
            device_id="4462510196",
            output_dir="",
            small_response_bytes=8,
            min_speed_window_secs=9999,
        )
        session = cmsv6_downloader.CMSV6Session(config)
        session.jsession = "new-session"

        with tempfile.TemporaryDirectory() as tmp_dir, _CMSV6RangeTestServer(data) as server:
            dest = Path(tmp_dir) / "video.tmp"
            dest.write_bytes(data[:offset])

            downloaded = session.download_file(
                server.url,
                dest,
                max_retries=1,
                dl_timeout=5,
                stall_secs=5,
            )

            self.assertEqual(downloaded, len(data))
            self.assertEqual(dest.read_bytes(), data)
            self.assertTrue(any(request["range"] == f"bytes={offset}-" for request in server.requests))
            self.assertFalse(any(f"FOFFSET={offset}" in request["query"] for request in server.requests))

    def test_download_file_no_appendea_si_range_es_ignorado(self):
        data = bytes(index % 251 for index in range(180_000))
        offset = 70_000
        config = cmsv6_downloader.CMSV6Config(
            base_url="",
            account="",
            password="",
            device_id="4462510196",
            output_dir="",
            small_response_bytes=8,
            min_speed_window_secs=9999,
        )
        session = cmsv6_downloader.CMSV6Session(config)
        session.jsession = "new-session"

        with tempfile.TemporaryDirectory() as tmp_dir, _CMSV6RangeTestServer(data, ignore_range=True) as server:
            dest = Path(tmp_dir) / "video.tmp"
            dest.write_bytes(data[:offset])

            with self.assertRaises(Exception):
                session.download_file(
                    server.url,
                    dest,
                    max_retries=1,
                    dl_timeout=5,
                    stall_secs=5,
                )

            self.assertEqual(dest.read_bytes(), data[:offset])

    def test_descarga_suficiente_rechaza_archivo_duplicado(self):
        config = _CMSV6ConfigStub()
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = Path(tmp_dir) / "video.tmp"
            dest.write_bytes(b"x" * 210)

            self.assertFalse(cmsv6_downloader._descarga_suficiente(dest, 100, config))


class MDVRWeeklyStatusTests(TestCase):
    def test_resumen_semana_muestra_descargados_parciales_y_procesados(self):
        camion = Camion.objects.create(
            patente="AA-BB-11",
            carpeta_id="4462510196",
        )
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 5, 14),
            tipo_turno="noche",
            id_camion=camion,
        )
        Video.objects.create(
            nombre="MDVR_4462510196_2026-05-14_noche_C1",
            camara=1,
            ruta_archivo="videos/procesado.mp4",
            id_turno=turno,
            estado=EstadoVideo.LISTO,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            carpeta_dia = Path(tmp_dir) / "4462510196(4462510196)" / "2026-05-14"
            carpeta_dia.mkdir(parents=True)
            (carpeta_dia / "4462510196-260514-000000-010000-20010100.mp4").write_bytes(
                b"x" * 1024
            )
            (carpeta_dia / "4462510196-260514-000000-010000-20010200.tmp").write_bytes(
                b"y" * 512
            )

            resumen = resumen_semana_mdvr(
                output_dir=tmp_dir,
                desde="2026-05-14",
                hasta="2026-05-14",
            )

        filas = {
            (item["turno"], item["camara"]): item
            for item in resumen["resultados"]
        }
        self.assertEqual(filas[("noche", 1)]["estado_resumen"], "procesado")
        self.assertEqual(filas[("noche", 1)]["descarga"]["completos"], 1)
        self.assertEqual(filas[("noche", 2)]["estado_resumen"], "descarga_parcial")
        self.assertEqual(filas[("noche", 2)]["descarga"]["parciales"], 1)
        self.assertEqual(resumen["totales"]["archivos_descargados"], 1)
        self.assertEqual(resumen["totales"]["archivos_parciales"], 1)
