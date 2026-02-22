import datetime
import os
import subprocess
import tempfile
from collections import namedtuple
from types import SimpleNamespace
from unittest.mock import mock_open, patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.test import APIRequestFactory, force_authenticate

from dashboard.models import (
    Camion,
    EstadoVelocidadesVideo,
    EstadoVideo,
    Incidente,
    TipoTurnoChoices,
    Turno,
    VelocidadTurno,
    Video,
)
from dashboard.services.importar_velocidades_csv import importar_velocidades_tabulares
from dashboard.services.importar_videos_mdvr import (
    SegmentoVideo,
    _archivo_listo_para_importar,
    _actualizar_estado_velocidades,
    _importar_camion_mdvr,
    _calcular_backoff_reintento,
    _alinear_duraciones,
    _concat_h264_transcodificando,
    _concatenar_segmentos,
    _es_error_transitorio,
    _puede_reprocesarse,
    _segmento_desde_archivo,
)
from dashboard.views import EspacioDiscoViewSet, IncidenteViewSet, _listar_montajes_disponibles


class SegmentoDesdeArchivoTests(SimpleTestCase):
    def test_soporta_formato_mdvr_legacy(self):
        segmento = _segmento_desde_archivo(
            "/tmp/201-01-114614-120114-10p000.h264",
            datetime.date(2026, 1, 15),
        )
        self.assertIsNotNone(segmento)
        self.assertEqual(segmento.camara, 1)
        self.assertEqual(segmento.inicio_dt.time(), datetime.time(11, 46, 14))
        self.assertEqual(segmento.fin_dt.time(), datetime.time(12, 1, 14))

    def test_soporta_formato_grec_nuevo_con_datos_completos(self):
        segmento = _segmento_desde_archivo(
            "/tmp/4462510196-260202-041706-051706-20010300.grec",
            datetime.date(2026, 2, 2),
        )
        self.assertIsNotNone(segmento)
        self.assertEqual(segmento.camara, 3)
        self.assertEqual(segmento.inicio_dt.time(), datetime.time(4, 17, 6))
        self.assertEqual(segmento.fin_dt.time(), datetime.time(5, 17, 6))

    def test_soporta_formato_mdvr_legacy_en_mp4(self):
        segmento = _segmento_desde_archivo(
            "/tmp/201-01-114614-120114-10p000.mp4",
            datetime.date(2026, 1, 15),
        )
        self.assertIsNotNone(segmento)
        self.assertEqual(segmento.camara, 1)
        self.assertEqual(segmento.inicio_dt.time(), datetime.time(11, 46, 14))
        self.assertEqual(segmento.fin_dt.time(), datetime.time(12, 1, 14))

    def test_soporta_formato_grec_nuevo_en_mp4(self):
        segmento = _segmento_desde_archivo(
            "/tmp/4462510196-260202-041706-051706-20010300.mp4",
            datetime.date(2026, 2, 2),
        )
        self.assertIsNotNone(segmento)
        self.assertEqual(segmento.camara, 3)
        self.assertEqual(segmento.inicio_dt.time(), datetime.time(4, 17, 6))
        self.assertEqual(segmento.fin_dt.time(), datetime.time(5, 17, 6))

    def test_formato_grec_nuevo_se_descarta_si_fecha_no_coincide(self):
        segmento = _segmento_desde_archivo(
            "/tmp/4462510196-260202-041706-051706-20010300.grec",
            datetime.date(2026, 2, 3),
        )
        self.assertIsNone(segmento)

    def test_formato_grec_nuevo_se_descarta_si_camara_no_es_valida(self):
        segmento = _segmento_desde_archivo(
            "/tmp/4462510196-260202-041706-051706-20019900.grec",
            datetime.date(2026, 2, 2),
        )
        self.assertIsNone(segmento)


class ArchivoListoMdvrTests(SimpleTestCase):
    def test_archivo_reciente_se_omite(self):
        stat_fake = SimpleNamespace(st_size=1024, st_mtime=1000.0)
        with patch(
            "dashboard.services.importar_videos_mdvr.MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS",
            180,
        ), patch(
            "dashboard.services.importar_videos_mdvr.os.stat",
            return_value=stat_fake,
        ), patch(
            "dashboard.services.importar_videos_mdvr.time.time",
            return_value=1010.0,
        ):
            ok, motivo = _archivo_listo_para_importar("/tmp/video.mp4")
        self.assertFalse(ok)
        self.assertIn("archivo en subida o reciente", motivo or "")

    def test_archivo_antiguo_se_procesa(self):
        stat_fake = SimpleNamespace(st_size=2048, st_mtime=1000.0)
        with patch(
            "dashboard.services.importar_videos_mdvr.MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS",
            60,
        ), patch(
            "dashboard.services.importar_videos_mdvr.os.stat",
            return_value=stat_fake,
        ), patch(
            "dashboard.services.importar_videos_mdvr.time.time",
            return_value=1200.0,
        ):
            ok, motivo = _archivo_listo_para_importar("/tmp/video.mp4")
        self.assertTrue(ok)
        self.assertIsNone(motivo)

    def test_archivo_inaccesible_se_omite(self):
        with patch(
            "dashboard.services.importar_videos_mdvr.os.stat",
            side_effect=OSError("permiso denegado"),
        ):
            ok, motivo = _archivo_listo_para_importar("/tmp/video.mp4")
        self.assertFalse(ok)
        self.assertIn("no se pudo leer metadatos de archivo", motivo or "")


class ImportarMdvrBackfillVelocidadesTests(TestCase):
    def test_video_listo_pendiente_reintenta_carga_xlsx(self):
        camion = Camion.objects.create(
            patente="BKCD11",
            carpeta_id="4462510196",
        )
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="MDVR_4462510196_2026-02-18_manana_C3",
            camara=3,
            ruta_archivo="videos/existente.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 47)),
            fecha_subida=datetime.date(2026, 2, 19),
            inicio_timestamp=datetime.time(8, 0, 47),
            estado=EstadoVideo.LISTO,
            estado_velocidades=EstadoVelocidadesVideo.PENDIENTE,
            id_turno=turno,
        )

        with tempfile.TemporaryDirectory() as base_dir:
            carpeta_mdvr = os.path.join(base_dir, "4462510196(4462510196)")
            carpeta_dia = os.path.join(carpeta_mdvr, "2026-02-18")
            os.makedirs(carpeta_dia, exist_ok=True)

            segmento = os.path.join(
                carpeta_dia,
                "4462510196-260218-080047-080047-20010300.mp4",
            )
            with open(segmento, "wb") as fh:
                fh.write(b"dummy-segment")

            ruta_xlsx = os.path.join(
                base_dir,
                "4462510196 2026-02-18 00-00-00~2026-02-18 23-59-59.xlsx",
            )
            with open(ruta_xlsx, "wb") as fh:
                fh.write(b"dummy-xlsx")

            with patch(
                "dashboard.services.importar_videos_mdvr.MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS",
                0,
            ), patch(
                "dashboard.services.importar_videos_mdvr.importar_velocidades_xlsx",
                return_value={"guardadas": 1},
            ) as importar_mock, patch(
                "dashboard.services.importar_videos_mdvr.procesar_video_subida"
            ) as procesar_video_mock:
                detalle = _importar_camion_mdvr(
                    camion=camion,
                    base_dir=base_dir,
                    importar_velocidades=True,
                    fecha_objetivo=datetime.date(2026, 2, 18),
                )

        self.assertEqual(detalle["videos_creados"], 0)
        self.assertEqual(importar_mock.call_count, 1)
        self.assertEqual(importar_mock.call_args.args[0].id, video.id)
        procesar_video_mock.assert_not_called()

        video.refresh_from_db()
        self.assertEqual(video.estado_velocidades, EstadoVelocidadesVideo.IMPORTADA)
        self.assertEqual(video.velocidades_error, "")
        self.assertIsNotNone(video.velocidades_actualizadas_en)

    def test_video_listo_sin_xlsx_no_reintenta(self):
        camion = Camion.objects.create(
            patente="BKCD12",
            carpeta_id="4462510197",
        )
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="MDVR_4462510197_2026-02-18_manana_C3",
            camara=3,
            ruta_archivo="videos/existente.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 47)),
            fecha_subida=datetime.date(2026, 2, 19),
            inicio_timestamp=datetime.time(8, 0, 47),
            estado=EstadoVideo.LISTO,
            estado_velocidades=EstadoVelocidadesVideo.SIN_XLSX,
            velocidades_error="No se encontró XLSX asociado para este video.",
            id_turno=turno,
        )

        with tempfile.TemporaryDirectory() as base_dir:
            carpeta_mdvr = os.path.join(base_dir, "4462510197(4462510197)")
            carpeta_dia = os.path.join(carpeta_mdvr, "2026-02-18")
            os.makedirs(carpeta_dia, exist_ok=True)

            segmento = os.path.join(
                carpeta_dia,
                "4462510197-260218-080047-080047-20010300.mp4",
            )
            with open(segmento, "wb") as fh:
                fh.write(b"dummy-segment")

            with patch(
                "dashboard.services.importar_videos_mdvr.MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS",
                0,
            ), patch(
                "dashboard.services.importar_videos_mdvr.importar_velocidades_xlsx",
                return_value={"guardadas": 1},
            ) as importar_mock:
                detalle = _importar_camion_mdvr(
                    camion=camion,
                    base_dir=base_dir,
                    importar_velocidades=True,
                    fecha_objetivo=datetime.date(2026, 2, 18),
                )

        self.assertEqual(detalle["videos_creados"], 0)
        self.assertEqual(importar_mock.call_count, 0)
        video.refresh_from_db()
        self.assertEqual(video.estado_velocidades, EstadoVelocidadesVideo.SIN_XLSX)


class EspacioDiscoMontajesTests(SimpleTestCase):
    def setUp(self):
        self.factory = APIRequestFactory()

    def test_listar_montajes_disponibles_filtra_virtuales(self):
        contenido = "\n".join(
            [
                "/dev/sda1 / ext4 rw,relatime 0 0",
                "/dev/sdb1 /media/usb\\040drive ext4 rw,relatime 0 0",
                "/dev/loop0 /snap/core/17123 squashfs ro,relatime 0 0",
                "tmpfs /run tmpfs rw,nosuid,nodev 0 0",
                "//192.168.0.10/share /data/windows_share cifs rw,relatime 0 0",
            ]
        )
        with patch("builtins.open", mock_open(read_data=contenido)), patch(
            "dashboard.views.os.path.isdir", return_value=True
        ):
            rutas = _listar_montajes_disponibles()

        self.assertIn("/", rutas)
        self.assertIn("/media/usb drive", rutas)
        self.assertIn("/data/windows_share", rutas)
        self.assertNotIn("/snap/core/17123", rutas)
        self.assertNotIn("/run", rutas)

    @override_settings(ESPACIO_DISCO_RUTA="/")
    def test_endpoint_incluye_montajes_auto_en_modo_default(self):
        uso = namedtuple("Uso", ["total", "used", "free"])
        por_ruta = {
            "/": uso(1000, 400, 600),
            "/media/usb1": uso(2000, 500, 1500),
            "/media/usb2": uso(3000, 1200, 1800),
        }
        por_dev = {"/": 1, "/media/usb1": 2, "/media/usb2": 3}

        def _disk_usage(ruta):
            return por_ruta[ruta]

        def _stat(ruta):
            return type("Stat", (), {"st_dev": por_dev[ruta]})()

        view = EspacioDiscoViewSet.as_view({"get": "list"})
        request = self.factory.get("/api/dashboard/espacio-disco/")
        with patch(
            "dashboard.views._listar_montajes_disponibles",
            return_value=["/media/usb1", "/media/usb2"],
        ), patch("dashboard.views.os.path.exists", return_value=True), patch(
            "dashboard.views.shutil.disk_usage", side_effect=_disk_usage
        ), patch("dashboard.views.os.stat", side_effect=_stat):
            response = view(request)

        self.assertEqual(response.status_code, 200)
        self.assertIn("/media/usb1", response.data["rutas"])
        self.assertIn("/media/usb2", response.data["rutas"])
        self.assertEqual(response.data["rutas_detectadas_auto"], ["/media/usb1", "/media/usb2"])

    @override_settings(ESPACIO_DISCO_RUTA="/")
    def test_endpoint_permite_desactivar_auto_montajes(self):
        uso = namedtuple("Uso", ["total", "used", "free"])
        view = EspacioDiscoViewSet.as_view({"get": "list"})
        request = self.factory.get("/api/dashboard/espacio-disco/?auto_montajes=0")

        with patch(
            "dashboard.views._listar_montajes_disponibles",
            return_value=["/media/usb1"],
        ), patch("dashboard.views.os.path.exists", return_value=True), patch(
            "dashboard.views.shutil.disk_usage", return_value=uso(1000, 400, 600)
        ), patch(
            "dashboard.views.os.stat",
            return_value=type("Stat", (), {"st_dev": 1})(),
        ):
            response = view(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["rutas"], ["/"])
        self.assertEqual(response.data["rutas_detectadas_auto"], [])


class AlineacionVideosMdvrTests(SimpleTestCase):
    def test_alinea_inicio_y_duracion_con_desfase_pequeno(self):
        base = datetime.datetime(2026, 2, 15, 8, 0, 0)
        video_1 = SimpleNamespace(duracion=600, fecha_inicio=base)
        video_2 = SimpleNamespace(
            duracion=600,
            fecha_inicio=base + datetime.timedelta(seconds=10),
        )

        with patch(
            "dashboard.services.importar_videos_mdvr._recortar_video",
            return_value=True,
        ) as recortar_mock, patch(
            "dashboard.services.importar_videos_mdvr.MAX_DESFASE_INICIO_ALINEACION_SEGUNDOS",
            15,
        ):
            recortados = _alinear_duraciones([video_1, video_2])

        self.assertEqual(recortados, 2)
        self.assertEqual(recortar_mock.call_count, 2)
        self.assertEqual(recortar_mock.call_args_list[0].args[1], 590)
        self.assertEqual(recortar_mock.call_args_list[0].kwargs["inicio_offset"], 10)
        self.assertEqual(recortar_mock.call_args_list[1].args[1], 590)
        self.assertEqual(recortar_mock.call_args_list[1].kwargs["inicio_offset"], 0)

    def test_desfase_grande_ca_e_a_recorte_por_duracion(self):
        base = datetime.datetime(2026, 2, 15, 8, 0, 0)
        video_1 = SimpleNamespace(duracion=600, fecha_inicio=base)
        video_2 = SimpleNamespace(
            duracion=590,
            fecha_inicio=base + datetime.timedelta(seconds=30),
        )

        with patch(
            "dashboard.services.importar_videos_mdvr._recortar_video",
            return_value=True,
        ) as recortar_mock, patch(
            "dashboard.services.importar_videos_mdvr.MAX_DESFASE_INICIO_ALINEACION_SEGUNDOS",
            15,
        ):
            recortados = _alinear_duraciones([video_1, video_2])

        self.assertEqual(recortados, 1)
        self.assertEqual(recortar_mock.call_count, 1)
        self.assertEqual(recortar_mock.call_args.args[1], 590)
        self.assertEqual(recortar_mock.call_args.kwargs, {})


class ConcatenacionSegmentosMdvrTests(SimpleTestCase):
    def _segmento(self, ruta: str, extension: str) -> SegmentoVideo:
        base = datetime.datetime(2026, 2, 15, 8, 0, 0)
        return SegmentoVideo(
            ruta=ruta,
            camara=1,
            inicio_dt=base,
            fin_dt=base + datetime.timedelta(minutes=10),
            extension=extension,
        )

    def test_segmentos_mp4_omiten_concat_binaria(self):
        segmentos = [self._segmento("/tmp/a.mp4", ".mp4")]
        with patch(
            "dashboard.services.importar_videos_mdvr._concat_h264",
            return_value=(True, None),
        ) as concat_raw, patch(
            "dashboard.services.importar_videos_mdvr._concat_mp4_copiando",
            return_value=(True, None),
        ) as concat_copy, patch(
            "dashboard.services.importar_videos_mdvr._concat_h264_transcodificando",
            return_value=(True, None),
        ) as concat_ffmpeg:
            ok, _error = _concatenar_segmentos(segmentos, "/tmp/salida.mp4")

        self.assertTrue(ok)
        concat_raw.assert_not_called()
        concat_copy.assert_called_once_with(["/tmp/a.mp4"], "/tmp/salida.mp4")
        concat_ffmpeg.assert_not_called()

    def test_segmentos_mp4_caen_a_transcode_si_copy_falla(self):
        segmentos = [self._segmento("/tmp/a.mp4", ".mp4")]
        with patch(
            "dashboard.services.importar_videos_mdvr._concat_h264",
            return_value=(True, None),
        ) as concat_raw, patch(
            "dashboard.services.importar_videos_mdvr._concat_mp4_copiando",
            return_value=(False, "fallo copy"),
        ) as concat_copy, patch(
            "dashboard.services.importar_videos_mdvr._concat_h264_transcodificando",
            return_value=(True, None),
        ) as concat_ffmpeg:
            ok, _error = _concatenar_segmentos(segmentos, "/tmp/salida.mp4")

        self.assertTrue(ok)
        concat_raw.assert_not_called()
        concat_copy.assert_called_once_with(["/tmp/a.mp4"], "/tmp/salida.mp4")
        concat_ffmpeg.assert_called_once_with(["/tmp/a.mp4"], "/tmp/salida.mp4")

    def test_segmentos_raw_intentan_concat_binaria_primero(self):
        segmentos = [self._segmento("/tmp/a.h264", ".h264")]
        with patch(
            "dashboard.services.importar_videos_mdvr._concat_h264",
            return_value=(True, None),
        ) as concat_raw, patch(
            "dashboard.services.importar_videos_mdvr._concat_h264_transcodificando",
            return_value=(True, None),
        ) as concat_ffmpeg:
            ok, _error = _concatenar_segmentos(segmentos, "/tmp/salida.h264")

        self.assertTrue(ok)
        concat_raw.assert_called_once_with(["/tmp/a.h264"], "/tmp/salida.h264")
        concat_ffmpeg.assert_not_called()

    def test_concat_transcodificando_propagates_ffmpeg_stderr(self):
        exc = subprocess.CalledProcessError(
            returncode=183,
            cmd=["ffmpeg", "-f", "concat"],
            stderr="Impossible to open '/tmp/broken_segment.mp4'",
        )
        with patch(
            "dashboard.services.importar_videos_mdvr._crear_lista_concat",
            return_value="/tmp/concat_lista.txt",
        ), patch(
            "dashboard.services.importar_videos_mdvr.subprocess.run",
            side_effect=exc,
        ), patch(
            "dashboard.services.importar_videos_mdvr.os.path.exists",
            return_value=False,
        ):
            ok, error = _concat_h264_transcodificando(["/tmp/a.mp4"], "/tmp/salida.mp4")

        self.assertFalse(ok)
        self.assertIn("código 183", error)
        self.assertIn("Impossible to open", error)


class ReintentosMdvrTests(SimpleTestCase):
    def test_detecta_error_transitorio_por_timeout(self):
        self.assertTrue(_es_error_transitorio(TimeoutError("timeout")))

    def test_detecta_validation_error_como_permanente(self):
        self.assertFalse(_es_error_transitorio(ValidationError("formato invalido")))

    def test_puede_reprocesarse_error_respeta_backoff(self):
        ahora = timezone.now()
        video = SimpleNamespace(
            estado=EstadoVideo.ERROR,
            reintentos=1,
            proximo_reintento_en=ahora + datetime.timedelta(minutes=5),
            creado_en=ahora - datetime.timedelta(hours=1),
        )
        self.assertFalse(_puede_reprocesarse(video, ahora))

    def test_puede_reprocesarse_procesando_stale(self):
        ahora = timezone.now()
        video = SimpleNamespace(
            estado=EstadoVideo.PROCESANDO,
            reintentos=0,
            proximo_reintento_en=ahora - datetime.timedelta(minutes=1),
            creado_en=ahora - datetime.timedelta(hours=3),
        )
        self.assertTrue(_puede_reprocesarse(video, ahora))

    def test_no_reprocesa_error_permanente(self):
        ahora = timezone.now()
        video = SimpleNamespace(
            estado=EstadoVideo.ERROR_PERMANENTE,
            reintentos=3,
            proximo_reintento_en=None,
            creado_en=ahora - datetime.timedelta(hours=10),
        )
        self.assertFalse(_puede_reprocesarse(video, ahora))

    def test_backoff_reintento_crece(self):
        primer = _calcular_backoff_reintento(1)
        segundo = _calcular_backoff_reintento(2)
        self.assertGreaterEqual(segundo, primer)


class EstadoVelocidadesMdvrTests(SimpleTestCase):
    def test_actualiza_estado_de_velocidades(self):
        class VideoMock:
            def __init__(self):
                self.estado_velocidades = EstadoVelocidadesVideo.PENDIENTE
                self.velocidades_error = ""
                self.velocidades_actualizadas_en = None
                self.guardados = []

            def save(self, update_fields):
                self.guardados.append(update_fields)

        video = VideoMock()
        ahora = timezone.now()
        _actualizar_estado_velocidades(
            video,
            EstadoVelocidadesVideo.IMPORTADA,
            actualizado_en=ahora,
        )

        self.assertEqual(video.estado_velocidades, EstadoVelocidadesVideo.IMPORTADA)
        self.assertEqual(video.velocidades_error, "")
        self.assertEqual(video.velocidades_actualizadas_en, ahora)
        self.assertEqual(
            video.guardados,
            [["estado_velocidades", "velocidades_actualizadas_en"]],
        )

    def test_no_guarda_si_no_hay_cambios(self):
        class VideoMock:
            def __init__(self):
                self.estado_velocidades = EstadoVelocidadesVideo.SIN_XLSX
                self.velocidades_error = "sin archivo"
                self.velocidades_actualizadas_en = None
                self.guardados = []

            def save(self, update_fields):
                self.guardados.append(update_fields)

        video = VideoMock()
        _actualizar_estado_velocidades(
            video,
            EstadoVelocidadesVideo.SIN_XLSX,
            error="sin archivo",
            actualizado_en=None,
        )
        self.assertEqual(video.guardados, [])


class ImportarVelocidadesTabularesTests(TestCase):
    def test_hueco_largo_no_arrastra_ultimo_valor(self):
        camion = Camion.objects.create(patente="BKCD13")
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="video_hueco_largo",
            camara=3,
            ruta_archivo="videos/video_hueco_largo.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 0)),
            fecha_subida=datetime.date(2026, 2, 18),
            inicio_timestamp=datetime.time(8, 0, 0),
            estado=EstadoVideo.LISTO,
            duracion=240,
            id_turno=turno,
        )

        fieldnames = ["Hora", "Velocidad(km / h)"]
        filas = [
            {"Hora": "2026-02-18 08:00:00", "Velocidad(km / h)": "10"},
            {"Hora": "2026-02-18 08:00:10", "Velocidad(km / h)": "20"},
            {"Hora": "2026-02-18 08:00:20", "Velocidad(km / h)": "4"},
            {"Hora": "2026-02-18 08:03:00", "Velocidad(km / h)": "30"},
        ]

        resultado = importar_velocidades_tabulares(video, fieldnames, filas)
        self.assertEqual(resultado["guardadas"], 240)

        velocidad_110 = VelocidadTurno.objects.get(turno=turno, segundo=110)
        self.assertEqual(velocidad_110.velocidad_kmh, 4)
        self.assertFalse(velocidad_110.sin_datos)
        self.assertTrue(velocidad_110.interpolado)

        velocidad_111 = VelocidadTurno.objects.get(turno=turno, segundo=111)
        self.assertEqual(velocidad_111.velocidad_kmh, 0)
        self.assertTrue(velocidad_111.sin_datos)
        self.assertTrue(velocidad_111.interpolado)

        velocidad_180 = VelocidadTurno.objects.get(turno=turno, segundo=180)
        self.assertEqual(velocidad_180.velocidad_kmh, 30)
        self.assertFalse(velocidad_180.sin_datos)
        self.assertFalse(velocidad_180.interpolado)

    def test_mdvr_compacta_salto_grande_de_reloj(self):
        camion = Camion.objects.create(patente="BKCD14")
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="MDVR_4462510196_2026-02-18_manana_C3",
            camara=3,
            ruta_archivo="videos/mdvr_hueco.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 0)),
            fecha_subida=datetime.date(2026, 2, 18),
            inicio_timestamp=datetime.time(8, 0, 0),
            estado=EstadoVideo.LISTO,
            duracion=240,
            id_turno=turno,
        )

        fieldnames = ["Hora", "Velocidad(km / h)"]
        filas = [
            {"Hora": "2026-02-18 08:00:00", "Velocidad(km / h)": "10"},
            {"Hora": "2026-02-18 08:00:10", "Velocidad(km / h)": "20"},
            {"Hora": "2026-02-18 08:00:20", "Velocidad(km / h)": "4"},
            {"Hora": "2026-02-18 08:03:00", "Velocidad(km / h)": "60"},
        ]

        resultado = importar_velocidades_tabulares(video, fieldnames, filas)
        self.assertEqual(resultado["guardadas"], 240)

        # El salto de 08:00:20 -> 08:03:00 se compacta para timeline MDVR.
        velocidad_30 = VelocidadTurno.objects.get(turno=turno, segundo=30)
        self.assertEqual(velocidad_30.velocidad_kmh, 60)
        self.assertFalse(velocidad_30.sin_datos)
        self.assertFalse(velocidad_30.interpolado)

    def test_mdvr_no_compacta_si_linea_cruda_cubre_video(self):
        camion = Camion.objects.create(patente="BKCD15")
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="MDVR_4462510196_2026-02-18_manana_C3",
            camara=3,
            ruta_archivo="videos/mdvr_cobertura_alta.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 0)),
            fecha_subida=datetime.date(2026, 2, 18),
            inicio_timestamp=datetime.time(8, 0, 0),
            estado=EstadoVideo.LISTO,
            duracion=4000,
            id_turno=turno,
        )

        fieldnames = ["Hora", "Velocidad(km / h)"]
        filas = [
            {"Hora": "2026-02-18 08:00:00", "Velocidad(km / h)": "10"},
            {"Hora": "2026-02-18 08:00:10", "Velocidad(km / h)": "20"},
            {"Hora": "2026-02-18 08:00:20", "Velocidad(km / h)": "4"},
            {"Hora": "2026-02-18 08:46:40", "Velocidad(km / h)": "55"},
            {"Hora": "2026-02-18 08:56:40", "Velocidad(km / h)": "65"},
        ]

        resultado = importar_velocidades_tabulares(video, fieldnames, filas)
        self.assertEqual(resultado["guardadas"], 4000)

        # No debe compactar: la muestra de 08:46:40 queda en segundo 2800.
        velocidad_2800 = VelocidadTurno.objects.get(turno=turno, segundo=2800)
        self.assertEqual(velocidad_2800.velocidad_kmh, 55)
        self.assertFalse(velocidad_2800.sin_datos)
        self.assertFalse(velocidad_2800.interpolado)

    def test_mdvr_compacta_con_paso_fijo_de_un_segundo(self):
        camion = Camion.objects.create(patente="BKCD16")
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="MDVR_4462510196_2026-02-18_manana_C3",
            camara=3,
            ruta_archivo="videos/mdvr_paso_fijo.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 0)),
            fecha_subida=datetime.date(2026, 2, 18),
            inicio_timestamp=datetime.time(8, 0, 0),
            estado=EstadoVideo.LISTO,
            duracion=240,
            id_turno=turno,
        )

        fieldnames = ["Hora", "Velocidad(km / h)"]
        filas = [
            {"Hora": "2026-02-18 08:00:00", "Velocidad(km / h)": "10"},
            {"Hora": "2026-02-18 08:00:10", "Velocidad(km / h)": "20"},
            {"Hora": "2026-02-18 08:00:20", "Velocidad(km / h)": "4"},
            {"Hora": "2026-02-18 08:03:00", "Velocidad(km / h)": "60"},
        ]

        with patch(
            "dashboard.services.importar_velocidades_csv.PASO_SALTO_RELOJ_FIJO_SEGUNDOS",
            1,
        ):
            resultado = importar_velocidades_tabulares(video, fieldnames, filas)

        self.assertEqual(resultado["guardadas"], 240)
        velocidad_21 = VelocidadTurno.objects.get(turno=turno, segundo=21)
        self.assertEqual(velocidad_21.velocidad_kmh, 60)
        self.assertFalse(velocidad_21.sin_datos)
        self.assertFalse(velocidad_21.interpolado)

    def test_mapa_segmentos_alinea_velocidades_en_video_concatenado(self):
        camion = Camion.objects.create(patente="BKCD17")
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 18),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.MANANA,
            hora_inicio=datetime.time(8, 0),
            hora_fin=datetime.time(16, 0),
        )
        video = Video.objects.create(
            nombre="MDVR_4462510196_2026-02-18_manana_C3",
            camara=3,
            ruta_archivo="videos/mdvr_mapa.mp4",
            fecha_inicio=timezone.make_aware(datetime.datetime(2026, 2, 18, 8, 0, 0)),
            fecha_subida=datetime.date(2026, 2, 18),
            inicio_timestamp=datetime.time(8, 0, 0),
            estado=EstadoVideo.LISTO,
            duracion=240,
            mapa_segmentos=[
                {
                    "orden": 1,
                    "video_inicio_segundo": 0,
                    "video_fin_segundo": 119,
                    "real_inicio": "2026-02-18T08:00:00-03:00",
                    "real_fin": "2026-02-18T08:02:00-03:00",
                },
                {
                    "orden": 2,
                    "video_inicio_segundo": 120,
                    "video_fin_segundo": 239,
                    "real_inicio": "2026-02-18T08:10:00-03:00",
                    "real_fin": "2026-02-18T08:12:00-03:00",
                },
            ],
            id_turno=turno,
        )

        fieldnames = ["Hora", "Velocidad(km / h)"]
        filas = [
            {"Hora": "2026-02-18 08:01:50", "Velocidad(km / h)": "20"},
            {"Hora": "2026-02-18 08:05:00", "Velocidad(km / h)": "99"},
            {"Hora": "2026-02-18 08:10:00", "Velocidad(km / h)": "60"},
        ]

        resultado = importar_velocidades_tabulares(video, fieldnames, filas)
        self.assertEqual(resultado["guardadas"], 240)
        self.assertGreaterEqual(resultado["descartadas"], 1)

        velocidad_110 = VelocidadTurno.objects.get(turno=turno, segundo=110)
        self.assertEqual(velocidad_110.velocidad_kmh, 20)

        velocidad_120 = VelocidadTurno.objects.get(turno=turno, segundo=120)
        self.assertEqual(velocidad_120.velocidad_kmh, 60)
        self.assertFalse(velocidad_120.sin_datos)
        self.assertFalse(velocidad_120.interpolado)


class ExportarIncidentesApiTests(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.user = get_user_model().objects.create_superuser(
            username="admin_export",
            password="test12345",
        )

    def test_exportar_incidentes_devuelve_formato_plano_requerido(self):
        camion = Camion.objects.create(patente="ABCD11")
        turno = Turno.objects.create(
            fecha=datetime.date(2026, 2, 17),
            id_camion=camion,
            tipo_turno=TipoTurnoChoices.TARDE,
            hora_inicio=datetime.time(16, 0),
            hora_fin=datetime.time(0, 0),
        )
        incidente = Incidente.objects.create(
            tipo_incidente=Incidente.TipoIncidente.FRENADO_BRUSCO,
            severidad=Incidente.Severidad.ALTA,
            tiempo_en_video=765,
            descripcion="El conductor frenó de manera agresiva en la curva 4.",
            turno=turno,
            velocidad_kmh=65.5,
        )

        request = self.factory.get("/api/dashboard/incidentes/exportar/")
        force_authenticate(request, user=self.user)
        view = IncidenteViewSet.as_view({"get": "exportar"})
        response = view(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            response.data[0],
            {
                "id": incidente.id,
                "fecha_hora": "2026-02-17 00:12",
                "jornada_turno": "Tarde",
                "minuto_incidente": "12:45",
                "tipo_incidente": "Frenado o Giro Brusco",
                "severidad": "alta",
                "velocidad_kmh": "65.5",
                "camionPatente": "ABCD11",
                "turno": "Turno 2",
                "descripcion": "El conductor frenó de manera agresiva en la curva 4.",
            },
        )
