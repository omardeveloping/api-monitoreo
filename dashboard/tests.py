import datetime
from collections import namedtuple
from types import SimpleNamespace
from unittest.mock import mock_open, patch

from django.test import SimpleTestCase, override_settings
from rest_framework.test import APIRequestFactory

from dashboard.services.importar_videos_mdvr import _alinear_duraciones, _segmento_desde_archivo
from dashboard.views import EspacioDiscoViewSet, _listar_montajes_disponibles


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
