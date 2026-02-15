import datetime

from django.test import SimpleTestCase

from dashboard.services.importar_videos_mdvr import _segmento_desde_archivo


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
