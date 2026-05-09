import datetime
import tempfile
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from dashboard.services import cmsv6_downloader
from dashboard.tasks import importar_videos_mdvr_task


class _CMSV6ConfigStub:
    device_id = "4462510196"
    output_dir = ""

    def validate(self):
        return None


class CMSV6AsyncPipelineTests(SimpleTestCase):
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
