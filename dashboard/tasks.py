from celery import shared_task
import time

from .models import EstadoVideo, Turno, Video
from dashboard.services.cmsv6_downloader import (
    analizar_mp4_reporte,
    ejecutar_job_cmsv6,
    recortar_mp4_en_salida,
    reparar_mp4_en_salida,
    resolver_ruta_cmsv6_output,
)
from dashboard.services.programar_turnos import (
    crear_turnos_diarios,
)
from dashboard.services.importar_videos_mdvr import importar_videos_mdvr
from dashboard.services.turnos_tiempo import esta_activo, esta_completado
from dashboard.services.video_importacion import (
    _validated_data_desde_video,
    crear_video_desde_ruta_servidor,
    marcar_video_con_error,
    obtener_base_importacion,
    resolver_ruta_importacion,
)


@shared_task
def actualizar_turnos_activos():
    """Marca turnos como activos/inactivos según horario y fecha."""
    turnos_actualizados = []
    activos = 0

    for turno in Turno.objects.all():
        activo_calculado = esta_activo(turno)
        completado_calculado = esta_completado(turno)
        if activo_calculado:
            activos += 1
        if turno.activo == activo_calculado and turno.completado == completado_calculado:
            continue

        turno.activo = activo_calculado
        turno.completado = completado_calculado
        turnos_actualizados.append(turno)

    if turnos_actualizados:
        Turno.objects.bulk_update(turnos_actualizados, ["activo", "completado"])
    return activos


@shared_task
def generar_turnos_diarios():
    """Crea turnos para cada camión (mañana, tarde, noche) cada día excepto domingo."""
    return crear_turnos_diarios()


@shared_task
def importar_videos_mdvr_task(
    importar_velocidades: bool = True, fecha_objetivo: str | None = None
):
    """Importa videos MDVR desde el servidor y los asocia a turnos."""
    return importar_videos_mdvr(
        importar_velocidades=importar_velocidades,
        fecha_objetivo=fecha_objetivo,
    )


@shared_task(bind=True)
def importar_video_desde_servidor_task(
    self,
    video_id: int,
    ruta_origen: str,
    duracion_esperada_segundos: int | None = None,
):
    video = Video.objects.select_related("id_turno").get(pk=video_id)
    try:
        base_dir_real = obtener_base_importacion()
        ruta_origen, origen_real = resolver_ruta_importacion(base_dir_real, ruta_origen)
        validated_data = _validated_data_desde_video(
            video,
            duracion_esperada_segundos=duracion_esperada_segundos,
        )
        resultado = crear_video_desde_ruta_servidor(
            validated_data,
            origen_real,
            ruta_origen=ruta_origen,
            video_obj=video,
        )
    except Exception as exc:
        video.refresh_from_db(fields=["estado", "detalle_error"])
        if video.estado != EstadoVideo.ERROR or not video.detalle_error:
            marcar_video_con_error(video, exc)
        raise

    if resultado.pk != video.pk:
        Video.objects.filter(pk=video.pk).exclude(estado=EstadoVideo.LISTO).delete()
    return {"video_id": resultado.pk, "estado": resultado.estado}


class _TaskReporter:
    def __init__(self, task, *, max_logs=300):
        self.task = task
        self.max_logs = max_logs
        self.logs = []
        self.progress = 0
        self.message = "Iniciando..."
        self.last_publish = 0.0

    def _payload(self):
        return {
            "progress": self.progress,
            "message": self.message,
            "logs": self.logs[-self.max_logs :],
        }

    def publish(self, *, force=False):
        now = time.monotonic()
        if not force and now - self.last_publish < 1.5:
            return
        self.last_publish = now
        self.task.update_state(state="PROGRESS", meta=self._payload())

    def log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")
        if len(self.logs) > self.max_logs * 2:
            self.logs = self.logs[-self.max_logs :]
        self.publish()

    def progress_cb(self, progress, message):
        if progress is not None:
            self.progress = int(max(0, min(100, progress)))
        self.message = str(message or self.message)
        self.publish()

    def final_payload(self, **extra):
        payload = self._payload()
        payload.update(extra)
        return payload


@shared_task(bind=True)
def cmsv6_descargar_task(self, params: dict):
    """Descarga desde CMSV6 a CMSV6_OUTPUT_DIR y opcionalmente importa a Django."""
    reporter = _TaskReporter(self)
    try:
        resultado = ejecutar_job_cmsv6(params, reporter.log, reporter.progress_cb)
    except Exception as exc:
        reporter.log(f"ERROR: {exc}")
        reporter.progress_cb(reporter.progress, "Error")
        raise
    reporter.progress_cb(100, "Completado")
    return reporter.final_payload(resultado=resultado)


@shared_task(bind=True)
def cmsv6_analizar_mp4_task(self, ruta: str, output_dir: str | None = None):
    reporter = _TaskReporter(self)
    reporter.progress_cb(5, "Resolviendo archivo...")
    path = resolver_ruta_cmsv6_output(ruta, output_dir=output_dir)
    reporter.log(f"Analizando {path}")
    reporter.progress_cb(20, "Analizando MP4...")
    reporte = analizar_mp4_reporte(path)
    reporter.progress_cb(100, "Completado")
    return reporter.final_payload(reporte=reporte, archivo=str(path))


@shared_task(bind=True)
def cmsv6_reparar_mp4_task(self, ruta: str, output_dir: str | None = None):
    reporter = _TaskReporter(self)
    reporter.progress_cb(5, "Resolviendo archivo...")
    reporter.log(f"Reparando {ruta}")
    reporter.progress_cb(20, "Reparando MP4...")
    resultado = reparar_mp4_en_salida(ruta, output_dir=output_dir)
    reporter.logs.extend(resultado.get("logs") or [])
    reporter.progress_cb(100, "Completado" if resultado.get("ok") else "Reparacion fallida")
    return reporter.final_payload(resultado=resultado)


@shared_task(bind=True)
def cmsv6_recortar_mp4_task(self, ruta: str, output_dir: str | None = None):
    reporter = _TaskReporter(self)
    reporter.progress_cb(5, "Resolviendo archivo...")
    reporter.log(f"Recortando {ruta}")
    reporter.progress_cb(20, "Recortando MP4...")
    resultado = recortar_mp4_en_salida(ruta, output_dir=output_dir)
    reporter.logs.extend(resultado.get("logs") or [])
    reporter.progress_cb(100, "Completado" if resultado.get("ok") else "Recorte fallido")
    return reporter.final_payload(resultado=resultado)

