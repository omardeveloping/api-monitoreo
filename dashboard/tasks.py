import datetime

from celery import current_app, shared_task
from celery.result import AsyncResult
import time

from django.conf import settings
from django.utils import timezone

from .models import EstadoVideo, Turno, Video
from dashboard.services.cmsv6_downloader import (
    CMSV6Config,
    analizar_mp4_reporte,
    ejecutar_job_cmsv6,
    ejecutar_rango,
    recortar_mp4_en_salida,
    reparar_mp4_en_salida,
    resolver_ruta_cmsv6_output,
)
from dashboard.services.programar_turnos import (
    crear_turnos_diarios,
)
from dashboard.services.importar_videos_mdvr import importar_videos_mdvr
from dashboard.services.monitor_mdvr_state import (
    MONITOR_MDVR_SEMANAL_ID_LOGICO,
    guardar_task_id_monitor_mdvr,
    obtener_task_id_monitor_mdvr,
)
from dashboard.services.turnos_tiempo import esta_activo, esta_completado
from dashboard.services.video_importacion import (
    _validated_data_desde_video,
    crear_video_desde_ruta_servidor,
    marcar_video_con_error,
    obtener_base_importacion,
    resolver_ruta_importacion,
)


CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID = MONITOR_MDVR_SEMANAL_ID_LOGICO


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
    importar_velocidades: bool = True,
    fecha_objetivo: str | None = None,
    base_dir: str | None = None,
    omitir_si_monitor_activo: bool = False,
    forzar_reproceso: bool = False,
):
    """Importa videos MDVR desde el servidor y los asocia a turnos."""
    if omitir_si_monitor_activo:
        monitor_activo = _monitor_mdvr_activo_en_workers()
        if monitor_activo:
            return {
                "skipped": True,
                "reason": "monitor_mdvr_semanal_activo",
                "worker_task": monitor_activo,
            }

    return importar_videos_mdvr(
        base_dir=base_dir,
        importar_velocidades=importar_velocidades,
        fecha_objetivo=fecha_objetivo,
        forzar_reproceso=forzar_reproceso,
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
        self.extra = {}

    def _payload(self):
        payload = {
            "progress": self.progress,
            "message": self.message,
            "logs": self.logs[-self.max_logs :],
            "actualizado_en": timezone.now().isoformat(),
        }
        payload.update(self.extra)
        return payload

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

    def progress_cb(self, progress, message, extra=None):
        if progress is not None:
            self.progress = int(max(0, min(100, progress)))
        self.message = str(message or self.message)
        if isinstance(extra, dict):
            self.extra.update(extra)
        self.publish()

    def set_extra(self, **extra):
        self.extra.update(extra)
        self.publish(force=True)

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


def _param_bool(params: dict, name: str, default=False) -> bool:
    if name not in params:
        return default
    return str(params.get(name, "")).lower() in {"1", "true", "yes", "on"}


def _param_int(params: dict, name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(params.get(name, default)))
    except (TypeError, ValueError):
        return default


def _param_float(params: dict, name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(params.get(name, default)))
    except (TypeError, ValueError):
        return default


def _normalizar_tarea_inspect(worker: str, origen: str, tarea: dict):
    request_info = tarea.get("request") if isinstance(tarea, dict) else None
    data = request_info if isinstance(request_info, dict) else tarea
    if not isinstance(data, dict):
        return None
    return {
        "worker": worker,
        "origen": origen,
        "id": data.get("id"),
        "name": data.get("name") or data.get("type"),
    }


def _monitor_backend_reciente(task: AsyncResult, task_id: str):
    if task.status == "PENDING" and task_id:
        return {
            "origen": "backend",
            "id": task_id,
            "name": "dashboard.tasks.cmsv6_monitor_mdvr_semanal_task",
            "status": task.status,
        }

    running_states = {"STARTED", "PROGRESS", "RETRY"}
    if task.status not in running_states:
        return None

    info = task.info if isinstance(task.info, dict) else {}
    actualizado_raw = info.get("actualizado_en")
    if not actualizado_raw:
        return None

    try:
        actualizado = datetime.datetime.fromisoformat(str(actualizado_raw))
        if timezone.is_naive(actualizado):
            actualizado = timezone.make_aware(actualizado, timezone.get_current_timezone())
    except (TypeError, ValueError):
        return None

    try:
        stale_minutos = max(
            1,
            int(getattr(settings, "CMSV6_MONITOR_SEMANAL_STALE_MINUTES", 120)),
        )
    except (TypeError, ValueError):
        stale_minutos = 120

    if actualizado < timezone.now() - datetime.timedelta(minutes=stale_minutos):
        return None

    return {
        "origen": "backend",
        "id": task_id,
        "name": "dashboard.tasks.cmsv6_monitor_mdvr_semanal_task",
        "status": task.status,
        "actualizado_en": actualizado.isoformat(),
    }


def _monitor_mdvr_activo_en_workers():
    task_name = "dashboard.tasks.cmsv6_monitor_mdvr_semanal_task"
    try:
        inspector = current_app.control.inspect(timeout=1.0)
        for origen, metodo in (
            ("active", inspector.active),
            ("reserved", inspector.reserved),
            ("scheduled", inspector.scheduled),
        ):
            data = metodo() or {}
            for worker, tareas in data.items():
                for tarea_info in tareas or []:
                    normalizada = _normalizar_tarea_inspect(worker, origen, tarea_info)
                    if not normalizada:
                        continue
                    if normalizada["id"] == CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID:
                        return normalizada
                    if normalizada["name"] == task_name:
                        return normalizada
    except Exception:
        pass

    task_id = obtener_task_id_monitor_mdvr() or CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID
    task = AsyncResult(task_id, app=current_app)
    return _monitor_backend_reciente(task, task_id)


def _rango_semanal_mdvr(*, incluir_futuro: bool = False):
    hoy = timezone.localdate()
    return hoy - datetime.timedelta(days=1), hoy


def _opciones_monitor_cmsv6(params: dict) -> dict:
    try:
        download_workers_default = int(getattr(settings, "CMSV6_DOWNLOAD_WORKERS", 1) or 1)
    except (TypeError, ValueError):
        download_workers_default = 1
    return {
        "excel_ruta": True,
        "excel_alarmas": _param_bool(params, "excel_alarmas", False),
        "videos": True,
        "testing": _param_bool(params, "testing", False),
        "test_order": params.get("test_order", "size_asc"),
        "test_limit": _param_int(params, "test_limit", 1, minimum=1),
        "test_max_mb": _param_float(params, "test_max_mb", 0, minimum=0),
        "test_channel": params.get("test_channel", "Todos"),
        "test_30d_mode": params.get("test_30d_mode", "off"),
        "test_range_mode": params.get("test_range_mode", "off"),
        "download_workers": _param_int(
            params,
            "download_workers",
            download_workers_default,
            minimum=1,
        ),
    }


def _importar_mdvr_por_rango(
    output_dir: str,
    fecha_inicio: datetime.date,
    fecha_fin: datetime.date,
    reporter: _TaskReporter,
) -> dict:
    resumen = {
        "dias": 0,
        "camiones": 0,
        "videos_creados": 0,
        "errores": [],
        "detalles": [],
    }
    fecha = fecha_inicio
    while fecha <= fecha_fin:
        reporter.progress_cb(98, f"Importando a Django: {fecha.isoformat()}...")
        resultado = importar_videos_mdvr(
            base_dir=output_dir,
            importar_velocidades=True,
            fecha_objetivo=fecha,
        )
        resumen["dias"] += 1
        resumen["camiones"] += int(resultado.get("camiones", 0) or 0)
        resumen["videos_creados"] += int(resultado.get("videos_creados", 0) or 0)
        resumen["errores"].extend(resultado.get("errores", []) or [])
        resumen["detalles"].append(
            {
                "fecha": fecha.isoformat(),
                "camiones": resultado.get("camiones", 0),
                "videos_creados": resultado.get("videos_creados", 0),
                "errores": resultado.get("errores", [])[:20],
            }
        )
        fecha += datetime.timedelta(days=1)
    if len(resumen["errores"]) > 200:
        resumen["errores"] = resumen["errores"][:200] + [
            "Se omitieron errores adicionales del resumen del monitor."
        ]
    return resumen


def _esperar_siguiente_ciclo(reporter: _TaskReporter, segundos: int, ciclo: int):
    if segundos <= 0:
        return
    proximo = timezone.now() + datetime.timedelta(seconds=segundos)
    restante = segundos
    while restante > 0:
        monitor = dict(reporter.extra.get("monitor") or {})
        monitor.update(
            {
                "estado": "esperando",
                "ciclo": ciclo,
                "proximo_ciclo_en": proximo.isoformat(),
                "segundos_para_proximo_ciclo": restante,
            }
        )
        reporter.set_extra(monitor=monitor)
        reporter.progress_cb(100, "Esperando próximo ciclo...")
        paso = min(60, restante)
        time.sleep(paso)
        restante -= paso


@shared_task(bind=True)
def cmsv6_monitor_mdvr_semanal_task(self, params: dict | None = None):
    """
    Mantiene una descarga cíclica CMSV6 para la semana actual.

    La tarea descarga videos y XLSX de ruta GPS en la estructura MDVR esperada,
    luego importa a modelos Django por día para evitar recorrer material histórico
    fuera de la ventana monitoreada.
    """
    params = dict(params or {})
    max_logs = _param_int(params, "max_logs", 500, minimum=50)
    reporter = _TaskReporter(self, max_logs=max_logs)
    output_dir = (params.get("output_dir") or settings.CMSV6_OUTPUT_DIR or "").strip()
    intervalo_default = _param_int(
        {"intervalo_minutos": getattr(settings, "CMSV6_MONITOR_SEMANAL_INTERVAL_MINUTES", 60)},
        "intervalo_minutos",
        60,
        minimum=1,
    )
    intervalo_minutos = _param_int(
        params,
        "intervalo_minutos",
        intervalo_default,
        minimum=1,
    )
    max_ciclos = _param_int(params, "ciclos", 0, minimum=0)
    incluir_futuro = _param_bool(params, "incluir_futuro", False)
    opts = _opciones_monitor_cmsv6(params)
    config = CMSV6Config.from_settings(output_dir)
    config.validate()
    output_dir = config.output_dir

    ciclo = 0
    resultados = []
    reporter.log("Monitor CMSV6 iniciado para hoy y ayer.")
    while max_ciclos == 0 or ciclo < max_ciclos:
        ciclo += 1
        fecha_inicio, fecha_fin = _rango_semanal_mdvr(incluir_futuro=incluir_futuro)
        inicio_dt = datetime.datetime.combine(fecha_inicio, datetime.time.min)
        fin_dt = datetime.datetime.combine(
            fecha_fin,
            datetime.time.max.replace(microsecond=0),
        )
        monitor_base = {
            "estado": "ejecutando",
            "ciclo": ciclo,
            "rango": {
                "desde": fecha_inicio.isoformat(),
                "hasta": fecha_fin.isoformat(),
            },
            "output_dir": output_dir,
            "intervalo_minutos": intervalo_minutos,
            "iniciado_en": timezone.now().isoformat(),
        }
        reporter.set_extra(monitor=monitor_base)
        reporter.progress_cb(1, f"Ciclo {ciclo}: descargando videos de hoy y ayer...")
        reporter.log(
            "Ciclo "
            f"{ciclo}: {fecha_inicio.isoformat()} -> {fecha_fin.isoformat()}"
        )

        importaciones_encoladas = []

        def encolar_importacion_dia(fecha_dia, resumen_dia):
            fecha_iso = fecha_dia.isoformat()
            resumen = dict(resumen_dia or {})
            task = importar_videos_mdvr_task.apply_async(
                kwargs={
                    "base_dir": output_dir,
                    "importar_velocidades": True,
                    "fecha_objetivo": fecha_iso,
                },
                queue="mdvr",
            )
            registro = {
                "fecha": fecha_iso,
                "task_id": task.id,
                "videos_descargados": resumen.get("descargados", 0),
                "videos_omitidos": resumen.get("omitidos", 0),
                "videos_errores": resumen.get("errores", 0),
                "videos_total": resumen.get("total", 0),
            }
            importaciones_encoladas.append(registro)
            reporter.log(f"Importación MDVR encolada para {fecha_iso}: {task.id}")

            monitor_actual = dict(monitor_base)
            monitor_actual["importaciones_encoladas"] = importaciones_encoladas[-50:]
            reporter.set_extra(monitor=monitor_actual)

        resultado_ciclo = {
            "ciclo": ciclo,
            "rango": monitor_base["rango"],
            "descarga": None,
            "importacion_django": None,
            "error": "",
        }
        try:
            descarga = ejecutar_rango(
                output_dir,
                inicio_dt,
                fin_dt,
                reporter.log,
                reporter.progress_cb,
                opts,
                config,
                on_day_complete=encolar_importacion_dia,
            )
            resultado_ciclo["descarga"] = descarga
            resultado_ciclo["importacion_django"] = {
                "modo": "asincronico_por_dia",
                "total_tareas": len(importaciones_encoladas),
                "tareas": list(importaciones_encoladas),
            }
            reporter.log(
                "Descarga semanal finalizada; "
                f"{len(importaciones_encoladas)} importaciones por dia encoladas."
            )
            monitor_base.update(
                {
                    "estado": "completado",
                    "finalizado_en": timezone.now().isoformat(),
                    "importaciones_encoladas": list(importaciones_encoladas),
                    "ultimo_resultado": resultado_ciclo,
                }
            )
            reporter.set_extra(monitor=monitor_base)
        except Exception as exc:
            mensaje = (str(exc) or exc.__class__.__name__).strip()
            resultado_ciclo["error"] = mensaje
            reporter.log(f"ERROR ciclo {ciclo}: {mensaje}")
            monitor_base.update(
                {
                    "estado": "error",
                    "finalizado_en": timezone.now().isoformat(),
                    "ultimo_error": mensaje,
                    "ultimo_resultado": resultado_ciclo,
                }
            )
            reporter.set_extra(monitor=monitor_base)

        resultados.append(resultado_ciclo)
        if len(resultados) > 10:
            resultados = resultados[-10:]
        if max_ciclos and ciclo >= max_ciclos:
            break
        _esperar_siguiente_ciclo(reporter, intervalo_minutos * 60, ciclo)

    reporter.progress_cb(100, "Monitor finalizado")
    return reporter.final_payload(resultados=resultados)


@shared_task
def asegurar_monitor_mdvr_semanal_task(params: dict | None = None):
    """Encola el monitor semanal si no hay uno activo."""
    activo = _monitor_mdvr_activo_en_workers()
    if activo:
        return {
            "running": True,
            "queued": False,
            "task_id": activo.get("id") or obtener_task_id_monitor_mdvr(),
            "monitor_id": CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID,
            "worker_task": activo,
        }

    task = cmsv6_monitor_mdvr_semanal_task.apply_async(args=[dict(params or {})])
    guardar_task_id_monitor_mdvr(task.id)
    return {
        "running": True,
        "queued": True,
        "task_id": task.id,
        "monitor_id": CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID,
    }


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
