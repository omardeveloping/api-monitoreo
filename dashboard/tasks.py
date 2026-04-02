from datetime import datetime, timedelta

from celery import shared_task
from django.utils import timezone

from .models import EstadoVideo, Turno, Video
from dashboard.services.programar_turnos import (
    crear_asignaciones_semanales,
    crear_turnos_diarios,
)
from dashboard.services.video_importacion import (
    _validated_data_desde_video,
    crear_video_desde_ruta_servidor,
    marcar_video_con_error,
    obtener_base_importacion,
    resolver_ruta_importacion,
)


def _limites_turno(turno: Turno):
    inicio = timezone.make_aware(datetime.combine(turno.fecha, turno.hora_inicio))
    fin = timezone.make_aware(datetime.combine(turno.fecha, turno.hora_fin))
    if fin <= inicio:
        # Turno que cruza medianoche
        fin = fin + timedelta(days=1)
    return inicio, fin


def _esta_activo(turno: Turno, ahora: datetime) -> bool:
    """Determina si un turno está activo en el momento actual."""
    inicio, fin = _limites_turno(turno)
    return inicio <= ahora < fin


def _esta_completado(turno: Turno, ahora: datetime) -> bool:
    """Determina si un turno ya alcanzó su hora_fin."""
    _, fin = _limites_turno(turno)
    return ahora >= fin


@shared_task
def actualizar_turnos_activos():
    """Marca turnos como activos/inactivos según horario y fecha."""
    ahora = timezone.localtime()
    turnos_actualizados = []
    activos = 0

    for turno in Turno.objects.all():
        activo_calculado = _esta_activo(turno, ahora)
        completado_calculado = _esta_completado(turno, ahora)
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
def generar_asignaciones_semanales():
    """Crea las asignaciones semanales de operadores (rotación A/B/C) cada lunes."""
    return crear_asignaciones_semanales()


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

    if resultado.pk != video.pk and video.estado != EstadoVideo.LISTO:
        video.delete()
    return {"video_id": resultado.pk, "estado": resultado.estado}
