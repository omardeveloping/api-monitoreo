from datetime import datetime, timedelta

from celery import shared_task
from django.utils import timezone

from .models import Turno
from dashboard.services.programar_turnos import (
    crear_turnos_diarios,
)
from dashboard.services.importar_videos_mdvr import importar_videos_mdvr


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
    turnos = Turno.objects.all()
    for turno in turnos:
        activo_calculado = _esta_activo(turno, ahora)
        completado_calculado = _esta_completado(turno, ahora)

        cambios = []
        if turno.activo != activo_calculado:
            turno.activo = activo_calculado
            cambios.append("activo")
        if turno.completado != completado_calculado:
            turno.completado = completado_calculado
            cambios.append("completado")

        if cambios:
            turno.save(update_fields=cambios)
    return Turno.objects.filter(activo=True).count()


@shared_task
def generar_turnos_diarios():
    """Crea turnos para cada camión (mañana, tarde, noche) cada día excepto domingo."""
    return crear_turnos_diarios()


@shared_task
def importar_videos_mdvr_task():
    """Importa videos MDVR desde el servidor y los asocia a turnos."""
    return importar_videos_mdvr()


