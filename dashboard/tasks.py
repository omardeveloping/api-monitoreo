from datetime import datetime, timedelta

from celery import shared_task
from django.utils import timezone

from .models import Turno


def _esta_activo(turno: Turno, ahora: datetime) -> bool:
    """Determina si un turno está activo en el momento actual."""
    inicio = timezone.make_aware(datetime.combine(turno.fecha, turno.hora_inicio))
    fin = timezone.make_aware(datetime.combine(turno.fecha, turno.hora_fin))
    if fin <= inicio:
        # Turno que cruza medianoche
        fin = fin + timedelta(days=1)
    return inicio <= ahora < fin


@shared_task
def actualizar_turnos_activos():
    """Marca turnos como activos/inactivos según horario y fecha."""
    ahora = timezone.localtime()
    turnos = Turno.objects.all()
    for turno in turnos:
        activo_calculado = _esta_activo(turno, ahora)
        if turno.activo != activo_calculado:
            turno.activo = activo_calculado
            turno.save(update_fields=["activo"])
    return Turno.objects.filter(activo=True).count()
