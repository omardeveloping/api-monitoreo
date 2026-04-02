from datetime import datetime, timedelta

from django.utils import timezone


def limites_turno(turno, fecha_base=None):
    fecha = fecha_base or turno.fecha
    inicio = timezone.make_aware(datetime.combine(fecha, turno.hora_inicio))
    fin = timezone.make_aware(datetime.combine(fecha, turno.hora_fin))
    if fin <= inicio:
        fin += timedelta(days=1)
    return inicio, fin


def duracion_turno_segundos(turno, fecha_base=None) -> int:
    inicio, fin = limites_turno(turno, fecha_base=fecha_base)
    return int((fin - inicio).total_seconds())


def esta_activo(turno, ahora=None) -> bool:
    ahora = ahora or timezone.localtime()
    inicio, fin = limites_turno(turno)
    return inicio <= ahora < fin


def esta_completado(turno, ahora=None) -> bool:
    ahora = ahora or timezone.localtime()
    _, fin = limites_turno(turno)
    return ahora >= fin
