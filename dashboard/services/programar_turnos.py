from django.utils import timezone

from dashboard.models import (
    Camion,
    TipoTurnoChoices,
    Turno,
)
def crear_turnos_diarios(fecha=None):
    """
    Crea 3 turnos diarios (mañana, tarde, noche) excepto los domingos.
    Usa el primer camión disponible.
    """
    fecha = fecha or timezone.localdate()

    # Domingo = 6 (weekday con lunes=0)
    if fecha.weekday() == 6:
        return 0

    camion = Camion.objects.order_by("id").first()
    if not camion:
        return 0

    turnos_creados = 0

    for tipo_turno in (
        TipoTurnoChoices.MANANA,
        TipoTurnoChoices.TARDE,
        TipoTurnoChoices.NOCHE,
    ):
        turno, creado = Turno.objects.get_or_create(
            fecha=fecha,
            id_camion=camion,
            tipo_turno=tipo_turno,
            defaults={"activo": False},
        )
        if creado:
            turnos_creados += 1

    return turnos_creados
