from django.utils import timezone

from dashboard.models import (
    AsignacionTurno,
    Camion,
    Operador,
    TipoTurnoChoices,
    Turno,
)


def _semana_iso(fecha):
    return fecha.isocalendar().week


def _asignacion_para_semana_y_tipo(semana, tipo_turno):
    return (
        AsignacionTurno.objects.filter(
            semana=semana,
            turno__tipo_turno=tipo_turno,
        )
        .select_related("operador")
        .first()
    )


def crear_turnos_diarios(fecha=None):
    """
    Crea 3 turnos diarios (mañana, tarde, noche) excepto los domingos.
    Usa el primer camión disponible. Si existe una asignación semanal para el tipo
    de turno, asigna ese operador.
    """
    fecha = fecha or timezone.localdate()

    # Domingo = 6 (weekday con lunes=0)
    if fecha.weekday() == 6:
        return 0

    camion = Camion.objects.order_by("id").first()
    if not camion:
        return 0

    semana_actual = _semana_iso(fecha)
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
            asignacion = _asignacion_para_semana_y_tipo(semana_actual, tipo_turno)
            if asignacion and asignacion.operador_id:
                turno.operador = asignacion.operador
                turno.save(update_fields=["operador"])
            turnos_creados += 1

    return turnos_creados


def crear_asignaciones_semanales(fecha=None):
    """
    Genera las asignaciones semanales (lunes), rotando operadores:
    Semana impar: A (mañana), B (tarde), C (noche)
    Semana par:  C (mañana), B (tarde), A (noche)
    """
    fecha = fecha or timezone.localdate()

    # Solo correr en lunes
    if fecha.weekday() != 0:
        return 0

    semana_actual = _semana_iso(fecha)

    operadores = list(Operador.objects.order_by("id")[:3])
    if len(operadores) < 3:
        return 0

    # Asegurar que existan turnos del lunes antes de asignarlos
    crear_turnos_diarios(fecha=fecha)
    turnos_lunes = Turno.objects.filter(
        fecha=fecha,
        tipo_turno__in=(
            TipoTurnoChoices.MANANA,
            TipoTurnoChoices.TARDE,
            TipoTurnoChoices.NOCHE,
        ),
    ).select_related("id_camion")

    if semana_actual % 2 == 0:
        # Semana par: C, B, A
        asignacion_por_tipo = {
            TipoTurnoChoices.MANANA: operadores[2],
            TipoTurnoChoices.TARDE: operadores[1],
            TipoTurnoChoices.NOCHE: operadores[0],
        }
    else:
        # Semana impar: A, B, C
        asignacion_por_tipo = {
            TipoTurnoChoices.MANANA: operadores[0],
            TipoTurnoChoices.TARDE: operadores[1],
            TipoTurnoChoices.NOCHE: operadores[2],
        }

    creadas = 0
    for turno in turnos_lunes:
        operador_objetivo = asignacion_por_tipo.get(turno.tipo_turno)
        if not operador_objetivo:
            continue

        asignacion, creada = AsignacionTurno.objects.get_or_create(
            semana=semana_actual,
            turno=turno,
            defaults={"operador": operador_objetivo},
        )

        if not creada and asignacion.operador_id != operador_objetivo.id:
            asignacion.operador = operador_objetivo
            asignacion.save(update_fields=["operador"])

        # Mantener alineado el operador del turno con la asignación semanal
        if turno.operador_id != operador_objetivo.id:
            turno.operador = operador_objetivo
            turno.save(update_fields=["operador"])

        creadas += int(creada)

    return creadas
