import csv
import datetime
import io
import re

from django.utils import timezone
from rest_framework.exceptions import ValidationError

from dashboard.models import VelocidadTurno

_CANDIDATOS_VELOCIDAD = {"velocidadkmh"}
_CANDIDATOS_HORA = {"hora", "fechahora", "datetime", "timestamp", "recibirtiempo"}
_FORMATOS_FECHA = (
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
)


def _normalizar_encabezado(valor):
    return re.sub(r"[^a-z0-9]", "", (valor or "").lower())


def _buscar_columna(fieldnames, candidatos):
    for nombre in fieldnames:
        if _normalizar_encabezado(nombre) in candidatos:
            return nombre
    return None


def _parsear_velocidad(valor):
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    match = re.search(r"[-+]?[0-9]+(?:[.,][0-9]+)?", texto)
    if not match:
        return None
    return float(match.group(0).replace(",", "."))


def _parsear_fecha(valor):
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    for formato in _FORMATOS_FECHA:
        try:
            dt = datetime.datetime.strptime(texto, formato)
        except ValueError:
            continue
        if timezone.is_naive(dt):
            return timezone.make_aware(dt, timezone.get_current_timezone())
        return dt
    return None


def _detectar_dialecto(texto):
    try:
        return csv.Sniffer().sniff(texto, delimiters="\t,;")
    except csv.Error:
        return csv.excel_tab


def importar_velocidades_tabulares(video, fieldnames, filas_iterable):
    turno = getattr(video, "id_turno", None)
    if turno is None:
        raise ValidationError("El video no tiene turno asociado.")

    duracion = video.duracion
    if duracion is None or duracion <= 0:
        raise ValidationError("El video no tiene una duracion valida.")
    ultimo_segundo = int(duracion) - 1
    if ultimo_segundo < 0:
        raise ValidationError("El video no tiene segundos disponibles.")

    if not fieldnames:
        raise ValidationError("El archivo no tiene encabezados.")

    campo_velocidad = _buscar_columna(fieldnames, _CANDIDATOS_VELOCIDAD)
    if not campo_velocidad:
        raise ValidationError("No se encontro la columna de velocidad.")

    campo_hora = _buscar_columna(fieldnames, {"hora"})
    campo_recibir = _buscar_columna(fieldnames, {"recibirtiempo"})
    campo_alterno = None
    if not campo_hora and not campo_recibir:
        campo_alterno = _buscar_columna(fieldnames, _CANDIDATOS_HORA)

    muestras_raw = []
    filas = 0
    descartadas = 0

    for idx, fila in enumerate(filas_iterable):
        filas += 1
        velocidad = _parsear_velocidad(fila.get(campo_velocidad))
        if velocidad is None:
            descartadas += 1
            continue

        timestamp = None
        if campo_hora:
            timestamp = _parsear_fecha(fila.get(campo_hora))
        if timestamp is None and campo_recibir:
            timestamp = _parsear_fecha(fila.get(campo_recibir))
        if timestamp is None and campo_alterno:
            timestamp = _parsear_fecha(fila.get(campo_alterno))
        if timestamp is None:
            descartadas += 1
            continue

        muestras_raw.append((timestamp, velocidad, idx))

    if not muestras_raw:
        raise ValidationError("No se encontraron filas con velocidad valida.")

    base_ts = video.fecha_inicio or min(timestamp for timestamp, _, _ in muestras_raw)
    if timezone.is_naive(base_ts):
        base_ts = timezone.make_aware(base_ts, timezone.get_current_timezone())
    muestras = {}
    muestras_timestamp = {}

    for timestamp, velocidad, _ in muestras_raw:
        segundo = int((timestamp - base_ts).total_seconds())
        if segundo < 0 or segundo > ultimo_segundo:
            descartadas += 1
            continue
        existente_ts = muestras_timestamp.get(segundo)
        if existente_ts is None or timestamp >= existente_ts:
            muestras[segundo] = velocidad
            muestras_timestamp[segundo] = timestamp

    if not muestras:
        raise ValidationError(
            "No hay muestras dentro de la duracion del video."
        )

    registros = {}
    interpoladas = 0
    ultimo_valor = None
    primer_segundo = min(muestras.keys())

    for segundo in range(0, ultimo_segundo + 1):
        if segundo < primer_segundo:
            timestamp = base_ts + datetime.timedelta(seconds=segundo)
            registros[segundo] = VelocidadTurno(
                turno=turno,
                segundo=segundo,
                velocidad_kmh=0,
                timestamp_csv=timestamp,
                interpolado=True,
                sin_datos=True,
            )
            interpoladas += 1
            continue
        if segundo in muestras:
            ultimo_valor = muestras[segundo]
            registros[segundo] = VelocidadTurno(
                turno=turno,
                segundo=segundo,
                velocidad_kmh=ultimo_valor,
                timestamp_csv=muestras_timestamp.get(segundo),
                interpolado=False,
                sin_datos=False,
            )
            continue
        if ultimo_valor is None:
            continue
        timestamp = base_ts + datetime.timedelta(seconds=segundo)
        registros[segundo] = VelocidadTurno(
            turno=turno,
            segundo=segundo,
            velocidad_kmh=ultimo_valor,
            timestamp_csv=timestamp,
            interpolado=True,
            sin_datos=False,
        )
        interpoladas += 1

    VelocidadTurno.objects.filter(turno=turno).delete()
    VelocidadTurno.objects.bulk_create(list(registros.values()), batch_size=1000)

    return {
        "filas": filas,
        "guardadas": len(registros),
        "descartadas": descartadas,
        "interpoladas": interpoladas,
        "reemplazadas": True,
        "turno_id": turno.id,
    }


def importar_velocidades_csv(video, archivo):
    contenido = archivo.read()
    if isinstance(contenido, bytes):
        texto = contenido.decode("utf-8-sig", errors="replace")
    else:
        texto = str(contenido)

    if not texto.strip():
        raise ValidationError("El CSV esta vacio.")

    muestra = texto[:4096]
    dialecto = _detectar_dialecto(muestra)
    lector = csv.DictReader(io.StringIO(texto), dialect=dialecto)
    if not lector.fieldnames:
        raise ValidationError("El CSV no tiene encabezados.")

    return importar_velocidades_tabulares(video, lector.fieldnames, lector)
