import datetime
import os
import re

from django.utils import timezone
from rest_framework.exceptions import ValidationError

from dashboard.models import Turno, VelocidadTurno

_FORMATOS_FECHA = (
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
)
_MAX_GAP_INTERPOLACION_DEFAULT = 90
_VELOCIDAD_CMSV6_DIVISOR_DEFAULT = 10.0
_VELOCIDAD_CMSV6_KEYS_DECIMALES = {"speed", "sp", "gpsspeed", "gs"}
try:
    MAX_GAP_INTERPOLACION_SEGUNDOS = int(
        os.environ.get(
            "VELOCIDADES_MAX_GAP_INTERPOLACION_SEGUNDOS",
            _MAX_GAP_INTERPOLACION_DEFAULT,
        )
    )
except ValueError:
    MAX_GAP_INTERPOLACION_SEGUNDOS = _MAX_GAP_INTERPOLACION_DEFAULT
MAX_GAP_INTERPOLACION_SEGUNDOS = max(0, MAX_GAP_INTERPOLACION_SEGUNDOS)
try:
    VELOCIDAD_CMSV6_DIVISOR = float(
        os.environ.get(
            "VELOCIDADES_CMSV6_DIVISOR",
            _VELOCIDAD_CMSV6_DIVISOR_DEFAULT,
        )
    )
except ValueError:
    VELOCIDAD_CMSV6_DIVISOR = _VELOCIDAD_CMSV6_DIVISOR_DEFAULT
if VELOCIDAD_CMSV6_DIVISOR <= 0:
    VELOCIDAD_CMSV6_DIVISOR = _VELOCIDAD_CMSV6_DIVISOR_DEFAULT


def _pick(track: dict, *keys, default=None):
    for key in keys:
        if key in track and track.get(key) not in (None, ""):
            return track.get(key)
    return default


def _pick_con_key(track: dict, *keys, default=None):
    for key in keys:
        if key in track and track.get(key) not in (None, ""):
            return key, track.get(key)
    return None, default


def _parsear_numero(valor):
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip()
    if not texto:
        return None
    match = re.search(r"[-+]?[0-9]+(?:[.,][0-9]+)?", texto)
    if not match:
        return None
    return float(match.group(0).replace(",", "."))


def _parsear_velocidad(valor, key=None):
    velocidad = _parsear_numero(valor)
    if velocidad is None:
        return None
    if key and str(key).lower() in _VELOCIDAD_CMSV6_KEYS_DECIMALES:
        velocidad = velocidad / VELOCIDAD_CMSV6_DIVISOR
        return round(velocidad, 1)
    return velocidad


def _parsear_fecha(valor):
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None

    try:
        dt = datetime.datetime.fromisoformat(texto)
    except ValueError:
        dt = None

    if dt is None:
        for formato in _FORMATOS_FECHA:
            try:
                dt = datetime.datetime.strptime(texto, formato)
                break
            except ValueError:
                continue

    if dt is None:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _asegurar_timezone(dt):
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _obtener_rango_turno(turno: Turno):
    fecha = getattr(turno, "fecha", None)
    hora_inicio = getattr(turno, "hora_inicio", None)
    hora_fin = getattr(turno, "hora_fin", None)
    if fecha is None or hora_inicio is None or hora_fin is None:
        raise ValidationError("El turno no tiene horario valido.")

    inicio = _asegurar_timezone(datetime.datetime.combine(fecha, hora_inicio))
    fecha_fin = fecha
    if hora_fin <= hora_inicio:
        fecha_fin += datetime.timedelta(days=1)
    fin = _asegurar_timezone(datetime.datetime.combine(fecha_fin, hora_fin))
    if fin <= inicio:
        raise ValidationError("El turno no tiene una duracion valida.")
    return inicio, fin


def _iterar_muestras_tracks(tracks):
    muestras_raw = []
    filas = 0
    descartadas = 0

    for idx, track in enumerate(tracks or []):
        if not isinstance(track, dict):
            descartadas += 1
            continue
        filas += 1

        velocidad_key, velocidad_valor = _pick_con_key(
            track,
            "speed",
            "sp",
            "gpsSpeed",
            "gs",
            "Velocidad(km / h)",
            "Velocidad km/h",
        )
        velocidad = _parsear_velocidad(velocidad_valor, velocidad_key)
        if velocidad is None:
            descartadas += 1
            continue

        timestamp = _parsear_fecha(
            _pick(
                track,
                "gpsTime",
                "gt",
                "gps_time",
                "Hora",
                "Hora GPS",
            )
        )
        if timestamp is None:
            timestamp = _parsear_fecha(
                _pick(
                    track,
                    "serverTime",
                    "rt",
                    "receive_time",
                    "Recibir Tiempo",
                )
            )
        if timestamp is None:
            descartadas += 1
            continue

        muestras_raw.append((timestamp, velocidad, idx))

    return muestras_raw, filas, descartadas


def _construir_registros_turno(turno, base_ts, ultimo_segundo, muestras, muestras_timestamp):
    registros = {}
    interpoladas = 0
    ultimo_valor = None
    ultimo_segundo_con_muestra = None
    primer_segundo = min(muestras.keys())

    for segundo in range(0, ultimo_segundo + 1):
        timestamp = base_ts + datetime.timedelta(seconds=segundo)
        if segundo < primer_segundo:
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
            ultimo_segundo_con_muestra = segundo
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

        gap_desde_muestra = (
            segundo - ultimo_segundo_con_muestra
            if ultimo_segundo_con_muestra is not None
            else None
        )
        sin_datos = (
            gap_desde_muestra is None
            or gap_desde_muestra > MAX_GAP_INTERPOLACION_SEGUNDOS
        )
        registros[segundo] = VelocidadTurno(
            turno=turno,
            segundo=segundo,
            velocidad_kmh=0 if sin_datos else ultimo_valor,
            timestamp_csv=timestamp,
            interpolado=True,
            sin_datos=sin_datos,
        )
        interpoladas += 1

    return registros, interpoladas


def importar_velocidades_cmsv6_tracks(turno: Turno, tracks: list[dict]):
    inicio_turno, fin_turno = _obtener_rango_turno(turno)
    ultimo_segundo = int((fin_turno - inicio_turno).total_seconds()) - 1
    if ultimo_segundo < 0:
        raise ValidationError("El turno no tiene segundos disponibles.")

    muestras_raw, filas, descartadas = _iterar_muestras_tracks(tracks)
    if not muestras_raw:
        raise ValidationError("No se encontraron tracks CMSV6 con velocidad valida.")

    muestras_raw_ordenadas = sorted(muestras_raw, key=lambda x: (x[0], x[2]))
    muestras = {}
    muestras_timestamp = {}

    for timestamp, velocidad, _ in muestras_raw_ordenadas:
        timestamp = _asegurar_timezone(timestamp)
        segundo = int((timestamp - inicio_turno).total_seconds())
        if segundo < 0 or segundo > ultimo_segundo:
            descartadas += 1
            continue
        existente_ts = muestras_timestamp.get(segundo)
        if existente_ts is None or timestamp >= existente_ts:
            muestras[segundo] = velocidad
            muestras_timestamp[segundo] = timestamp

    if not muestras:
        raise ValidationError("No hay tracks CMSV6 dentro del rango del turno.")

    registros, interpoladas = _construir_registros_turno(
        turno,
        inicio_turno,
        ultimo_segundo,
        muestras,
        muestras_timestamp,
    )

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
