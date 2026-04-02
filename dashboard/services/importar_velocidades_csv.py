import csv
import datetime
import io
import os
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
_MAX_GAP_INTERPOLACION_DEFAULT = 90
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
_UMBRAL_SALTO_RELOJ_DEFAULT = 120
try:
    UMBRAL_SALTO_RELOJ_SEGUNDOS = int(
        os.environ.get(
            "VELOCIDADES_UMBRAL_SALTO_RELOJ_SEGUNDOS",
            _UMBRAL_SALTO_RELOJ_DEFAULT,
        )
    )
except ValueError:
    UMBRAL_SALTO_RELOJ_SEGUNDOS = _UMBRAL_SALTO_RELOJ_DEFAULT
UMBRAL_SALTO_RELOJ_SEGUNDOS = max(1, UMBRAL_SALTO_RELOJ_SEGUNDOS)
COMPACTAR_SALTOS_RELOJ_MDVR = (
    str(os.environ.get("VELOCIDADES_COMPACTAR_SALTOS_RELOJ_MDVR", "1"))
    .strip()
    .lower()
    in {"1", "true", "yes"}
)
_UMBRAL_COBERTURA_SIN_COMPACTAR_DEFAULT = 0.8
try:
    UMBRAL_COBERTURA_SIN_COMPACTAR = float(
        os.environ.get(
            "VELOCIDADES_UMBRAL_COBERTURA_SIN_COMPACTAR",
            _UMBRAL_COBERTURA_SIN_COMPACTAR_DEFAULT,
        )
    )
except ValueError:
    UMBRAL_COBERTURA_SIN_COMPACTAR = _UMBRAL_COBERTURA_SIN_COMPACTAR_DEFAULT
UMBRAL_COBERTURA_SIN_COMPACTAR = min(
    1.0, max(0.0, UMBRAL_COBERTURA_SIN_COMPACTAR)
)
_PASO_SALTO_RELOJ_FIJO_DEFAULT = 0
try:
    PASO_SALTO_RELOJ_FIJO_SEGUNDOS = int(
        os.environ.get(
            "VELOCIDADES_PASO_SALTO_RELOJ_FIJO_SEGUNDOS",
            _PASO_SALTO_RELOJ_FIJO_DEFAULT,
        )
    )
except ValueError:
    PASO_SALTO_RELOJ_FIJO_SEGUNDOS = _PASO_SALTO_RELOJ_FIJO_DEFAULT
PASO_SALTO_RELOJ_FIJO_SEGUNDOS = max(0, PASO_SALTO_RELOJ_FIJO_SEGUNDOS)


def _normalizar_encabezado(valor):
    return re.sub(r"[^a-z0-9]", "", (valor or "").lower())


def _buscar_columna(fieldnames, candidatos):
    for nombre in fieldnames:
        if _normalizar_encabezado(nombre) in candidatos:
            return nombre
    return None


def _resolver_columnas(fieldnames):
    campo_velocidad = _buscar_columna(fieldnames, _CANDIDATOS_VELOCIDAD)
    if not campo_velocidad:
        raise ValidationError("No se encontro la columna de velocidad.")

    campo_hora = _buscar_columna(fieldnames, {"hora"})
    campo_recibir = _buscar_columna(fieldnames, {"recibirtiempo"})
    campo_alterno = None
    if not campo_hora and not campo_recibir:
        campo_alterno = _buscar_columna(fieldnames, _CANDIDATOS_HORA)

    return campo_velocidad, campo_hora, campo_recibir, campo_alterno


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


def _iterar_muestras_validas(filas_iterable, columnas):
    campo_velocidad, campo_hora, campo_recibir, campo_alterno = columnas
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

    return muestras_raw, filas, descartadas


def _detectar_dialecto(texto):
    try:
        return csv.Sniffer().sniff(texto, delimiters="\t,;")
    except csv.Error:
        return csv.excel_tab


def _leer_csv_texto(archivo):
    contenido = archivo.read()
    if isinstance(contenido, bytes):
        return contenido.decode("utf-8-sig", errors="replace")
    return str(contenido)


def _es_video_mdvr(video):
    nombre = (getattr(video, "nombre", "") or "").strip()
    return nombre.startswith("MDVR_")


def _calcular_paso_referencia(muestras_ordenadas):
    if PASO_SALTO_RELOJ_FIJO_SEGUNDOS > 0:
        return PASO_SALTO_RELOJ_FIJO_SEGUNDOS

    deltas = []
    previo = None
    for timestamp, _velocidad, _idx in muestras_ordenadas:
        if previo is None:
            previo = timestamp
            continue
        delta = int((timestamp - previo).total_seconds())
        previo = timestamp
        if 0 < delta <= UMBRAL_SALTO_RELOJ_SEGUNDOS:
            deltas.append(delta)
    if not deltas:
        return 1
    deltas.sort()
    return max(1, deltas[len(deltas) // 2])


def _debe_compactar_saltos_mdvr(muestras_ordenadas, base_ts, ultimo_segundo):
    if not muestras_ordenadas:
        return False
    if ultimo_segundo <= 0:
        return False

    segundos_en_rango = []
    for timestamp, _velocidad, _idx in muestras_ordenadas:
        segundo = int((timestamp - base_ts).total_seconds())
        if 0 <= segundo <= ultimo_segundo:
            segundos_en_rango.append(segundo)

    if not segundos_en_rango:
        return True

    cobertura = max(segundos_en_rango) / float(ultimo_segundo)
    return cobertura < UMBRAL_COBERTURA_SIN_COMPACTAR


def _parsear_iso_datetime(valor):
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    try:
        dt = datetime.datetime.fromisoformat(texto)
    except ValueError:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _obtener_mapa_segmentos(video):
    mapa_raw = getattr(video, "mapa_segmentos", None)
    if not isinstance(mapa_raw, list):
        return []

    segmentos = []
    for item in mapa_raw:
        if not isinstance(item, dict):
            continue
        try:
            video_inicio = int(item.get("video_inicio_segundo"))
            video_fin = int(item.get("video_fin_segundo"))
        except (TypeError, ValueError):
            continue
        if video_fin < video_inicio:
            continue
        real_inicio = _parsear_iso_datetime(item.get("real_inicio"))
        real_fin = _parsear_iso_datetime(item.get("real_fin"))
        if real_inicio is None or real_fin is None:
            continue
        if real_fin <= real_inicio:
            continue
        segmentos.append((real_inicio, real_fin, video_inicio, video_fin))

    segmentos.sort(key=lambda seg: seg[2])
    return segmentos


def _mapear_segundo_con_segmentos(timestamp, segmentos, indice_inicio=0):
    if not segmentos:
        return None, indice_inicio

    idx = max(0, min(indice_inicio, len(segmentos) - 1))
    while idx < len(segmentos):
        real_inicio, real_fin, video_inicio, video_fin = segmentos[idx]
        if timestamp < real_inicio:
            return None, idx
        if timestamp > real_fin:
            idx += 1
            continue
        delta = int((timestamp - real_inicio).total_seconds())
        if delta < 0:
            return None, idx
        segundo = video_inicio + delta
        if segundo > video_fin:
            segundo = video_fin
        if segundo < video_inicio:
            segundo = video_inicio
        return segundo, idx

    return None, len(segmentos) - 1


def _timestamp_para_segundo_mapeado(segundo, segmentos):
    for real_inicio, _real_fin, video_inicio, video_fin in segmentos:
        if video_inicio <= segundo <= video_fin:
            return real_inicio + datetime.timedelta(seconds=segundo - video_inicio)
    return None


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

    columnas = _resolver_columnas(fieldnames)
    muestras_raw, filas, descartadas = _iterar_muestras_validas(filas_iterable, columnas)

    if not muestras_raw:
        raise ValidationError("No se encontraron filas con velocidad valida.")

    base_ts = video.fecha_inicio or min(timestamp for timestamp, _, _ in muestras_raw)
    if timezone.is_naive(base_ts):
        base_ts = timezone.make_aware(base_ts, timezone.get_current_timezone())

    muestras_raw_ordenadas = sorted(muestras_raw, key=lambda x: (x[0], x[2]))
    mapa_segmentos = _obtener_mapa_segmentos(video)
    usar_mapa_segmentos = bool(mapa_segmentos)
    compactar_saltos = (
        (not usar_mapa_segmentos)
        and COMPACTAR_SALTOS_RELOJ_MDVR
        and _es_video_mdvr(video)
        and _debe_compactar_saltos_mdvr(muestras_raw_ordenadas, base_ts, ultimo_segundo)
    )
    paso_referencia = (
        _calcular_paso_referencia(muestras_raw_ordenadas)
        if compactar_saltos
        else 0
    )

    muestras = {}
    muestras_timestamp = {}
    timestamp_previo = None
    segundo_compactado = None
    indice_segmento = 0

    for timestamp, velocidad, _ in muestras_raw_ordenadas:
        if usar_mapa_segmentos:
            segundo, indice_segmento = _mapear_segundo_con_segmentos(
                timestamp,
                mapa_segmentos,
                indice_inicio=indice_segmento,
            )
            if segundo is None:
                descartadas += 1
                continue
        else:
            segundo = int((timestamp - base_ts).total_seconds())
        if compactar_saltos:
            if segundo_compactado is None:
                segundo_compactado = segundo
            else:
                delta = int((timestamp - timestamp_previo).total_seconds())
                if delta < 0:
                    timestamp_previo = timestamp
                    continue
                if delta > UMBRAL_SALTO_RELOJ_SEGUNDOS:
                    delta = paso_referencia
                segundo_compactado += delta
            timestamp_previo = timestamp
            segundo = segundo_compactado

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
    ultimo_segundo_con_muestra = None
    primer_segundo = min(muestras.keys())

    for segundo in range(0, ultimo_segundo + 1):
        timestamp_mapeado = (
            _timestamp_para_segundo_mapeado(segundo, mapa_segmentos)
            if usar_mapa_segmentos
            else None
        )
        if segundo < primer_segundo:
            if usar_mapa_segmentos:
                timestamp = timestamp_mapeado
            else:
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
        if usar_mapa_segmentos:
            timestamp = timestamp_mapeado
        else:
            timestamp = base_ts + datetime.timedelta(seconds=segundo)
        if usar_mapa_segmentos and timestamp is None:
            registros[segundo] = VelocidadTurno(
                turno=turno,
                segundo=segundo,
                velocidad_kmh=0,
                timestamp_csv=None,
                interpolado=True,
                sin_datos=True,
            )
            interpoladas += 1
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
    texto = _leer_csv_texto(archivo)

    if not texto.strip():
        raise ValidationError("El CSV esta vacio.")

    muestra = texto[:4096]
    dialecto = _detectar_dialecto(muestra)
    lector = csv.DictReader(io.StringIO(texto), dialect=dialecto)
    if not lector.fieldnames:
        raise ValidationError("El CSV no tiene encabezados.")

    return importar_velocidades_tabulares(video, lector.fieldnames, lector)
