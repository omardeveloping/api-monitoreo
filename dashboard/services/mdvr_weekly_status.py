import datetime
import os
import re
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from dashboard.models import Camion, EstadoVideo, Turno, Video


_ID_PREFIX_RE = re.compile(r"^(?P<id>\d+)")
_SEGMENTO_RE = re.compile(
    r"^(?P<equipo>\d+)-(?P<camara>\d{2})-(?P<inicio>\d{6})-(?P<fin>\d{6})-.*\.(?P<ext>h264|grec|mp4)$",
    re.IGNORECASE,
)
_SEGMENTO_NUEVO_RE = re.compile(
    r"^(?P<equipo>\d+)-(?P<fecha>\d{6})-(?P<inicio>\d{6})-(?P<fin>\d{6})-(?P<codigo>\d+)\.(?P<ext>grec|mp4)$",
    re.IGNORECASE,
)


def rango_semana_actual(*, incluir_futuro: bool = False) -> tuple[datetime.date, datetime.date]:
    hoy = timezone.localdate()
    return hoy - datetime.timedelta(days=1), hoy


def _parse_fecha(value: str | datetime.date | None, *, default: datetime.date) -> datetime.date:
    if isinstance(value, datetime.date):
        return value
    value = str(value or "").strip()
    if not value:
        return default
    return datetime.date.fromisoformat(value)


def _output_dir(output_dir: str | None = None) -> str:
    return (
        (output_dir or "").strip()
        or getattr(settings, "CMSV6_OUTPUT_DIR", "")
        or getattr(settings, "VIDEOS_MDVR_DIR", "")
        or getattr(settings, "VIDEOS_IMPORT_DIR", "")
    )


def _buscar_carpeta_mdvr(base_dir: str, carpeta_id: str) -> Path | None:
    try:
        entries = os.listdir(base_dir)
    except OSError:
        return None
    for nombre in entries:
        ruta = Path(base_dir) / nombre
        if not ruta.is_dir():
            continue
        match = _ID_PREFIX_RE.match(nombre)
        if match and match.group("id") == carpeta_id:
            return ruta
    return None


def _parse_hora_hhmmss(valor: str) -> datetime.time | None:
    if not valor or len(valor) != 6:
        return None
    try:
        hh = int(valor[0:2])
        mm = int(valor[2:4])
        ss = int(valor[4:6])
    except ValueError:
        return None
    if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
        return None
    return datetime.time(hh, mm, ss)


def _parse_fecha_yymmdd(valor: str) -> datetime.date | None:
    if not valor or len(valor) != 6 or not valor.isdigit():
        return None
    try:
        return datetime.date(2000 + int(valor[0:2]), int(valor[2:4]), int(valor[4:6]))
    except ValueError:
        return None


def _extraer_camara_codigo(codigo: str) -> int | None:
    if not codigo or len(codigo) < 6 or not codigo.isdigit():
        return None
    try:
        camara = int(codigo[4:6])
    except ValueError:
        return None
    return camara if camara in {1, 2, 3, 4} else None


def _tipo_turno_para_hora(hora: datetime.time) -> str:
    if hora < datetime.time(8, 0):
        return "noche"
    if hora < datetime.time(16, 0):
        return "manana"
    return "tarde"


def _turno_label(tipo_turno: str) -> str:
    return {
        "noche": "Noche",
        "manana": "Dia",
        "tarde": "Tarde",
    }.get(tipo_turno or "", str(tipo_turno or "Turno"))


def _archivo_para_parsear(path: Path) -> str:
    if path.suffix.lower() == ".tmp":
        return f"{path.stem}.mp4"
    return path.name


def _segmento_desde_archivo(path: Path, fecha: datetime.date) -> dict | None:
    nombre = _archivo_para_parsear(path)
    match = _SEGMENTO_RE.match(nombre)
    if match:
        try:
            camara = int(match.group("camara"))
        except ValueError:
            return None
        inicio = _parse_hora_hhmmss(match.group("inicio"))
        fin = _parse_hora_hhmmss(match.group("fin"))
    else:
        match = _SEGMENTO_NUEVO_RE.match(nombre)
        if not match:
            return None
        fecha_nombre = _parse_fecha_yymmdd(match.group("fecha"))
        if not fecha_nombre or fecha_nombre != fecha:
            return None
        camara = _extraer_camara_codigo(match.group("codigo"))
        inicio = _parse_hora_hhmmss(match.group("inicio"))
        fin = _parse_hora_hhmmss(match.group("fin"))

    if camara not in {1, 2, 3, 4} or not inicio or not fin:
        return None

    stat = path.stat()
    tipo_turno = _tipo_turno_para_hora(inicio)
    return {
        "nombre": path.name,
        "ruta": str(path),
        "camara": camara,
        "turno": tipo_turno,
        "turno_label": _turno_label(tipo_turno),
        "hora_inicio": inicio.isoformat(),
        "hora_fin": fin.isoformat(),
        "extension": path.suffix.lower(),
        "bytes": stat.st_size,
        "mb": round(stat.st_size / 1048576, 1),
        "modificado_en": datetime.datetime.fromtimestamp(
            stat.st_mtime,
            tz=timezone.get_current_timezone(),
        ).isoformat(),
        "parcial": path.suffix.lower() == ".tmp",
    }


def _row_base(camion: Camion, fecha: datetime.date, tipo_turno: str, camara: int) -> dict:
    return {
        "fecha": fecha.isoformat(),
        "turno": tipo_turno,
        "turno_label": _turno_label(tipo_turno),
        "camara": camara,
        "camion": {
            "id": camion.id,
            "patente": camion.patente,
            "carpeta_id": camion.carpeta_id,
        },
        "descarga": {
            "estado": "sin_archivos",
            "completos": 0,
            "parciales": 0,
            "mb_completos": 0.0,
            "mb_parciales": 0.0,
            "archivos": [],
            "parciales_detalle": [],
        },
        "procesamiento": {
            "estado": "sin_registro",
            "procesado": False,
            "video_id": None,
            "nombre": "",
            "estado_velocidades": "",
            "detalle_error": "",
        },
        "estado_resumen": "sin_archivos",
    }


def _actualizar_estado_descarga(row: dict):
    descarga = row["descarga"]
    if descarga["parciales"] and descarga["completos"]:
        descarga["estado"] = "mixto"
    elif descarga["parciales"]:
        descarga["estado"] = "parcial"
    elif descarga["completos"]:
        descarga["estado"] = "descargado"
    else:
        descarga["estado"] = "sin_archivos"


def _estado_resumen(row: dict) -> str:
    procesamiento = row["procesamiento"]["estado"]
    descarga = row["descarga"]["estado"]
    if procesamiento == EstadoVideo.LISTO:
        return "procesado"
    if procesamiento == EstadoVideo.INCOMPLETO:
        return "procesado_incompleto"
    if procesamiento == EstadoVideo.PROCESANDO:
        return "procesando"
    if procesamiento in {EstadoVideo.ERROR, EstadoVideo.ERROR_PERMANENTE}:
        return "error_procesamiento"
    if descarga == "parcial":
        return "descarga_parcial"
    if descarga == "mixto":
        return "descarga_parcial_con_completos"
    if descarga == "descargado":
        return "descargado_sin_procesar"
    return "sin_archivos"


def _sumar_archivo(row: dict, segmento: dict, *, detalle_archivos: bool):
    descarga = row["descarga"]
    if segmento["parcial"]:
        descarga["parciales"] += 1
        descarga["mb_parciales"] = round(descarga["mb_parciales"] + segmento["mb"], 1)
        descarga["parciales_detalle"].append(
            {
                "nombre": segmento["nombre"],
                "mb": segmento["mb"],
                "modificado_en": segmento["modificado_en"],
            }
        )
    else:
        descarga["completos"] += 1
        descarga["mb_completos"] = round(descarga["mb_completos"] + segmento["mb"], 1)
        if detalle_archivos:
            descarga["archivos"].append(
                {
                    "nombre": segmento["nombre"],
                    "mb": segmento["mb"],
                    "modificado_en": segmento["modificado_en"],
                }
            )
        else:
            descarga["archivos"].append(segmento["nombre"])
    _actualizar_estado_descarga(row)


def _agregar_videos_db(rows: dict, camiones: list[Camion], inicio: datetime.date, fin: datetime.date):
    camion_ids = [camion.id for camion in camiones]
    if not camion_ids:
        return
    videos = (
        Video.objects.filter(
            id_turno__fecha__gte=inicio,
            id_turno__fecha__lte=fin,
            id_turno__id_camion_id__in=camion_ids,
        )
        .select_related("id_turno", "id_turno__id_camion")
        .order_by("id")
    )
    for video in videos:
        turno = video.id_turno
        camion = turno.id_camion
        tipo_turno = turno.tipo_turno or "turno"
        key = (camion.id, turno.fecha, tipo_turno, int(video.camara))
        row = rows.setdefault(
            key,
            _row_base(camion, turno.fecha, tipo_turno, int(video.camara)),
        )
        row["procesamiento"] = {
            "estado": video.estado,
            "procesado": video.estado in {EstadoVideo.LISTO, EstadoVideo.INCOMPLETO},
            "video_id": video.id,
            "nombre": video.nombre,
            "estado_velocidades": video.estado_velocidades,
            "detalle_error": (video.detalle_error or video.ultimo_error or "")[:240],
        }


def _inicializar_vacios(rows: dict, camiones: list[Camion], inicio: datetime.date, fin: datetime.date):
    fecha = inicio
    while fecha <= fin:
        for camion in camiones:
            for tipo_turno in ("noche", "manana", "tarde"):
                for camara in range(1, 5):
                    key = (camion.id, fecha, tipo_turno, camara)
                    rows.setdefault(key, _row_base(camion, fecha, tipo_turno, camara))
        fecha += datetime.timedelta(days=1)


def resumen_semana_mdvr(
    *,
    output_dir: str | None = None,
    desde: str | datetime.date | None = None,
    hasta: str | datetime.date | None = None,
    incluir_futuro: bool = False,
    incluir_vacios: bool = False,
    detalle_archivos: bool = False,
) -> dict:
    default_desde, default_hasta = rango_semana_actual(incluir_futuro=incluir_futuro)
    inicio = _parse_fecha(desde, default=default_desde)
    fin = _parse_fecha(hasta, default=default_hasta)
    if inicio > fin:
        raise ValueError("'desde' no puede ser mayor que 'hasta'.")

    base = _output_dir(output_dir)
    rows: dict[tuple[int, datetime.date, str, int], dict] = {}
    camiones = list(Camion.objects.exclude(carpeta_id="").order_by("id"))
    carpetas = []

    if incluir_vacios:
        _inicializar_vacios(rows, camiones, inicio, fin)

    if base and os.path.isdir(base):
        for camion in camiones:
            carpeta = _buscar_carpeta_mdvr(base, camion.carpeta_id)
            if not carpeta:
                continue
            carpetas.append(str(carpeta))
            fecha = inicio
            while fecha <= fin:
                carpeta_dia = carpeta / fecha.isoformat()
                if carpeta_dia.is_dir():
                    for path in sorted(carpeta_dia.iterdir()):
                        if not path.is_file():
                            continue
                        segmento = _segmento_desde_archivo(path, fecha)
                        if not segmento:
                            continue
                        key = (camion.id, fecha, segmento["turno"], segmento["camara"])
                        row = rows.setdefault(
                            key,
                            _row_base(camion, fecha, segmento["turno"], segmento["camara"]),
                        )
                        _sumar_archivo(row, segmento, detalle_archivos=detalle_archivos)
                fecha += datetime.timedelta(days=1)

    _agregar_videos_db(rows, camiones, inicio, fin)

    resultados = []
    totales = {
        "filas": 0,
        "archivos_descargados": 0,
        "archivos_parciales": 0,
        "videos_db": 0,
        "videos_listos": 0,
        "videos_incompletos": 0,
        "videos_procesando": 0,
        "videos_error": 0,
    }
    por_estado = {}
    for key in sorted(rows):
        row = rows[key]
        row["estado_resumen"] = _estado_resumen(row)
        por_estado[row["estado_resumen"]] = por_estado.get(row["estado_resumen"], 0) + 1
        descarga = row["descarga"]
        procesamiento = row["procesamiento"]["estado"]
        totales["filas"] += 1
        totales["archivos_descargados"] += descarga["completos"]
        totales["archivos_parciales"] += descarga["parciales"]
        if procesamiento != "sin_registro":
            totales["videos_db"] += 1
        if procesamiento == EstadoVideo.LISTO:
            totales["videos_listos"] += 1
        elif procesamiento == EstadoVideo.INCOMPLETO:
            totales["videos_incompletos"] += 1
        elif procesamiento == EstadoVideo.PROCESANDO:
            totales["videos_procesando"] += 1
        elif procesamiento in {EstadoVideo.ERROR, EstadoVideo.ERROR_PERMANENTE}:
            totales["videos_error"] += 1
        resultados.append(row)

    return {
        "rango": {"desde": inicio.isoformat(), "hasta": fin.isoformat()},
        "output_dir": base,
        "carpetas_mdvr": carpetas,
        "totales": totales,
        "por_estado": por_estado,
        "resultados": resultados,
    }
