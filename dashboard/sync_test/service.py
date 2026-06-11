import json
import os
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from django.conf import settings
from django.http import FileResponse, StreamingHttpResponse
from django.utils import timezone
from rest_framework.exceptions import NotFound, ValidationError

from dashboard.models import Turno, Video
from dashboard.serializers import VideoSerializer
from dashboard.services.calcular_duracion_video import calcular_duracion_video
from dashboard.services.cmsv6_downloader import (
    CMSV6Config,
    CMSV6Session,
    _descargar_videos_dia,
    _segundos_video,
    _tipo_turno_para_segundos,
    _video_channel_idx,
    _video_size_bytes,
    nombre_video_cmsv6,
)


TURNOS = {
    "noche": ("Noche", time(0, 0), time(8, 0)),
    "manana": ("Dia", time(8, 0), time(16, 0)),
    "tarde": ("Tarde", time(16, 0), time(0, 0)),
}

_DOWNLOAD_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_DOWNLOAD_JOBS: dict[str, dict] = {}
_DOWNLOAD_LOCK = threading.Lock()
_MAX_DOWNLOAD_JOBS = 20


def sync_test_habilitado() -> bool:
    valor = os.environ.get("MDVR_SYNC_TEST_ENABLED")
    if valor is not None:
        return valor.strip().lower() in {"1", "true", "yes", "on"}
    return bool(settings.DEBUG)


def validar_sync_test_habilitado():
    if not sync_test_habilitado():
        raise NotFound("Ruta de test de sincronizacion no habilitada.")


def _valor_request(request, nombre: str, default=None):
    if request.method in {"POST", "PUT", "PATCH"} and hasattr(request, "data"):
        if nombre in request.data:
            return request.data.get(nombre)
    return request.query_params.get(nombre, default)


def _request_params(request) -> dict:
    params = {key: value for key, value in request.query_params.items()}
    if request.method in {"POST", "PUT", "PATCH"} and hasattr(request, "data"):
        for key, value in request.data.items():
            params[key] = value
    return params


def _valor_params(params: dict, nombre: str, default=None):
    return params.get(nombre, default)


def _int_request(request, nombre: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    valor = _valor_request(request, nombre, None)
    if valor in (None, ""):
        resultado = default
    else:
        try:
            resultado = int(valor)
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"Parametro '{nombre}' invalido.") from exc
    resultado = max(minimum, resultado)
    if maximum is not None:
        resultado = min(maximum, resultado)
    return resultado


def _int_params(params: dict, nombre: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    valor = _valor_params(params, nombre, None)
    if valor in (None, ""):
        resultado = default
    else:
        try:
            resultado = int(valor)
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"Parametro '{nombre}' invalido.") from exc
    resultado = max(minimum, resultado)
    if maximum is not None:
        resultado = min(maximum, resultado)
    return resultado


def _parse_fecha(valor: str, nombre: str):
    try:
        return datetime.strptime(valor, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"Parametro '{nombre}' invalido. Use formato YYYY-MM-DD."
        ) from exc


def _job_timestamp() -> str:
    return timezone.localtime(timezone.now()).strftime("%H:%M:%S")


def _prune_jobs_locked():
    if len(_DOWNLOAD_JOBS) <= _MAX_DOWNLOAD_JOBS:
        return
    ordered = sorted(
        _DOWNLOAD_JOBS.items(),
        key=lambda item: item[1].get("created_at", ""),
    )
    for job_id, _job in ordered[: max(0, len(ordered) - _MAX_DOWNLOAD_JOBS)]:
        _DOWNLOAD_JOBS.pop(job_id, None)


def _create_job(params: dict) -> dict:
    job_id = uuid.uuid4().hex
    now = timezone.now().isoformat()
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0,
        "message": "En cola",
        "logs": [f"[{_job_timestamp()}] En cola"],
        "params": dict(params),
        "created_at": now,
        "updated_at": now,
        "result": None,
        "error": "",
    }
    with _DOWNLOAD_LOCK:
        _DOWNLOAD_JOBS[job_id] = job
        _prune_jobs_locked()
    return job


def _update_job(job_id: str, **fields):
    with _DOWNLOAD_LOCK:
        job = _DOWNLOAD_JOBS.get(job_id)
        if not job:
            return
        job.update(fields)
        job["updated_at"] = timezone.now().isoformat()


def _append_job_log(job_id: str, message: str):
    line = f"[{_job_timestamp()}] {message}"
    with _DOWNLOAD_LOCK:
        job = _DOWNLOAD_JOBS.get(job_id)
        if not job:
            return
        logs = job.setdefault("logs", [])
        logs.append(line)
        del logs[:-500]
        job["message"] = message
        job["updated_at"] = timezone.now().isoformat()


def _job_progress(job_id: str, progress: int, message: str, extra=None):
    fields = {
        "status": "running",
        "progress": max(0, min(100, int(progress or 0))),
        "message": str(message or ""),
    }
    if extra:
        fields["extra"] = extra
    _update_job(job_id, **fields)
    if message:
        _append_job_log(job_id, str(message))


def _job_snapshot(job_id: str) -> dict:
    with _DOWNLOAD_LOCK:
        job = _DOWNLOAD_JOBS.get(job_id)
        if not job:
            raise NotFound("Trabajo de descarga no encontrado.")
        return dict(job, logs=list(job.get("logs", [])))


def _fecha_desde_request(request):
    fecha_param = (_valor_request(request, "fecha", "") or "").strip()
    desde_param = (_valor_request(request, "desde", "") or "").strip()
    hasta_param = (_valor_request(request, "hasta", "") or "").strip()

    if fecha_param and (desde_param or hasta_param):
        raise ValidationError("Use 'fecha' o 'desde'/'hasta', no ambos.")

    if fecha_param:
        fecha = _parse_fecha(fecha_param, "fecha")
        return fecha, fecha

    dias_default = _int_request(request, "dias", 14, minimum=1, maximum=90)
    hasta = _parse_fecha(hasta_param, "hasta") if hasta_param else timezone.localdate()
    desde = (
        _parse_fecha(desde_param, "desde")
        if desde_param
        else hasta - timedelta(days=dias_default - 1)
    )
    if desde > hasta:
        raise ValidationError("Parametro 'desde' no puede ser mayor que 'hasta'.")
    return desde, hasta


def _turno_rango_dt(turno: Turno):
    inicio = datetime.combine(turno.fecha, turno.hora_inicio)
    fin = datetime.combine(turno.fecha, turno.hora_fin)
    if fin <= inicio:
        fin += timedelta(days=1)
    tz_actual = timezone.get_current_timezone()
    if timezone.is_naive(inicio):
        inicio = timezone.make_aware(inicio, tz_actual)
    if timezone.is_naive(fin):
        fin = timezone.make_aware(fin, tz_actual)
    return inicio, fin


def _turno_rango_manual(fecha, tipo_turno: str):
    label, hora_inicio, hora_fin = TURNOS.get(tipo_turno, (tipo_turno or "Turno", time(0, 0), time(0, 0)))
    inicio = datetime.combine(fecha, hora_inicio)
    fin = datetime.combine(fecha, hora_fin)
    if fin <= inicio:
        fin += timedelta(days=1)
    tz_actual = timezone.get_current_timezone()
    inicio = timezone.make_aware(inicio, tz_actual) if timezone.is_naive(inicio) else inicio
    fin = timezone.make_aware(fin, tz_actual) if timezone.is_naive(fin) else fin
    return label, inicio, fin


def _video_sync_prioridad(video: Video):
    estado_prio = 2 if video.estado == "listo" else 1
    return estado_prio, video.id or 0


def _video_db_payload(request, video: Video | None):
    if video is None:
        return None
    serializer = VideoSerializer(
        video,
        context={
            "request": request,
            "compat_playable_incomplete": True,
        },
    )
    data = dict(serializer.data)
    return {
        "id": f"db-{data.get('id')}",
        "video_id": data.get("id"),
        "nombre": data.get("nombre"),
        "camara": data.get("camara"),
        "estado": data.get("estado"),
        "estado_real": data.get("estado_real", data.get("estado")),
        "ruta_archivo": data.get("ruta_archivo"),
        "duracion": data.get("duracion"),
        "fecha_inicio": data.get("fecha_inicio"),
        "fecha_fin": data.get("fecha_fin"),
        "inicio_timestamp": data.get("inicio_timestamp"),
        "fin_timestamp": data.get("fin_timestamp"),
        "ruta_origen": data.get("ruta_origen"),
        "segmentos_origen": data.get("segmentos_origen") or [],
        "mapa_segmentos": data.get("mapa_segmentos") or [],
        "detalle_error": data.get("detalle_error") or "",
        "origen_test": "db",
    }


def _turno_to_flat(turno_payload: dict, fecha_key: str):
    disponible = sum(1 for item in turno_payload["camaras"] if item.get("video"))
    turno_payload["fecha"] = fecha_key
    turno_payload["camaras_disponibles"] = disponible
    turno_payload["titulo"] = (
        f"{fecha_key} | {turno_payload.get('tipo_turno_label') or turno_payload.get('tipo_turno')}"
        f" | {turno_payload.get('hora_inicio')} - {turno_payload.get('hora_fin')}"
        f" | {disponible} CH"
    )
    return turno_payload


def sync_test_payload(request):
    validar_sync_test_habilitado()
    desde, hasta = _fecha_desde_request(request)
    turnos = list(
        Turno.objects.select_related("id_camion")
        .filter(fecha__gte=desde, fecha__lte=hasta)
        .order_by("-fecha", "hora_inicio", "id")
    )
    videos = (
        Video.objects.filter(
            id_turno__in=turnos,
            estado__in=["listo", "incompleto"],
        )
        .exclude(ruta_archivo="")
        .select_related("id_turno")
        .order_by("id_turno_id", "camara", "-id")
    )
    mejores: dict[tuple[int, int], Video] = {}
    for video in videos:
        key = (video.id_turno_id, video.camara)
        actual = mejores.get(key)
        if actual is None or _video_sync_prioridad(video) > _video_sync_prioridad(actual):
            mejores[key] = video

    dias: dict[str, dict] = {}
    planos = []
    for turno in turnos:
        inicio_dt, fin_dt = _turno_rango_dt(turno)
        fecha_key = turno.fecha.isoformat()
        dia = dias.setdefault(fecha_key, {"fecha": fecha_key, "turnos": []})
        camaras = []
        for camara in range(1, 5):
            camaras.append(
                {
                    "camara": camara,
                    "video": _video_db_payload(request, mejores.get((turno.id, camara))),
                }
            )
        turno_payload = {
            "id": f"db-turno-{turno.id}",
            "turno_id": turno.id,
            "tipo_turno": turno.tipo_turno,
            "tipo_turno_label": turno.get_tipo_turno_display() if turno.tipo_turno else "",
            "hora_inicio": turno.hora_inicio.isoformat(),
            "hora_fin": turno.hora_fin.isoformat(),
            "inicio": inicio_dt.isoformat(),
            "fin": fin_dt.isoformat(),
            "duracion_segundos": int((fin_dt - inicio_dt).total_seconds()),
            "camion": {
                "id": turno.id_camion_id,
                "patente": turno.id_camion.patente if turno.id_camion_id else "",
                "carpeta_id": turno.id_camion.carpeta_id if turno.id_camion_id else "",
            },
            "camaras": camaras,
            "origen_test": "db",
        }
        turno_payload = _turno_to_flat(turno_payload, fecha_key)
        dia["turnos"].append(turno_payload)
        planos.append(turno_payload)

    return {
        "desde": desde.isoformat(),
        "hasta": hasta.isoformat(),
        "total_dias": len(dias),
        "total_turnos": len(turnos),
        "dias": list(dias.values()),
        "turnos": planos,
    }


def _sync_test_output_root() -> Path:
    configured = os.environ.get("MDVR_SYNC_TEST_OUTPUT_DIR", "").strip()
    if configured:
        return Path(configured)
    return Path(settings.BASE_DIR) / "local_debug" / "sync_test"


def _dias_scan(params: dict):
    fecha_param = (_valor_params(params, "fecha", "") or "").strip()
    desde_param = (_valor_params(params, "desde", "") or "").strip()
    hasta_param = (_valor_params(params, "hasta", "") or "").strip()
    if fecha_param:
        fecha = _parse_fecha(fecha_param, "fecha")
        return [fecha]
    dias = _int_params(params, "dias_busqueda", 30, minimum=1, maximum=180)
    hasta = _parse_fecha(hasta_param, "hasta") if hasta_param else timezone.localdate()
    desde = _parse_fecha(desde_param, "desde") if desde_param else hasta - timedelta(days=dias - 1)
    if desde > hasta:
        raise ValidationError("Parametro 'desde' no puede ser mayor que 'hasta'.")
    total = (hasta - desde).days + 1
    return [hasta - timedelta(days=idx) for idx in range(total)]


def _overlap(a: dict, b: dict) -> int:
    inicio = max(_segundos_video(a, "beg"), _segundos_video(b, "beg"))
    fin = min(_segundos_video(a, "end"), _segundos_video(b, "end"))
    return max(0, fin - inicio)


def _archivo_resumen(archivo: dict) -> dict:
    return {
        "camara": _video_channel_idx(archivo) + 1,
        "beg": _segundos_video(archivo, "beg"),
        "end": _segundos_video(archivo, "end"),
        "len": _video_size_bytes(archivo),
        "loc": archivo.get("loc"),
        "file": archivo.get("file"),
        "stream": archivo.get("stream"),
        "streamType": archivo.get("streamType"),
    }


def _seleccionar_set_archivos(files: list[dict], *, min_ch: int, max_ch: int, turno_filtro: str):
    candidatos = [item for item in files or [] if _video_size_bytes(item) > 4096]
    if turno_filtro in TURNOS:
        candidatos = [
            item
            for item in candidatos
            if _tipo_turno_para_segundos(_segundos_video(item, "beg")) == turno_filtro
        ]
    candidatos.sort(
        key=lambda item: (
            _tipo_turno_para_segundos(_segundos_video(item, "beg")),
            _segundos_video(item, "beg"),
            -_video_size_bytes(item),
        )
    )
    mejor = None
    mejor_score = None

    for base in candidatos:
        tipo = _tipo_turno_para_segundos(_segundos_video(base, "beg"))
        por_canal: dict[int, list[dict]] = {}
        for item in candidatos:
            if _tipo_turno_para_segundos(_segundos_video(item, "beg")) != tipo:
                continue
            por_canal.setdefault(_video_channel_idx(item), []).append(item)

        seleccion = [base]
        canal_base = _video_channel_idx(base)
        for canal, items in sorted(por_canal.items()):
            if canal == canal_base:
                continue
            solapados = [
                (item, _overlap(base, item))
                for item in items
                if _overlap(base, item) > 0
            ]
            if not solapados:
                continue
            item, overlap = max(solapados, key=lambda pair: (pair[1], _video_size_bytes(pair[0])))
            seleccion.append(item)
            if len(seleccion) >= max_ch:
                break

        canales = {_video_channel_idx(item) for item in seleccion}
        if len(canales) < min_ch:
            continue
        inicio = max(_segundos_video(item, "beg") for item in seleccion)
        fin = min(_segundos_video(item, "end") for item in seleccion)
        score = (len(canales), max(0, fin - inicio), sum(_video_size_bytes(item) for item in seleccion))
        if mejor is None or score > mejor_score:
            mejor = {
                "tipo_turno": tipo,
                "archivos": sorted(seleccion, key=lambda item: _video_channel_idx(item)),
                "overlap_inicio": inicio,
                "overlap_fin": fin,
            }
            mejor_score = score
    return mejor


def _actualizar_url_params(url: str, nuevos: dict) -> str:
    if not url:
        return url
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    for key, value in nuevos.items():
        if value is None:
            query.pop(key, None)
        else:
            query[key] = str(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _recortar_archivo_cmsv6(archivo: dict, inicio: int, fin: int) -> dict:
    original_inicio = _segundos_video(archivo, "beg")
    duracion_rel_inicio = max(0, inicio - original_inicio)
    duracion_rel_fin = max(duracion_rel_inicio + 1, fin - original_inicio)
    recortado = dict(archivo)
    recortado["beg"] = inicio
    recortado["end"] = fin
    recortado["len"] = 0
    recortado["file"] = ""
    recortado["_sync_test_clip"] = {
        "original_beg": original_inicio,
        "original_end": _segundos_video(archivo, "end"),
        "clip_beg": inicio,
        "clip_end": fin,
    }
    recortado["DownUrl"] = _actualizar_url_params(
        str(archivo.get("DownUrl", "") or ""),
        {
            "BEG": inicio,
            "END": fin,
            "SAVENAME": "",
        },
    )
    recortado["PlaybackUrl"] = _actualizar_url_params(
        str(archivo.get("PlaybackUrl", "") or ""),
        {
            "FILEBEG": inicio,
            "FILEEND": fin,
            "PLAYBEG": duracion_rel_inicio,
            "PLAYEND": duracion_rel_fin,
        },
    )
    return recortado


def _recortar_archivos_por_porcentaje(candidato: dict, porcentaje: int) -> dict:
    porcentaje = max(1, min(100, int(porcentaje or 100)))
    if porcentaje >= 100:
        candidato["porcentaje_muestra"] = 100
        return candidato

    inicio = int(candidato.get("overlap_inicio") or 0)
    fin = int(candidato.get("overlap_fin") or 0)
    duracion = max(0, fin - inicio)
    if duracion <= 1:
        candidato["porcentaje_muestra"] = 100
        return candidato

    minimo = _int_params(
        {"min_sample": os.environ.get("MDVR_SYNC_TEST_MIN_SAMPLE_SECONDS", 60)},
        "min_sample",
        60,
        minimum=10,
        maximum=3600,
    )
    duracion_muestra = max(minimo, int(round(duracion * porcentaje / 100)))
    duracion_muestra = min(duracion, duracion_muestra)
    fin_muestra = inicio + duracion_muestra
    recortado = dict(candidato)
    recortado["archivos"] = [
        _recortar_archivo_cmsv6(archivo, inicio, fin_muestra)
        for archivo in candidato["archivos"]
    ]
    recortado["overlap_fin"] = fin_muestra
    recortado["porcentaje_muestra"] = porcentaje
    recortado["duracion_muestra_segundos"] = duracion_muestra
    return recortado


def _local_file_url(base_file_url: str, root: Path, path: Path) -> str:
    rel = path.resolve().relative_to(root.resolve()).as_posix()
    separator = "&" if "?" in base_file_url else "?"
    return f"{base_file_url}{separator}{urlencode({'path': rel})}"


def _leer_sidecar(path: Path) -> dict:
    sidecar = Path(f"{path}.cmsv6.json")
    if not sidecar.exists():
        return {}
    try:
        return json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _hhmmss(segundos: int) -> str:
    segundos = max(0, int(segundos or 0)) % 86400
    return f"{segundos // 3600:02d}:{(segundos % 3600) // 60:02d}:{segundos % 60:02d}"


def _real_dt_from_day_second(fecha, segundos: int):
    tz_actual = timezone.get_current_timezone()
    base = datetime.combine(fecha, time(0, 0)) + timedelta(seconds=max(0, int(segundos or 0)))
    return timezone.make_aware(base, tz_actual) if timezone.is_naive(base) else base


def _local_video_payload(base_file_url: str, root: Path, path: Path, archivo: dict, fecha) -> dict:
    sidecar = _leer_sidecar(path)
    beg = int(sidecar.get("beg_segundo_dia", _segundos_video(archivo, "beg")) or 0)
    end = int(sidecar.get("end_segundo_dia", _segundos_video(archivo, "end")) or 0)
    camara = int(sidecar.get("camara", _video_channel_idx(archivo) + 1) or 1)
    try:
        duracion = int(round(calcular_duracion_video(str(path))))
    except Exception:
        duracion = max(0, end - beg)
    duracion_solicitada = max(0, end - beg)
    ajuste_timeline = None
    if duracion_solicitada >= 30 and duracion > int(duracion_solicitada * 1.4):
        ajuste_timeline = {
            "motivo": "cmsv6_ignoro_recorte_muestra",
            "duracion_solicitada_segundos": duracion_solicitada,
            "duracion_mp4_segundos": duracion,
            "fin_solicitado_segundo_dia": end,
        }
        end = beg + duracion
    real_inicio = _real_dt_from_day_second(fecha, beg)
    real_fin = _real_dt_from_day_second(fecha, end)
    return {
        "id": f"local-{path.stem}-{camara}",
        "nombre": path.stem,
        "camara": camara,
        "estado": "local",
        "estado_real": "local",
        "ruta_archivo": _local_file_url(base_file_url, root, path),
        "duracion": duracion,
        "fecha_inicio": real_inicio.isoformat(),
        "fecha_fin": real_fin.isoformat(),
        "inicio_timestamp": _hhmmss(beg),
        "fin_timestamp": _hhmmss(end),
        "ruta_origen": path.name,
        "segmentos_origen": [path.name],
        "mapa_segmentos": [
            {
                "orden": 1,
                "archivo": path.name,
                "video_inicio_segundo": 0,
                "video_fin_segundo": max(0, duracion - 1),
                "real_inicio": real_inicio.isoformat(),
                "real_fin": real_fin.isoformat(),
                "cmsv6": sidecar,
                "ajuste_timeline_test": ajuste_timeline,
            }
        ],
        "detalle_error": "",
        "origen_test": "local_debug",
        "ajuste_timeline_test": ajuste_timeline,
    }


def _turno_local_payload(
    base_file_url: str,
    root: Path,
    config: CMSV6Config,
    fecha,
    tipo_turno: str,
    archivos: list[dict],
    inicio_seg: int | None = None,
    fin_seg: int | None = None,
) -> dict:
    label, inicio_dt, fin_dt = _turno_rango_manual(fecha, tipo_turno)
    if inicio_seg is not None and fin_seg is not None and fin_seg > inicio_seg:
        inicio_dt = _real_dt_from_day_second(fecha, inicio_seg)
        fin_dt = _real_dt_from_day_second(fecha, fin_seg)
    carpeta_dia = root / f"{config.device_id}({config.device_id})" / fecha.isoformat()
    videos_por_camara = {}
    for archivo in archivos:
        nombre = nombre_video_cmsv6(archivo, config.device_id, fecha)
        path = carpeta_dia / nombre
        if not path.exists():
            continue
        video = _local_video_payload(base_file_url, root, path, archivo, fecha)
        videos_por_camara[video["camara"]] = video

    if videos_por_camara:
        inicios = [parse for parse in (datetime.fromisoformat(v["fecha_inicio"]) for v in videos_por_camara.values())]
        fines = [parse for parse in (datetime.fromisoformat(v["fecha_fin"]) for v in videos_por_camara.values())]
        if inicios and fines:
            inicio_dt = min(inicios)
            fin_dt = max(fines)

    camaras = [
        {"camara": camara, "video": videos_por_camara.get(camara)}
        for camara in range(1, 5)
    ]
    payload = {
        "id": f"local-{fecha.isoformat()}-{tipo_turno}-{int(timezone.now().timestamp())}",
        "tipo_turno": tipo_turno,
        "tipo_turno_label": label,
        "hora_inicio": inicio_dt.time().isoformat(),
        "hora_fin": fin_dt.time().isoformat(),
        "inicio": inicio_dt.isoformat(),
        "fin": fin_dt.isoformat(),
        "duracion_segundos": int((fin_dt - inicio_dt).total_seconds()),
        "camion": {
            "id": None,
            "patente": config.device_id,
            "carpeta_id": config.device_id,
        },
        "camaras": camaras,
        "origen_test": "local_debug",
    }
    return _turno_to_flat(payload, fecha.isoformat())


def _descargar_set_prueba(params: dict, base_file_url: str, *, job_id: str | None = None) -> dict:
    min_ch = _int_params(params, "min_ch", 2, minimum=2, maximum=4)
    max_ch = _int_params(params, "max_ch", 4, minimum=min_ch, maximum=4)
    sample_percent = _int_params(params, "sample_percent", 100, minimum=1, maximum=100)
    turno_filtro = (_valor_params(params, "turno", "") or "").strip()
    if turno_filtro and turno_filtro not in TURNOS:
        raise ValidationError("Parametro 'turno' invalido.")

    root = _sync_test_output_root()
    root.mkdir(parents=True, exist_ok=True)
    config = CMSV6Config.from_settings(str(root))
    config.validate()
    session = CMSV6Session(config)
    logs = []

    def log(msg):
        logs.append(str(msg))
        if job_id:
            _append_job_log(job_id, str(msg))

    log("Login CMSV6...")
    session.login()
    log("Login CMSV6 exitoso.")

    for fecha in _dias_scan(params):
        log(f"Consultando {fecha.isoformat()}...")
        files = session.get_video_files(fecha, log_fn=log)
        log(f"Archivos encontrados: {len(files)}")
        candidato = _seleccionar_set_archivos(
            files,
            min_ch=min_ch,
            max_ch=max_ch,
            turno_filtro=turno_filtro,
        )
        if not candidato:
            log("Sin set con suficientes CH solapados.")
            continue

        candidato = _recortar_archivos_por_porcentaje(candidato, sample_percent)
        archivos = candidato["archivos"]
        log(
            "Set elegido: "
            + ", ".join(
                f"CH{_video_channel_idx(item) + 1} {_hhmmss(_segundos_video(item, 'beg'))}-{_hhmmss(_segundos_video(item, 'end'))}"
                for item in archivos
            )
        )
        if candidato.get("porcentaje_muestra", 100) < 100:
            log(
                "Muestra solicitada: "
                f"{candidato['porcentaje_muestra']}% "
                f"({candidato.get('duracion_muestra_segundos', 0)}s del tramo comun)."
            )
        resumen = _descargar_videos_dia(
            session,
            archivos,
            datetime.combine(fecha, time(0, 0)),
            root / f"{config.device_id}({config.device_id})",
            log,
            (
                (lambda progress, message, extra=None: _job_progress(job_id, progress, message, extra))
                if job_id
                else (lambda *_args, **_kwargs: None)
            ),
            config,
            0,
            100,
            download_workers=1,
        )
        turno = _turno_local_payload(
            base_file_url,
            root,
            config,
            fecha,
            candidato["tipo_turno"],
            archivos,
            inicio_seg=candidato.get("overlap_inicio"),
            fin_seg=candidato.get("overlap_fin"),
        )
        videos_descargados = sum(1 for item in turno["camaras"] if item.get("video"))
        if videos_descargados < min_ch:
            log(
                "Descarga insuficiente para prueba: "
                f"{videos_descargados}/{min_ch} camaras disponibles."
            )
            continue
        report = {
            "ok": True,
            "fecha": fecha.isoformat(),
            "tipo_turno": candidato["tipo_turno"],
            "output_dir": str(root),
            "resumen_descarga": resumen,
            "archivos": [_archivo_resumen(item) for item in archivos],
            "sample_percent": candidato.get("porcentaje_muestra", 100),
            "turno": turno,
            "logs": logs[-200:],
        }
        report_path = root / "sync_test_latest.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report

    report = {
        "ok": False,
        "output_dir": str(root),
        "logs": logs[-200:],
        "detalle": "No se encontro un set de 2 o mas camaras con solape en el rango.",
    }
    (root / "sync_test_latest.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report


def descargar_set_prueba_payload(request):
    validar_sync_test_habilitado()
    params = _request_params(request)
    base_file_url = request.build_absolute_uri("../test-sync-file/")
    return _descargar_set_prueba(params, base_file_url)


def _run_download_job(job_id: str, params: dict, base_file_url: str):
    _update_job(job_id, status="running", progress=0, message="Iniciando descarga")
    try:
        result = _descargar_set_prueba(params, base_file_url, job_id=job_id)
        status = "success" if result.get("ok") else "failed"
        _update_job(
            job_id,
            status=status,
            progress=100,
            message="Descarga lista" if result.get("ok") else result.get("detalle", "Sin resultado"),
            result=result,
            error="" if result.get("ok") else result.get("detalle", ""),
        )
    except Exception as exc:
        _append_job_log(job_id, f"ERROR: {exc}")
        _update_job(
            job_id,
            status="failed",
            progress=100,
            message=str(exc),
            error=str(exc),
        )


def iniciar_descarga_prueba_payload(request):
    validar_sync_test_habilitado()
    params = _request_params(request)
    job = _create_job(params)
    base_file_url = request.build_absolute_uri("../test-sync-file/")
    _DOWNLOAD_EXECUTOR.submit(_run_download_job, job["job_id"], params, base_file_url)
    return _job_snapshot(job["job_id"])


def estado_descarga_prueba_payload(request):
    validar_sync_test_habilitado()
    job_id = (request.query_params.get("job_id") or "").strip()
    if not job_id:
        raise ValidationError("Parametro 'job_id' requerido.")
    return _job_snapshot(job_id)


def servir_archivo_prueba(request):
    validar_sync_test_habilitado()
    rel = (request.query_params.get("path") or "").strip()
    if not rel:
        raise NotFound("Archivo no indicado.")
    root = _sync_test_output_root().resolve()
    path = (root / rel).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise NotFound("Archivo fuera de la carpeta de test.") from exc
    if not path.exists() or not path.is_file():
        raise NotFound("Archivo de test no encontrado.")
    return _video_file_response(request, path)


def _iter_file_range(path: Path, start: int, length: int, chunk_size: int = 1024 * 1024):
    with open(path, "rb") as archivo:
        archivo.seek(start)
        remaining = length
        while remaining > 0:
            chunk = archivo.read(min(chunk_size, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


def _video_file_response(request, path: Path):
    size = path.stat().st_size
    range_header = request.headers.get("Range") or request.META.get("HTTP_RANGE", "")
    match = re.match(r"bytes=(\d*)-(\d*)$", range_header or "")
    if match:
        start_raw, end_raw = match.groups()
        if start_raw:
            start = int(start_raw)
            end = int(end_raw) if end_raw else size - 1
        else:
            suffix = int(end_raw or 0)
            start = max(0, size - suffix)
            end = size - 1
        start = max(0, min(start, size - 1))
        end = max(start, min(end, size - 1))
        length = end - start + 1
        response = StreamingHttpResponse(
            _iter_file_range(path, start, length),
            status=206,
            content_type="video/mp4",
        )
        response["Content-Range"] = f"bytes {start}-{end}/{size}"
        response["Content-Length"] = str(length)
        response["Accept-Ranges"] = "bytes"
        return response

    response = FileResponse(open(path, "rb"), content_type="video/mp4")
    response["Content-Length"] = str(size)
    response["Accept-Ranges"] = "bytes"
    return response


def sync_test_html(payload: dict, data_url: str, download_url: str, download_status_url: str) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    data_url_json = json.dumps(data_url)
    download_url_json = json.dumps(download_url)
    download_status_url_json = json.dumps(download_status_url)
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Test sincronizacion MDVR</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --line: #d8dde6;
      --text: #17202a;
      --muted: #5c6675;
      --accent: #1769aa;
      --bad: #b42318;
      --ok: #067647;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.4 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    header {{
      position: sticky;
      top: 0;
      z-index: 5;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      padding: 12px 16px;
    }}
    h1 {{ margin: 0 0 10px; font-size: 18px; }}
    form, .toolbar {{ display: flex; flex-wrap: wrap; gap: 8px; align-items: end; }}
    .toolbar {{ margin-top: 8px; }}
    label {{ display: grid; gap: 3px; color: var(--muted); font-size: 12px; }}
    input, select, button {{
      min-height: 32px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 8px;
      background: white;
      color: var(--text);
    }}
    select {{ min-width: min(760px, 100%); }}
    button {{
      background: var(--accent);
      color: white;
      border-color: var(--accent);
      cursor: pointer;
      font-weight: 600;
    }}
    button.secondary {{ background: white; color: var(--accent); }}
    main {{ padding: 16px; display: grid; gap: 16px; }}
    .turn {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }}
    .turn-head {{
      display: grid;
      gap: 10px;
      padding: 12px;
      border-bottom: 1px solid var(--line);
    }}
    .turn-meta {{ display: flex; gap: 12px; flex-wrap: wrap; color: var(--muted); }}
    .controls {{ display: grid; gap: 8px; }}
    .buttons {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .timeline-row {{ display: grid; grid-template-columns: 110px 1fr 120px; gap: 10px; align-items: center; }}
    input[type="range"] {{ width: 100%; padding: 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(220px, 1fr)); gap: 10px; padding: 10px; }}
    .cam {{ border: 1px solid var(--line); border-radius: 6px; overflow: hidden; background: #fbfcfd; }}
    .cam-head {{ display: flex; justify-content: space-between; gap: 8px; padding: 8px; border-bottom: 1px solid var(--line); }}
    .cam-title {{ font-weight: 700; }}
    .state {{ color: var(--muted); font-size: 12px; }}
    .missing {{ min-height: 190px; display: grid; place-items: center; color: var(--muted); }}
    video {{ display: block; width: 100%; aspect-ratio: 16 / 9; background: #101214; }}
    .cam-info {{ padding: 8px; display: grid; gap: 4px; color: var(--muted); font-size: 12px; }}
    .drift {{ color: var(--text); font-variant-numeric: tabular-nums; }}
    .drift.bad {{ color: var(--bad); }}
    .drift.ok {{ color: var(--ok); }}
    .small {{ color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }}
    .status {{ min-height: 18px; color: var(--muted); }}
    .log-panel {{
      margin-top: 8px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #101214;
      color: #e8edf2;
      overflow: hidden;
    }}
    .log-panel .log-title {{
      padding: 7px 10px;
      border-bottom: 1px solid #2b3036;
      color: #c8d0da;
      font-weight: 700;
    }}
    .log-panel pre {{
      margin: 0;
      max-height: 260px;
      overflow: auto;
      padding: 10px;
      white-space: pre-wrap;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    @media (max-width: 1100px) {{ .grid {{ grid-template-columns: repeat(2, minmax(220px, 1fr)); }} }}
    @media (max-width: 640px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .timeline-row {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Test sincronizacion MDVR</h1>
    <form method="get">
      <label>Fecha <input name="fecha" type="date"></label>
      <label>Desde <input name="desde" type="date" value="{payload.get('desde', '')}"></label>
      <label>Hasta <input name="hasta" type="date" value="{payload.get('hasta', '')}"></label>
      <label>Dias <input name="dias" type="number" min="1" max="90" value="14"></label>
      <button type="submit">Cargar</button>
      <a class="small" href="" id="json-link">JSON</a>
    </form>
    <div class="toolbar">
      <label>Dia / turno <select id="turn-selector"></select></label>
      <button type="button" id="prev-turn" class="secondary">Anterior</button>
      <button type="button" id="next-turn" class="secondary">Siguiente</button>
    </div>
    <div class="toolbar">
      <label>Buscar dias <input id="download-days" type="number" min="1" max="180" value="30"></label>
      <label>Min CH <input id="download-min-ch" type="number" min="2" max="4" value="2"></label>
      <label>Max CH <input id="download-max-ch" type="number" min="2" max="4" value="4"></label>
      <label>Muestra % <input id="download-percent" type="number" min="1" max="100" step="1" value="10"></label>
      <label>Turno
        <select id="download-turno">
          <option value="">Cualquiera</option>
          <option value="noche">Noche</option>
          <option value="manana">Dia</option>
          <option value="tarde">Tarde</option>
        </select>
      </label>
      <button type="button" id="download-sample">Descargar prueba</button>
      <span class="status" id="download-status"></span>
    </div>
    <div class="log-panel">
      <div class="log-title">Logs de descarga local</div>
      <pre id="download-logs">Sin descarga activa.</pre>
    </div>
  </header>
  <main id="app"></main>
  <script>
    const DATA_URL = {data_url_json};
    const DOWNLOAD_URL = {download_url_json};
    const DOWNLOAD_STATUS_URL = {download_status_url_json};
    const state = {{
      payload: {payload_json},
      activeIndex: 0,
      timer: null,
      downloadPoll: null,
      loadedJobs: new Set(),
    }};
    document.getElementById('json-link').href = DATA_URL + window.location.search;

    function turnos() {{
      if (Array.isArray(state.payload.turnos)) return state.payload.turnos;
      return (state.payload.dias || []).flatMap((dia) => (dia.turnos || []).map((turno) => ({{...turno, fecha: dia.fecha}})));
    }}
    function pad(n) {{ return String(Math.floor(Math.max(0, n))).padStart(2, '0'); }}
    function fmtSeconds(value) {{
      const total = Math.max(0, Math.floor(Number(value) || 0));
      const h = Math.floor(total / 3600);
      const m = Math.floor((total % 3600) / 60);
      const s = total % 60;
      return `${{pad(h)}}:${{pad(m)}}:${{pad(s)}}`;
    }}
    function parseMs(value) {{
      const ms = Date.parse(value || '');
      return Number.isFinite(ms) ? ms : null;
    }}
    function segments(video) {{
      return Array.isArray(video?.mapa_segmentos) ? video.mapa_segmentos : [];
    }}
    function realToVideoTime(video, targetMs) {{
      const segs = segments(video);
      for (const seg of segs) {{
        const rs = parseMs(seg.real_inicio);
        const re = parseMs(seg.real_fin);
        if (rs === null || re === null || re <= rs) continue;
        if (targetMs >= rs && targetMs <= re) {{
          const vs = Number(seg.video_inicio_segundo) || 0;
          const ve = Number(seg.video_fin_segundo) || vs;
          const ratio = (targetMs - rs) / (re - rs);
          return Math.max(0, vs + ratio * Math.max(0, ve - vs));
        }}
      }}
      const start = parseMs(video?.fecha_inicio);
      if (start === null) return 0;
      return Math.max(0, (targetMs - start) / 1000);
    }}
    function videoTimeToRealMs(video, currentTime) {{
      const segs = segments(video);
      for (const seg of segs) {{
        const vs = Number(seg.video_inicio_segundo) || 0;
        const ve = Number(seg.video_fin_segundo) || vs;
        if (currentTime >= vs && currentTime <= ve) {{
          const rs = parseMs(seg.real_inicio);
          const re = parseMs(seg.real_fin);
          if (rs === null || re === null || re <= rs || ve <= vs) continue;
          const ratio = (currentTime - vs) / (ve - vs);
          return rs + ratio * (re - rs);
        }}
      }}
      const start = parseMs(video?.fecha_inicio);
      return start === null ? null : start + currentTime * 1000;
    }}
    function currentTurnEl() {{ return document.querySelector('.turn'); }}
    function seekTurn(turnEl) {{
      const range = turnEl.querySelector('[data-role="range"]');
      if (turnEl.dataset.playing === '1') {{
        turnEl.dataset.playBase = String(Number(range.value) || 0);
        turnEl.dataset.playStarted = String(performance.now());
      }}
      const startMs = Number(turnEl.dataset.startMs);
      const targetMs = startMs + Number(range.value) * 1000;
      turnEl.querySelectorAll('video[data-video-id]').forEach((el) => {{
        const videoData = JSON.parse(el.dataset.videoJson);
        const target = realToVideoTime(videoData, targetMs);
        if (Number.isFinite(target)) el.currentTime = Math.max(0, target);
      }});
      updateTurn(turnEl);
    }}
    function updateTurn(turnEl) {{
      if (!turnEl) return;
      const range = turnEl.querySelector('[data-role="range"]');
      if (turnEl.dataset.playing === '1') {{
        const base = Number(turnEl.dataset.playBase || range.value || 0);
        const started = Number(turnEl.dataset.playStarted || performance.now());
        const elapsed = Math.max(0, (performance.now() - started) / 1000);
        range.value = String(Math.min(Number(range.max), base + elapsed));
      }}
      const startMs = Number(turnEl.dataset.startMs);
      const sliderRealMs = startMs + Number(range.value) * 1000;
      turnEl.querySelector('[data-role="time"]').textContent = fmtSeconds(range.value);
      turnEl.querySelectorAll('video[data-video-id]').forEach((el) => {{
        const videoData = JSON.parse(el.dataset.videoJson);
        const realMs = videoTimeToRealMs(videoData, el.currentTime);
        const drift = realMs === null ? null : (realMs - sliderRealMs) / 1000;
        const driftEl = turnEl.querySelector(`[data-drift-for="${{el.dataset.videoId}}"]`);
        if (driftEl) {{
          driftEl.textContent = drift === null ? 'drift: ?' : `drift: ${{drift.toFixed(2)}}s`;
          driftEl.className = 'drift ' + (Math.abs(drift || 0) <= 2 ? 'ok' : 'bad');
        }}
      }});
    }}
    function playTurn(turnEl) {{
      seekTurn(turnEl);
      const range = turnEl.querySelector('[data-role="range"]');
      turnEl.dataset.playing = '1';
      turnEl.dataset.playBase = String(Number(range.value) || 0);
      turnEl.dataset.playStarted = String(performance.now());
      turnEl.querySelectorAll('video[data-video-id]').forEach((el) => el.play().catch(() => {{}}));
    }}
    function pauseTurn(turnEl) {{
      updateTurn(turnEl);
      turnEl.dataset.playing = '0';
      turnEl.querySelectorAll('video[data-video-id]').forEach((el) => el.pause());
    }}
    function renderSelector() {{
      const selector = document.getElementById('turn-selector');
      const items = turnos();
      selector.innerHTML = '';
      items.forEach((turno, index) => {{
        const opt = document.createElement('option');
        opt.value = String(index);
        opt.textContent = turno.titulo || `${{turno.fecha || ''}} | ${{turno.tipo_turno_label || turno.tipo_turno || 'Turno'}} | ${{turno.camaras_disponibles || 0}} CH`;
        selector.appendChild(opt);
      }});
      if (items.length) {{
        state.activeIndex = Math.max(0, Math.min(state.activeIndex, items.length - 1));
        selector.value = String(state.activeIndex);
      }}
    }}
    function renderTurn() {{
      const app = document.getElementById('app');
      const items = turnos();
      app.innerHTML = '';
      if (!items.length) {{
        app.innerHTML = '<p>No hay turnos/videos en el rango seleccionado.</p>';
        return;
      }}
      const turno = items[state.activeIndex];
      const startMs = parseMs(turno.inicio) || 0;
      const turnEl = document.createElement('article');
      turnEl.className = 'turn';
      turnEl.dataset.startMs = String(startMs);
      const title = turno.tipo_turno_label || turno.tipo_turno || 'Turno';
      turnEl.innerHTML = `
        <div class="turn-head">
          <div class="turn-meta">
            <strong>${{turno.fecha || ''}} ${{title}}</strong>
            <span>${{turno.hora_inicio}} - ${{turno.hora_fin}}</span>
            <span>${{turno.camion?.patente || ''}}</span>
            <span>${{fmtSeconds(turno.duracion_segundos)}}</span>
            <span>${{turno.origen_test || ''}}</span>
          </div>
          <div class="controls">
            <div class="buttons">
              <button type="button" data-action="play">Play sync</button>
              <button type="button" data-action="pause">Pausa</button>
              <button type="button" data-action="seek">Re-sincronizar</button>
            </div>
            <div class="timeline-row">
              <span>Linea tiempo</span>
              <input data-role="range" type="range" min="0" max="${{turno.duracion_segundos}}" value="0" step="1">
              <strong data-role="time">00:00:00</strong>
            </div>
          </div>
        </div>
        <div class="grid"></div>
      `;
      const grid = turnEl.querySelector('.grid');
      for (const cam of turno.camaras || []) {{
        const camEl = document.createElement('section');
        camEl.className = 'cam';
        const video = cam.video;
        if (!video || !video.ruta_archivo) {{
          camEl.innerHTML = `<div class="cam-head"><span class="cam-title">CH${{cam.camara}}</span><span class="state">sin video</span></div><div class="missing">Sin video</div>`;
        }} else {{
          const encoded = JSON.stringify(video).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
          camEl.innerHTML = `
            <div class="cam-head">
              <span class="cam-title">CH${{cam.camara}}</span>
              <span class="state">${{video.estado_real || video.estado}}</span>
            </div>
            <video controls preload="metadata" playsinline data-video-id="${{video.id}}" data-video-json="${{encoded}}" src="${{video.ruta_archivo}}"></video>
            <div class="cam-info">
              <span class="drift" data-drift-for="${{video.id}}">drift: ?</span>
              <span>${{video.inicio_timestamp || ''}} - ${{video.fin_timestamp || ''}} | ${{fmtSeconds(video.duracion || 0)}}</span>
              <span class="small">${{video.ruta_origen || video.nombre}}</span>
            </div>
          `;
        }}
        grid.appendChild(camEl);
      }}
      turnEl.querySelector('[data-action="play"]').addEventListener('click', () => playTurn(turnEl));
      turnEl.querySelector('[data-action="pause"]').addEventListener('click', () => pauseTurn(turnEl));
      turnEl.querySelector('[data-action="seek"]').addEventListener('click', () => seekTurn(turnEl));
      turnEl.querySelector('[data-role="range"]').addEventListener('input', () => seekTurn(turnEl));
      turnEl.querySelectorAll('video').forEach((el) => {{
        el.addEventListener('timeupdate', () => updateTurn(turnEl));
        el.addEventListener('seeked', () => updateTurn(turnEl));
      }});
      app.appendChild(turnEl);
    }}
    function render() {{
      renderSelector();
      renderTurn();
      if (state.timer) clearInterval(state.timer);
      state.timer = setInterval(() => updateTurn(currentTurnEl()), 1000);
    }}
    document.getElementById('turn-selector').addEventListener('change', (event) => {{
      state.activeIndex = Number(event.target.value) || 0;
      renderTurn();
    }});
    document.getElementById('prev-turn').addEventListener('click', () => {{
      state.activeIndex = Math.max(0, state.activeIndex - 1);
      render();
    }});
    document.getElementById('next-turn').addEventListener('click', () => {{
      state.activeIndex = Math.min(turnos().length - 1, state.activeIndex + 1);
      render();
    }});
    function renderDownloadJob(job) {{
      const status = document.getElementById('download-status');
      const logsEl = document.getElementById('download-logs');
      const pct = Number(job.progress || 0);
      status.textContent = `${{job.status || 'running'}} | ${{pct}}% | ${{job.message || ''}}`;
      logsEl.textContent = (job.logs || []).join('\\n') || 'Sin logs todavia.';
      logsEl.scrollTop = logsEl.scrollHeight;
      const result = job.result || {{}};
      if (job.status === 'success' && result.ok && result.turno && !state.loadedJobs.has(job.job_id)) {{
        state.loadedJobs.add(job.job_id);
        state.payload.turnos = [result.turno, ...turnos()];
        state.activeIndex = 0;
        render();
      }}
    }}
    async function pollDownloadJob(jobId) {{
      const params = new URLSearchParams();
      params.set('job_id', jobId);
      const response = await fetch(`${{DOWNLOAD_STATUS_URL}}?${{params.toString()}}`);
      const job = await response.json();
      renderDownloadJob(job);
      if (['success', 'failed'].includes(job.status)) {{
        if (state.downloadPoll) clearInterval(state.downloadPoll);
        state.downloadPoll = null;
      }}
    }}
    document.getElementById('download-sample').addEventListener('click', async () => {{
      const status = document.getElementById('download-status');
      const params = new URLSearchParams();
      params.set('dias_busqueda', document.getElementById('download-days').value || '30');
      params.set('min_ch', document.getElementById('download-min-ch').value || '2');
      params.set('max_ch', document.getElementById('download-max-ch').value || '4');
      params.set('sample_percent', document.getElementById('download-percent').value || '10');
      const turno = document.getElementById('download-turno').value;
      if (turno) params.set('turno', turno);
      status.textContent = 'Iniciando...';
      document.getElementById('download-logs').textContent = 'Iniciando descarga local...';
      try {{
        const response = await fetch(`${{DOWNLOAD_URL}}?${{params.toString()}}`);
        const job = await response.json();
        if (!response.ok || !job.job_id) {{
          throw new Error(job.detail || 'No se pudo iniciar la descarga.');
        }}
        renderDownloadJob(job);
        if (state.downloadPoll) clearInterval(state.downloadPoll);
        state.downloadPoll = setInterval(() => pollDownloadJob(job.job_id).catch((error) => {{
          status.textContent = error.message || String(error);
        }}), 1000);
        pollDownloadJob(job.job_id).catch(() => {{}});
      }} catch (error) {{
        status.textContent = error.message || String(error);
      }}
    }});
    render();
  </script>
</body>
</html>"""
