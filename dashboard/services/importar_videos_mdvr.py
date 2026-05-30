import datetime
import errno
import hashlib
import json
import logging
import mimetypes
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass

from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from dashboard.models import (
    Camion,
    EstadoVelocidadesVideo,
    EstadoVideo,
    TipoTurnoChoices,
    Turno,
    Video,
)
from dashboard.services.calcular_duracion_video import (
    VIDEO_IMPORT_DURATION_TOLERANCE_SECONDS,
    calcular_duracion_video,
    envolver_h264_en_mp4,
    procesar_video_subida,
)
from dashboard.services.importar_velocidades_xlsx import importar_velocidades_xlsx
from dashboard.services.video_commands import (
    asegurar_permisos_archivo,
    build_ffmpeg_command,
    remove_if_exists,
    run_command,
    run_ffprobe_json,
    validation_error_message,
)
from dashboard.services.video_importacion import (
    asegurar_permisos_storage,
    inspeccionar_origen_importacion,
)


_DIR_FECHA_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ID_PREFIX_RE = re.compile(r"^(?P<id>\d+)")
_SEGMENTO_RE = re.compile(
    r"^(?P<equipo>\d+)-(?P<camara>\d{2})-(?P<inicio>\d{6})-(?P<fin>\d{6})-.*\.(?P<ext>h264|grec|mp4)$",
    re.IGNORECASE,
)
_SEGMENTO_GREC_NUEVO_RE = re.compile(
    r"^(?P<equipo>\d+)-(?P<fecha>\d{6})-(?P<inicio>\d{6})-(?P<fin>\d{6})-(?P<codigo>\d+)\.(?P<ext>grec|mp4)$",
    re.IGNORECASE,
)
_XLSX_RE = re.compile(
    r"^(?P<id>\d+)\s+"
    r"(?P<inicio_fecha>\d{4}-\d{2}-\d{2})\s+(?P<inicio_hora>\d{2}-\d{2}-\d{2})\s*~\s*"
    r"(?P<fin_fecha>\d{4}-\d{2}-\d{2})\s+(?P<fin_hora>\d{2}-\d{2}-\d{2})\.xlsx$",
    re.IGNORECASE,
)
_MIN_DURACION_ALINEACION_DEFAULT = 60
try:
    MIN_DURACION_ALINEACION_SEGUNDOS = int(
        os.environ.get(
            "MDVR_MIN_DURACION_ALINEACION_SEGUNDOS",
            _MIN_DURACION_ALINEACION_DEFAULT,
        )
    )
except ValueError:
    MIN_DURACION_ALINEACION_SEGUNDOS = _MIN_DURACION_ALINEACION_DEFAULT
_MAX_DESFASE_INICIO_ALINEACION_DEFAULT = 15
try:
    MAX_DESFASE_INICIO_ALINEACION_SEGUNDOS = int(
        os.environ.get(
            "MDVR_MAX_DESFASE_INICIO_ALINEACION_SEGUNDOS",
            _MAX_DESFASE_INICIO_ALINEACION_DEFAULT,
        )
    )
except ValueError:
    MAX_DESFASE_INICIO_ALINEACION_SEGUNDOS = _MAX_DESFASE_INICIO_ALINEACION_DEFAULT

_MAX_REINTENTOS_DEFAULT = 3
try:
    MAX_REINTENTOS_MDVR = int(os.environ.get("MDVR_MAX_REINTENTOS", _MAX_REINTENTOS_DEFAULT))
except ValueError:
    MAX_REINTENTOS_MDVR = _MAX_REINTENTOS_DEFAULT
MAX_REINTENTOS_MDVR = max(0, MAX_REINTENTOS_MDVR)

_BACKOFF_BASE_SEGUNDOS_DEFAULT = 300
try:
    BACKOFF_BASE_SEGUNDOS = int(
        os.environ.get("MDVR_BACKOFF_BASE_SEGUNDOS", _BACKOFF_BASE_SEGUNDOS_DEFAULT)
    )
except ValueError:
    BACKOFF_BASE_SEGUNDOS = _BACKOFF_BASE_SEGUNDOS_DEFAULT
BACKOFF_BASE_SEGUNDOS = max(1, BACKOFF_BASE_SEGUNDOS)

_BACKOFF_MAX_SEGUNDOS_DEFAULT = 7200
try:
    BACKOFF_MAX_SEGUNDOS = int(
        os.environ.get("MDVR_BACKOFF_MAX_SEGUNDOS", _BACKOFF_MAX_SEGUNDOS_DEFAULT)
    )
except ValueError:
    BACKOFF_MAX_SEGUNDOS = _BACKOFF_MAX_SEGUNDOS_DEFAULT
BACKOFF_MAX_SEGUNDOS = max(BACKOFF_BASE_SEGUNDOS, BACKOFF_MAX_SEGUNDOS)

_PROCESANDO_STALE_MINUTOS_DEFAULT = 120
try:
    PROCESANDO_STALE_MINUTOS = int(
        os.environ.get(
            "MDVR_PROCESANDO_STALE_MINUTOS",
            _PROCESANDO_STALE_MINUTOS_DEFAULT,
        )
    )
except ValueError:
    PROCESANDO_STALE_MINUTOS = _PROCESANDO_STALE_MINUTOS_DEFAULT
PROCESANDO_STALE_MINUTOS = max(1, PROCESANDO_STALE_MINUTOS)

_IMPORT_SOFT_TIME_LIMIT_DEFAULT = 21600
try:
    IMPORT_SOFT_TIME_LIMIT = int(
        os.environ.get("MDVR_IMPORT_SOFT_TIME_LIMIT", _IMPORT_SOFT_TIME_LIMIT_DEFAULT)
    )
except ValueError:
    IMPORT_SOFT_TIME_LIMIT = _IMPORT_SOFT_TIME_LIMIT_DEFAULT
IMPORT_SOFT_TIME_LIMIT = max(1, IMPORT_SOFT_TIME_LIMIT)

_PROCESANDO_LEASE_SEGUNDOS_DEFAULT = max(3600, IMPORT_SOFT_TIME_LIMIT + 900)
try:
    PROCESANDO_LEASE_SEGUNDOS = int(
        os.environ.get("MDVR_PROCESANDO_LEASE_SEGUNDOS", _PROCESANDO_LEASE_SEGUNDOS_DEFAULT)
    )
except ValueError:
    PROCESANDO_LEASE_SEGUNDOS = _PROCESANDO_LEASE_SEGUNDOS_DEFAULT
PROCESANDO_LEASE_SEGUNDOS = max(60, PROCESANDO_LEASE_SEGUNDOS)

_MAX_DETALLES_OMISION_DEFAULT = 300
try:
    MAX_DETALLES_OMISION = int(
        os.environ.get("MDVR_MAX_DETALLES_OMISION", _MAX_DETALLES_OMISION_DEFAULT)
    )
except ValueError:
    MAX_DETALLES_OMISION = _MAX_DETALLES_OMISION_DEFAULT
MAX_DETALLES_OMISION = max(0, MAX_DETALLES_OMISION)

_MIN_ANTIGUEDAD_ARCHIVO_DEFAULT = 180
try:
    MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS = int(
        os.environ.get("MDVR_MIN_FILE_AGE_SECONDS", _MIN_ANTIGUEDAD_ARCHIVO_DEFAULT)
    )
except ValueError:
    MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS = _MIN_ANTIGUEDAD_ARCHIVO_DEFAULT
MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS = max(0, MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS)

_GAP_PADDING_MIN_SEGUNDOS_DEFAULT = 3.0
try:
    GAP_PADDING_MIN_SEGUNDOS = float(
        os.environ.get("MDVR_GAP_PADDING_MIN_SECONDS", _GAP_PADDING_MIN_SEGUNDOS_DEFAULT)
    )
except ValueError:
    GAP_PADDING_MIN_SEGUNDOS = _GAP_PADDING_MIN_SEGUNDOS_DEFAULT
GAP_PADDING_MIN_SEGUNDOS = max(0.0, GAP_PADDING_MIN_SEGUNDOS)

MDVR_CORREGIR_TIMING_FPS = os.environ.get("MDVR_CORREGIR_TIMING_FPS", "1").lower() in {
    "1",
    "true",
    "yes",
}
MDVR_RECONSTRUIR_TIMELINE = os.environ.get("MDVR_RECONSTRUIR_TIMELINE", "0").lower() in {
    "1",
    "true",
    "yes",
}
try:
    MDVR_TIMING_MIN_FACTOR = float(os.environ.get("MDVR_TIMING_MIN_FACTOR", "1.05"))
except ValueError:
    MDVR_TIMING_MIN_FACTOR = 1.05
try:
    MDVR_TIMING_MAX_FACTOR = float(os.environ.get("MDVR_TIMING_MAX_FACTOR", "2.5"))
except ValueError:
    MDVR_TIMING_MAX_FACTOR = 2.5
try:
    MDVR_TIMING_MIN_FPS = float(os.environ.get("MDVR_TIMING_MIN_FPS", "5"))
except ValueError:
    MDVR_TIMING_MIN_FPS = 5.0
try:
    MDVR_TIMING_MAX_FPS = float(os.environ.get("MDVR_TIMING_MAX_FPS", "30"))
except ValueError:
    MDVR_TIMING_MAX_FPS = 30.0

MDVR_RECHAZAR_VIDEO_NEGRO = os.environ.get("MDVR_RECHAZAR_VIDEO_NEGRO", "1").lower() in {
    "1",
    "true",
    "yes",
}
try:
    MDVR_BLACK_MIN_DURATION_SECONDS = int(os.environ.get("MDVR_BLACK_MIN_DURATION_SECONDS", "10"))
except ValueError:
    MDVR_BLACK_MIN_DURATION_SECONDS = 10
MDVR_BLACK_MIN_DURATION_SECONDS = max(1, MDVR_BLACK_MIN_DURATION_SECONDS)
try:
    MDVR_BLACK_MAX_RATIO = float(os.environ.get("MDVR_BLACK_MAX_RATIO", "0.98"))
except ValueError:
    MDVR_BLACK_MAX_RATIO = 0.98
MDVR_BLACK_MAX_RATIO = min(max(MDVR_BLACK_MAX_RATIO, 0.5), 1.0)
try:
    MDVR_BLACK_PIX_TH = float(os.environ.get("MDVR_BLACK_PIX_TH", "0.03"))
except ValueError:
    MDVR_BLACK_PIX_TH = 0.03
MDVR_BLACK_PIX_TH = min(max(MDVR_BLACK_PIX_TH, 0.0), 1.0)
try:
    MDVR_BLACK_PIC_TH = float(os.environ.get("MDVR_BLACK_PIC_TH", "0.995"))
except ValueError:
    MDVR_BLACK_PIC_TH = 0.995
MDVR_BLACK_PIC_TH = min(max(MDVR_BLACK_PIC_TH, 0.5), 1.0)
MDVR_BLACK_ANALYSIS_MODE = os.environ.get("MDVR_BLACK_ANALYSIS_MODE", "sample").strip().lower()
if MDVR_BLACK_ANALYSIS_MODE not in {"sample", "full"}:
    MDVR_BLACK_ANALYSIS_MODE = "sample"
try:
    MDVR_BLACK_SAMPLE_COUNT = int(os.environ.get("MDVR_BLACK_SAMPLE_COUNT", "7"))
except ValueError:
    MDVR_BLACK_SAMPLE_COUNT = 7
MDVR_BLACK_SAMPLE_COUNT = min(max(MDVR_BLACK_SAMPLE_COUNT, 3), 25)
try:
    MDVR_BLACK_MIN_VALID_SAMPLES = int(os.environ.get("MDVR_BLACK_MIN_VALID_SAMPLES", "3"))
except ValueError:
    MDVR_BLACK_MIN_VALID_SAMPLES = 3
MDVR_BLACK_MIN_VALID_SAMPLES = min(max(MDVR_BLACK_MIN_VALID_SAMPLES, 1), 25)
try:
    MDVR_BLACK_SAMPLE_WIDTH = int(os.environ.get("MDVR_BLACK_SAMPLE_WIDTH", "64"))
except ValueError:
    MDVR_BLACK_SAMPLE_WIDTH = 64
MDVR_BLACK_SAMPLE_WIDTH = min(max(MDVR_BLACK_SAMPLE_WIDTH, 16), 320)
try:
    MDVR_BLACK_SAMPLE_HEIGHT = int(os.environ.get("MDVR_BLACK_SAMPLE_HEIGHT", "36"))
except ValueError:
    MDVR_BLACK_SAMPLE_HEIGHT = 36
MDVR_BLACK_SAMPLE_HEIGHT = min(max(MDVR_BLACK_SAMPLE_HEIGHT, 16), 240)
try:
    MDVR_BLACK_FRAME_TIMEOUT_SECONDS = int(
        os.environ.get("MDVR_BLACK_FRAME_TIMEOUT_SECONDS", "20")
    )
except ValueError:
    MDVR_BLACK_FRAME_TIMEOUT_SECONDS = 20
MDVR_BLACK_FRAME_TIMEOUT_SECONDS = max(3, MDVR_BLACK_FRAME_TIMEOUT_SECONDS)

RAW_VIDEO_EXTENSIONS = {".h264", ".grec"}
TRANSIENT_ERRNOS = {
    errno.EAGAIN,
    errno.EBUSY,
    errno.EINTR,
    errno.ETIMEDOUT,
    errno.ECONNRESET,
    errno.ENETDOWN,
    errno.ENETRESET,
    errno.ENETUNREACH,
    errno.EHOSTUNREACH,
}
logger = logging.getLogger(__name__)


class VideoSinImagenUtilError(ValidationError):
    pass


@dataclass(frozen=True)
class SegmentoVideo:
    ruta: str
    camara: int
    inicio_dt: datetime.datetime
    fin_dt: datetime.datetime
    extension: str


@dataclass(frozen=True)
class XlsxInfo:
    ruta: str
    inicio: datetime.datetime
    fin: datetime.datetime


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
        ano = 2000 + int(valor[0:2])
        mes = int(valor[2:4])
        dia = int(valor[4:6])
        return datetime.date(ano, mes, dia)
    except ValueError:
        return None


def _extraer_camara_desde_codigo_nuevo(codigo: str) -> int | None:
    """
    Formato observado: 20010100 / 20010200 / 20010300.
    Se toma la pareja central para inferir cámara (01..04).
    """
    if not codigo or len(codigo) < 6 or not codigo.isdigit():
        return None
    camara_txt = codigo[4:6]
    try:
        camara = int(camara_txt)
    except ValueError:
        return None
    if camara not in {1, 2, 3, 4}:
        return None
    return camara


def _parse_hora_hh_mm_ss(valor: str) -> datetime.time | None:
    if not valor:
        return None
    parts = valor.split("-")
    if len(parts) != 3:
        return None
    try:
        hh, mm, ss = [int(p) for p in parts]
    except ValueError:
        return None
    if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
        return None
    return datetime.time(hh, mm, ss)


def _tipo_turno_para_hora(hora: datetime.time) -> str:
    if hora < datetime.time(8, 0):
        return TipoTurnoChoices.NOCHE
    if hora < datetime.time(16, 0):
        return TipoTurnoChoices.MANANA
    return TipoTurnoChoices.TARDE


def _buscar_carpeta_mdvr(base_dir: str, carpeta_id: str) -> str | None:
    try:
        entries = os.listdir(base_dir)
    except OSError:
        return None
    for nombre in entries:
        ruta = os.path.join(base_dir, nombre)
        if not os.path.isdir(ruta):
            continue
        match = _ID_PREFIX_RE.match(nombre)
        if match and match.group("id") == carpeta_id:
            return ruta
    return None


def _listar_xlsx(base_dir: str, carpeta_id: str) -> list[XlsxInfo]:
    resultados = []
    try:
        entries = os.listdir(base_dir)
    except OSError:
        return resultados
    for nombre in entries:
        match = _XLSX_RE.match(nombre)
        if not match or match.group("id") != carpeta_id:
            continue
        hora_inicio = _parse_hora_hh_mm_ss(match.group("inicio_hora"))
        hora_fin = _parse_hora_hh_mm_ss(match.group("fin_hora"))
        if not hora_inicio or not hora_fin:
            continue
        try:
            fecha_inicio = datetime.date.fromisoformat(match.group("inicio_fecha"))
            fecha_fin = datetime.date.fromisoformat(match.group("fin_fecha"))
        except ValueError:
            continue
        inicio_dt = datetime.datetime.combine(fecha_inicio, hora_inicio)
        fin_dt = datetime.datetime.combine(fecha_fin, hora_fin)
        if fin_dt < inicio_dt:
            continue
        resultados.append(
            XlsxInfo(ruta=os.path.join(base_dir, nombre), inicio=inicio_dt, fin=fin_dt)
        )
    return resultados


def _obtener_o_crear_turno(
    *, fecha: datetime.date, camion: Camion, tipo_turno: str, errores: list[str]
) -> Turno:
    """
    Resuelve duplicados de turnos sin abortar la importación completa.
    Si hay más de un turno para la misma clave, reutiliza el más antiguo.
    """
    qs = Turno.objects.filter(
        fecha=fecha,
        id_camion=camion,
        tipo_turno=tipo_turno,
    ).order_by("id")
    turno = qs.first()
    if turno:
        if qs.count() > 1:
            errores.append(
                f"Turno duplicado detectado para {fecha} ({tipo_turno}); se usará id={turno.id}."
            )
        return turno
    return Turno.objects.create(
        fecha=fecha,
        id_camion=camion,
        tipo_turno=tipo_turno,
        activo=False,
    )


def _seleccionar_xlsx(xlsx_files: list[XlsxInfo], fecha_inicio: datetime.datetime) -> XlsxInfo | None:
    candidatos = [
        info
        for info in xlsx_files
        if info.inicio <= fecha_inicio <= info.fin
    ]
    if not candidatos:
        return None
    return min(candidatos, key=lambda info: info.fin - info.inicio)


def _programar_importacion_velocidades_turno(
    *,
    turno: Turno,
    video_ref: Video,
    xlsx_files: list[XlsxInfo],
    fecha_inicio: datetime.datetime,
    xlsx_por_turno: dict[int, str],
    videos_referencia_por_turno: dict[int, Video],
):
    xlsx_info = _seleccionar_xlsx(xlsx_files, fecha_inicio)
    if xlsx_info:
        if turno.id not in xlsx_por_turno:
            xlsx_por_turno[turno.id] = xlsx_info.ruta
            videos_referencia_por_turno[turno.id] = video_ref
        _actualizar_estado_velocidades_turno(
            turno,
            EstadoVelocidadesVideo.PENDIENTE,
        )
        return True

    _actualizar_estado_velocidades_turno(
        turno,
        EstadoVelocidadesVideo.SIN_XLSX,
        error="No se encontró XLSX que cubra la fecha/hora de inicio del video.",
    )
    return False


def _normalizar_error(exc: Exception) -> str:
    mensaje = (str(exc) or exc.__class__.__name__).strip()
    if not mensaje:
        mensaje = exc.__class__.__name__
    return mensaje[:2000]


def _resumir_texto_error_subprocess(valor: str | None, limite: int = 800) -> str:
    texto = (valor or "").strip()
    if not texto:
        return ""
    texto = re.sub(r"\s+", " ", texto)
    if len(texto) > limite:
        return f"{texto[:limite]}..."
    return texto


def _mensaje_error_subprocess(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        detalle = _resumir_texto_error_subprocess(exc.stderr) or _resumir_texto_error_subprocess(
            exc.stdout
        )
        mensaje = f"ffmpeg devolvió código {exc.returncode}"
        if detalle:
            mensaje = f"{mensaje}. detalle: {detalle}"
        return mensaje
    return _normalizar_error(exc)


def _actualizar_estado_velocidades(
    video: Video,
    estado: str,
    *,
    error: str = "",
    actualizado_en: datetime.datetime | None = None,
):
    error_limpio = (error or "").strip()[:2000]
    cambios = []
    if video.estado_velocidades != estado:
        video.estado_velocidades = estado
        cambios.append("estado_velocidades")
    if video.velocidades_error != error_limpio:
        video.velocidades_error = error_limpio
        cambios.append("velocidades_error")
    if video.velocidades_actualizadas_en != actualizado_en:
        video.velocidades_actualizadas_en = actualizado_en
        cambios.append("velocidades_actualizadas_en")
    if cambios:
        video.save(update_fields=cambios)


def _actualizar_estado_velocidades_turno(
    turno: Turno,
    estado: str,
    *,
    error: str = "",
    actualizado_en: datetime.datetime | None = None,
):
    error_limpio = (error or "").strip()[:2000]
    Video.objects.filter(id_turno=turno).update(
        estado_velocidades=estado,
        velocidades_error=error_limpio,
        velocidades_actualizadas_en=actualizado_en,
    )


def _es_error_transitorio(exc: Exception) -> bool:
    if isinstance(exc, (SoftTimeLimitExceeded, TimeoutError, subprocess.TimeoutExpired)):
        return True
    if isinstance(exc, ValidationError):
        return False
    if isinstance(exc, subprocess.CalledProcessError):
        return False
    if isinstance(exc, OSError):
        return exc.errno in TRANSIENT_ERRNOS
    return False


def _calcular_backoff_reintento(reintento_num: int) -> datetime.timedelta:
    exponente = max(0, reintento_num - 1)
    segundos = BACKOFF_BASE_SEGUNDOS * (2**exponente)
    segundos = min(segundos, BACKOFF_MAX_SEGUNDOS)
    return datetime.timedelta(seconds=segundos)


def _proximo_reintento_desde(video: Video, ahora: datetime.datetime) -> datetime.datetime | None:
    proximo = getattr(video, "proximo_reintento_en", None)
    if proximo is None:
        return None
    if timezone.is_naive(proximo):
        return timezone.make_aware(proximo, timezone.get_current_timezone())
    return proximo


def _es_procesando_stale(video: Video, ahora: datetime.datetime) -> bool:
    proximo = _proximo_reintento_desde(video, ahora)
    if proximo is not None:
        return proximo <= ahora
    creado_en = getattr(video, "creado_en", None)
    if creado_en is None:
        return True
    if timezone.is_naive(creado_en):
        creado_en = timezone.make_aware(creado_en, timezone.get_current_timezone())
    umbral = ahora - datetime.timedelta(minutes=PROCESANDO_STALE_MINUTOS)
    return creado_en <= umbral


def _puede_reprocesarse(video: Video, ahora: datetime.datetime) -> bool:
    if video.estado in {EstadoVideo.LISTO, EstadoVideo.ERROR_PERMANENTE}:
        return False
    if video.estado == EstadoVideo.PROCESANDO:
        return _es_procesando_stale(video, ahora)
    if video.estado == EstadoVideo.ERROR:
        if (video.reintentos or 0) >= MAX_REINTENTOS_MDVR:
            return False
        proximo = _proximo_reintento_desde(video, ahora)
        if proximo and proximo > ahora:
            return False
        return True
    return True


def _marcar_video_para_reintento(video: Video, ahora: datetime.datetime, error: Exception) -> str:
    mensaje = _normalizar_error(error)
    reintentos_actuales = int(video.reintentos or 0)
    transitorio = _es_error_transitorio(error)
    if hasattr(video, "detalle_error"):
        video.detalle_error = mensaje
    if hasattr(video, "error_tipo"):
        video.error_tipo = "transitorio" if transitorio else "procesamiento"

    if transitorio and reintentos_actuales < MAX_REINTENTOS_MDVR:
        nuevo_reintento = reintentos_actuales + 1
        video.estado = EstadoVideo.ERROR
        video.reintentos = nuevo_reintento
        video.ultimo_error = mensaje
        video.proximo_reintento_en = ahora + _calcular_backoff_reintento(nuevo_reintento)
        video.save(
            update_fields=[
                "estado",
                "reintentos",
                "ultimo_error",
                "proximo_reintento_en",
                "detalle_error",
                "error_tipo",
            ]
        )
        return (
            f"error transitorio ({mensaje}). "
            f"Reintento {nuevo_reintento}/{MAX_REINTENTOS_MDVR} programado."
        )

    sufijo = ""
    if transitorio and reintentos_actuales >= MAX_REINTENTOS_MDVR:
        sufijo = f" Reintentos agotados ({MAX_REINTENTOS_MDVR}/{MAX_REINTENTOS_MDVR})."
    video.estado = EstadoVideo.ERROR_PERMANENTE
    video.ultimo_error = f"{mensaje}{sufijo}"
    video.proximo_reintento_en = None
    video.save(
        update_fields=[
            "estado",
            "ultimo_error",
            "proximo_reintento_en",
            "detalle_error",
            "error_tipo",
        ]
    )
    return f"error permanente ({video.ultimo_error})"


def _normalizar_video_exitoso(video: Video):
    cambios = []
    if video.reintentos != 0:
        video.reintentos = 0
        cambios.append("reintentos")
    if video.ultimo_error:
        video.ultimo_error = ""
        cambios.append("ultimo_error")
    if video.proximo_reintento_en is not None:
        video.proximo_reintento_en = None
        cambios.append("proximo_reintento_en")
    if getattr(video, "detalle_error", ""):
        video.detalle_error = ""
        cambios.append("detalle_error")
    if getattr(video, "error_tipo", ""):
        video.error_tipo = ""
        cambios.append("error_tipo")
    if cambios:
        video.save(update_fields=cambios)


def _rehidratar_metadata_video_existente(video: Video):
    necesita_duracion = video.duracion is None or video.duracion <= 0
    necesita_fin = video.fecha_inicio is not None and (
        video.fecha_fin is None or video.fin_timestamp is None
    )
    necesita_mimetype = not (video.mimetype or "").strip()
    if not (necesita_duracion or necesita_fin or necesita_mimetype):
        return

    ruta_archivo = getattr(video, "ruta_archivo", None)
    if not ruta_archivo or not getattr(ruta_archivo, "name", ""):
        return

    try:
        ruta = ruta_archivo.path
    except Exception:
        return

    if not ruta or not os.path.exists(ruta):
        return

    cambios = []
    duracion_recalculada = None
    if necesita_duracion or necesita_fin:
        try:
            duracion_recalculada = math.floor(calcular_duracion_video(ruta))
        except Exception:
            duracion_recalculada = None

    if necesita_duracion and duracion_recalculada is not None and duracion_recalculada > 0:
        video.duracion = duracion_recalculada
        cambios.append("duracion")

    duracion_base = video.duracion
    if (duracion_base is None or duracion_base <= 0) and duracion_recalculada is not None:
        duracion_base = duracion_recalculada

    if necesita_fin and duracion_base is not None and duracion_base > 0:
        fecha_fin = video.fecha_inicio + datetime.timedelta(seconds=int(duracion_base))
        fin = fecha_fin.timetz().replace(tzinfo=None)
        if video.fecha_fin != fecha_fin:
            video.fecha_fin = fecha_fin
            cambios.append("fecha_fin")
        if video.fin_timestamp != fin:
            video.fin_timestamp = fin
            cambios.append("fin_timestamp")

    if necesita_mimetype:
        mimetype = mimetypes.guess_type(ruta)[0] or ""
        if mimetype and video.mimetype != mimetype:
            video.mimetype = mimetype
            cambios.append("mimetype")

    if cambios:
        video.save(update_fields=cambios)


def _grupo_origen_mdvr(carpeta_id: str, fecha: datetime.date, tipo_turno: str, camara: int) -> str:
    return f"mdvr:{carpeta_id}:{fecha.isoformat()}:{tipo_turno}:camara:{camara}"


def _inspeccionar_segmentos_mdvr(
    segmentos: list[SegmentoVideo],
    *,
    carpeta_id: str,
    fecha: datetime.date,
    tipo_turno: str,
    camara: int,
):
    detalles = []
    hash_grupo = hashlib.sha256()
    total_bytes = 0
    ultima_modificacion = None

    for segmento in segmentos:
        inspeccion = inspeccionar_origen_importacion(segmento.ruta)
        ruta_rel = os.path.basename(segmento.ruta)
        hash_grupo.update(ruta_rel.encode("utf-8"))
        hash_grupo.update(b"|")
        hash_grupo.update(inspeccion["sha256"].encode("utf-8"))
        hash_grupo.update(b"\n")
        total_bytes += inspeccion["tamano_bytes"]
        if ultima_modificacion is None or inspeccion["modificado_en"] > ultima_modificacion:
            ultima_modificacion = inspeccion["modificado_en"]
        detalles.append(
            {
                "ruta": segmento.ruta,
                "ruta_rel": ruta_rel,
                "camara": segmento.camara,
                "inicio_dt": segmento.inicio_dt,
                "fin_dt": segmento.fin_dt,
                "extension": segmento.extension,
                **inspeccion,
            }
        )

    return {
        "grupo_origen": _grupo_origen_mdvr(carpeta_id, fecha, tipo_turno, camara),
        "segmentos": detalles,
        "segmentos_origen": [item["ruta_rel"] for item in detalles],
        "ruta_origen": detalles[0]["ruta_rel"],
        "origen_sha256": hash_grupo.hexdigest(),
        "origen_tamano_bytes": total_bytes,
        "origen_modificado_en": ultima_modificacion,
    }


def _registrar_omision(
    detalles: dict,
    *,
    motivo: str,
    ruta_archivo: str | None = None,
    nombre_video: str | None = None,
    omision_video: bool = False,
):
    motivo_limpio = (motivo or "motivo no especificado").strip()
    if not motivo_limpio:
        motivo_limpio = "motivo no especificado"

    resumen = detalles.setdefault("motivos_omision", {})
    resumen[motivo_limpio] = int(resumen.get(motivo_limpio, 0)) + 1

    if omision_video:
        detalles["videos_omitidos"] += 1

    registro = {"motivo": motivo_limpio}
    contexto_log = "desconocido"
    if ruta_archivo:
        detalles["archivos_omitidos"] += 1
        contexto = os.path.join(
            os.path.basename(os.path.dirname(ruta_archivo)),
            os.path.basename(ruta_archivo),
        )
        registro["archivo"] = contexto
        contexto_log = ruta_archivo
    elif nombre_video:
        registro["video"] = nombre_video
        contexto_log = nombre_video

    mensaje_error = motivo_limpio
    if "archivo" in registro:
        mensaje_error = f"{registro['archivo']}: {motivo_limpio}"
    elif "video" in registro:
        mensaje_error = f"{registro['video']}: {motivo_limpio}"

    omisiones = detalles.setdefault("omisiones", [])
    if len(omisiones) < MAX_DETALLES_OMISION:
        omisiones.append(registro)
    elif len(omisiones) == MAX_DETALLES_OMISION:
        omisiones.append(
            {
                "motivo": (
                    f"Se alcanzó el límite de omisiones detalladas ({MAX_DETALLES_OMISION})."
                )
            }
        )

    errores = detalles.setdefault("errores", [])
    if len(errores) < MAX_DETALLES_OMISION:
        errores.append(f"omisión: {mensaje_error}")
    elif len(errores) == MAX_DETALLES_OMISION:
        errores.append(
            f"omisión: se alcanzó el límite de detalles ({MAX_DETALLES_OMISION})."
        )

    logger.warning("MDVR omitido [%s]: %s", contexto_log, motivo_limpio)


def _segmento_desde_archivo_con_motivo(
    ruta: str,
    fecha: datetime.date,
) -> tuple[SegmentoVideo | None, str | None]:
    nombre = os.path.basename(ruta)
    match = _SEGMENTO_RE.match(nombre)
    camara = None
    inicio_raw = None
    fin_raw = None
    ext = None

    if match:
        camara_raw = match.group("camara")
        try:
            camara = int(camara_raw)
        except ValueError:
            return None, f"cámara inválida en nombre ({camara_raw})."
        inicio_raw = match.group("inicio")
        fin_raw = match.group("fin")
        ext = f".{(match.group('ext') or '').lower()}"
    else:
        match_nuevo = _SEGMENTO_GREC_NUEVO_RE.match(nombre)
        if not match_nuevo:
            return None, "nombre no coincide con formatos MDVR soportados."
        fecha_archivo = _parse_fecha_yymmdd(match_nuevo.group("fecha"))
        if not fecha_archivo:
            return None, "fecha YYMMDD inválida en nombre."
        if fecha_archivo != fecha:
            return (
                None,
                (
                    "fecha en nombre no coincide con carpeta: "
                    f"{fecha_archivo.isoformat()} vs {fecha.isoformat()}."
                ),
            )
        camara = _extraer_camara_desde_codigo_nuevo(match_nuevo.group("codigo"))
        if camara is None:
            return None, "cámara inválida en código del nombre."
        inicio_raw = match_nuevo.group("inicio")
        fin_raw = match_nuevo.group("fin")
        ext = f".{(match_nuevo.group('ext') or '').lower()}"

    if camara not in {1, 2, 3, 4}:
        return None, f"cámara fuera de rango ({camara})."
    inicio = _parse_hora_hhmmss(inicio_raw)
    fin = _parse_hora_hhmmss(fin_raw)
    if not inicio or not fin:
        return None, "hora de inicio o fin inválida en nombre."
    inicio_dt = datetime.datetime.combine(fecha, inicio)
    fin_dt = datetime.datetime.combine(fecha, fin)
    if fin_dt <= inicio_dt:
        fin_dt += datetime.timedelta(days=1)
    extension = ext or os.path.splitext(nombre)[1].lower()
    return (
        SegmentoVideo(
            ruta=ruta,
            camara=camara,
            inicio_dt=inicio_dt,
            fin_dt=fin_dt,
            extension=extension,
        ),
        None,
    )


def _segmento_desde_archivo(ruta: str, fecha: datetime.date) -> SegmentoVideo | None:
    segmento, _motivo = _segmento_desde_archivo_con_motivo(ruta, fecha)
    return segmento


def _prioridad_segmento_para_importar(segmento: SegmentoVideo) -> tuple[int, int, float]:
    if segmento.extension == ".mp4":
        prioridad_extension = 2 if _mp4_directo_seguro(segmento.ruta) else 0
    else:
        prioridad_extension = 1
    try:
        stat = os.stat(segmento.ruta)
        tamano = int(stat.st_size or 0)
        modificado = float(stat.st_mtime or 0)
    except OSError:
        tamano = 0
        modificado = 0.0
    return prioridad_extension, tamano, modificado


def _deduplicar_segmentos_preferir_mp4(
    segmentos: list[SegmentoVideo],
) -> list[SegmentoVideo]:
    """
    CMSV6 puede dejar el crudo y el MP4 final para el mismo canal/rango.
    En ese caso el MP4 es la fuente visible que queremos registrar.
    """
    deduplicados: dict[tuple[int, datetime.datetime, datetime.datetime], SegmentoVideo] = {}
    for segmento in segmentos:
        key = (segmento.camara, segmento.inicio_dt, segmento.fin_dt)
        actual = deduplicados.get(key)
        if actual is None:
            deduplicados[key] = segmento
            continue
        if _prioridad_segmento_para_importar(segmento) > _prioridad_segmento_para_importar(actual):
            deduplicados[key] = segmento
    return sorted(
        deduplicados.values(),
        key=lambda item: (item.inicio_dt, item.camara, item.ruta),
    )


def _archivo_listo_para_importar(ruta_archivo: str) -> tuple[bool, str | None]:
    try:
        st = os.stat(ruta_archivo)
    except OSError as exc:
        return False, f"no se pudo leer metadatos de archivo ({exc})."

    if st.st_size <= 0:
        return False, "archivo vacío (0 bytes)."

    if MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS <= 0:
        return True, None

    edad_segundos = float(time.time()) - float(st.st_mtime)
    if edad_segundos < MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS:
        faltan = max(1, math.ceil(MIN_ANTIGUEDAD_ARCHIVO_SEGUNDOS - edad_segundos))
        return (
            False,
            (
                "archivo en subida o reciente; "
                f"esperar ~{faltan}s para procesarlo."
            ),
        )

    firma_inicial = (st.st_size, getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
    time.sleep(0.5)
    try:
        st_final = os.stat(ruta_archivo)
    except OSError as exc:
        return False, f"no se pudo revalidar metadatos del archivo ({exc})."
    firma_final = (
        st_final.st_size,
        getattr(st_final, "st_mtime_ns", int(st_final.st_mtime * 1_000_000_000)),
    )
    if firma_final != firma_inicial:
        return False, "archivo aún en cambio; espere a que finalice la subida."

    return True, None


def _es_extension_video_soportada(nombre: str) -> bool:
    ext = os.path.splitext(nombre)[1].lower()
    return ext in {".mp4", ".h264", ".grec"}


def _ffprobe_ok(ruta: str) -> bool:
    try:
        subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name",
                "-of",
                "json",
                ruta,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
    return True


def _parsear_fraccion_ffprobe(valor: str | None) -> float | None:
    if not valor:
        return None
    if "/" not in valor:
        try:
            return float(valor)
        except ValueError:
            return None
    num, den = valor.split("/", 1)
    try:
        num_f = float(num)
        den_f = float(den)
    except ValueError:
        return None
    if den_f == 0:
        return None
    return num_f / den_f


def _obtener_parametros_padding_video(ruta: str) -> tuple[int, int, str]:
    ancho = 1280
    alto = 720
    fps_expr = "25"
    try:
        data = run_ffprobe_json(
            ruta,
            show_entries="stream=width,height,avg_frame_rate,r_frame_rate",
            error_prefix="No se pudo inspeccionar video MDVR",
        )
    except ValidationError:
        return ancho, alto, fps_expr

    for stream in data.get("streams", []):
        try:
            ancho_stream = int(stream.get("width") or 0)
            alto_stream = int(stream.get("height") or 0)
        except (TypeError, ValueError):
            continue
        if ancho_stream <= 0 or alto_stream <= 0:
            continue
        ancho = ancho_stream
        alto = alto_stream
        candidato = stream.get("avg_frame_rate") or stream.get("r_frame_rate")
        fps_val = _parsear_fraccion_ffprobe(candidato)
        if fps_val and fps_val > 0:
            fps_expr = candidato
        break
    return ancho, alto, fps_expr


def _duracion_hueco_entre_segmentos(
    segmento_actual: SegmentoVideo,
    duracion_actual: float,
    siguiente_segmento: SegmentoVideo,
) -> float:
    duracion_base = max(0.0, float(duracion_actual or 0.0))
    fin_real_actual = segmento_actual.inicio_dt + datetime.timedelta(seconds=duracion_base)
    hueco = float((siguiente_segmento.inicio_dt - fin_real_actual).total_seconds())
    if hueco < GAP_PADDING_MIN_SEGUNDOS:
        return 0.0
    return hueco


def _planificar_timeline_segmentos(segmentos: list[SegmentoVideo]) -> tuple[list[dict], float]:
    if not segmentos:
        return [], 0.0

    duraciones = [max(1.0, _duracion_segmento_real(seg)) for seg in segmentos]
    plan: list[dict] = []
    cursor = 0.0
    total = len(segmentos)
    for idx, (segmento, duracion_seg) in enumerate(zip(segmentos, duraciones), start=1):
        inicio_timeline = cursor
        fin_timeline = inicio_timeline + duracion_seg
        hueco_despues = 0.0
        if idx < total:
            hueco_despues = _duracion_hueco_entre_segmentos(
                segmento,
                duracion_seg,
                segmentos[idx],
            )
        plan.append(
            {
                "segmento": segmento,
                "duracion_segundos": duracion_seg,
                "timeline_inicio": inicio_timeline,
                "timeline_fin": fin_timeline,
                "hueco_despues": hueco_despues,
            }
        )
        cursor = fin_timeline + hueco_despues
    return plan, cursor


def _segmentos_contiguos_hasta_primer_hueco(
    segmentos: list[SegmentoVideo],
) -> tuple[list[SegmentoVideo], dict | None]:
    if not segmentos:
        return [], None

    duraciones = [max(1.0, _duracion_segmento_real(seg)) for seg in segmentos]
    for idx in range(len(segmentos) - 1):
        hueco = _duracion_hueco_entre_segmentos(
            segmentos[idx],
            duraciones[idx],
            segmentos[idx + 1],
        )
        if hueco >= GAP_PADDING_MIN_SEGUNDOS:
            return segmentos[: idx + 1], {
                "duracion_segundos": float(hueco),
                "segmento_previo": segmentos[idx],
                "segmento_siguiente": segmentos[idx + 1],
                "segmentos_omitidos": len(segmentos) - (idx + 1),
            }
    return list(segmentos), None


def _mensaje_video_cortado_en_hueco(info_hueco: dict | None) -> str:
    if not info_hueco:
        return ""
    hueco = int(round(float(info_hueco.get("duracion_segundos") or 0.0)))
    segmento_previo = info_hueco.get("segmento_previo")
    segmento_siguiente = info_hueco.get("segmento_siguiente")
    omitidos = int(info_hueco.get("segmentos_omitidos") or 0)
    previo_nombre = os.path.basename(getattr(segmento_previo, "ruta", "") or "")
    siguiente_nombre = os.path.basename(getattr(segmento_siguiente, "ruta", "") or "")
    return (
        "Se procesó solo el tramo continuo inicial del video: "
        f"se detectó un hueco de aproximadamente {hueco}s entre "
        f"'{previo_nombre}' y '{siguiente_nombre}'. "
        f"Quedaron {omitidos} segmento(s) pendientes después del hueco; "
        "el video se regenerará cuando aparezca continuidad."
    )[:2000]


def _crear_padding_negro_mp4(segundos: float, referencia_ruta: str) -> str:
    ancho, alto, fps_expr = _obtener_parametros_padding_video(referencia_ruta)
    ruta_padding = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
    run_command(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"color=c=black:s={ancho}x{alto}:r={fps_expr}:d={segundos:.3f}",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            ruta_padding,
        ],
        error_prefix="No se pudo generar padding negro MDVR",
    )
    if os.path.getsize(ruta_padding) <= 0:
        remove_if_exists(ruta_padding)
        raise ValidationError("No se pudo generar padding negro MDVR.")
    return ruta_padding


def _preparar_segmentos_para_concat(
    segmentos: list[SegmentoVideo],
) -> tuple[list[str], list[str], bool]:
    son_mp4 = all(seg.extension == ".mp4" for seg in segmentos)
    temporales: list[str] = []
    if son_mp4:
        rutas_mp4 = [seg.ruta for seg in segmentos]
    else:
        rutas_mp4, temporales = _normalizar_segmentos_raw_a_mp4(segmentos)
    return rutas_mp4, temporales, False


def _crear_lista_concat(segmentos: list[str]) -> str:
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as archivo:
        for ruta in segmentos:
            ruta_esc = ruta.replace("'", r"'\''")
            archivo.write(f"file '{ruta_esc}'\n")
        return archivo.name


def _concat_h264(segmentos: list[str], salida: str) -> tuple[bool, str | None]:
    try:
        with open(salida, "wb") as out_file:
            for ruta in segmentos:
                with open(ruta, "rb") as in_file:
                    shutil.copyfileobj(in_file, out_file, length=1024 * 1024)
        if os.path.getsize(salida) <= 0:
            return False, "salida vacía tras concatenación binaria."
        return True, None
    except OSError as exc:
        return False, str(exc)


def _concat_mp4_copiando(segmentos: list[str], salida: str) -> tuple[bool, str | None]:
    lista = _crear_lista_concat(segmentos)
    try:
        run_command(
            build_ffmpeg_command(
                lista,
                salida,
                input_args=["-hide_banner", "-loglevel", "error", "-f", "concat", "-safe", "0"],
                output_args=["-c", "copy", "-movflags", "+faststart"],
            )
        )
        if os.path.getsize(salida) <= 0:
            return False, "salida vacía tras concatenación MP4 por copia."
        return True, None
    except (ValidationError, OSError) as exc:
        return False, validation_error_message(exc)
    finally:
        if os.path.exists(lista):
            os.remove(lista)


def _concat_mp4_transcodificando(segmentos: list[str], salida: str) -> tuple[bool, str | None]:
    lista = _crear_lista_concat(segmentos)
    try:
        run_command(
            build_ffmpeg_command(
                lista,
                salida,
                input_args=["-hide_banner", "-loglevel", "error", "-f", "concat", "-safe", "0"],
                output_args=[
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-crf",
                    "23",
                    "-pix_fmt",
                    "yuv420p",
                    "-an",
                ],
            )
        )
        return True, None
    except (ValidationError, OSError) as exc:
        return False, validation_error_message(exc)
    finally:
        if os.path.exists(lista):
            os.remove(lista)


_concat_h264_transcodificando = _concat_mp4_transcodificando


def _normalizar_segmentos_raw_a_mp4(segmentos: list[SegmentoVideo]) -> tuple[list[str], list[str]]:
    rutas_mp4 = []
    temporales = []
    for segmento in segmentos:
        if segmento.extension == ".mp4":
            rutas_mp4.append(segmento.ruta)
            continue
        ruta_temporal_raw = tempfile.NamedTemporaryFile(delete=False, suffix=segmento.extension).name
        shutil.copyfile(segmento.ruta, ruta_temporal_raw)
        ruta_mp4 = envolver_h264_en_mp4(ruta_temporal_raw)
        temporales.extend([ruta_temporal_raw, ruta_mp4])
        rutas_mp4.append(ruta_mp4)
    return rutas_mp4, temporales


def _concatenar_segmentos(segmentos: list[SegmentoVideo], salida: str) -> tuple[bool, str | None]:
    rutas = [seg.ruta for seg in segmentos]
    if not rutas:
        return False, "sin segmentos para concatenar."

    son_mp4 = all(seg.extension == ".mp4" for seg in segmentos)
    try:
        rutas_preparadas, temporales, hubo_padding = _preparar_segmentos_para_concat(segmentos)
    except Exception as exc:
        return False, _normalizar_error(exc)

    if son_mp4 and not hubo_padding:
        ok, error = _concat_mp4_copiando(rutas_preparadas, salida)
        if ok:
            return True, None
        ok, error = _concat_h264_transcodificando(rutas_preparadas, salida)
        if ok:
            return True, None
        return False, error

    try:
        if not hubo_padding:
            ok, error = _concat_mp4_copiando(rutas_preparadas, salida)
            if ok:
                return True, None
        ok, error = _concat_h264_transcodificando(rutas_preparadas, salida)
        if ok:
            return True, None
        return False, error
    finally:
        for ruta in temporales:
            remove_if_exists(ruta)


def _subir_archivo_temporal(ruta_local: str, nombre_base: str) -> str:
    destino = default_storage.get_available_name(os.path.join("videos", nombre_base))
    with open(ruta_local, "rb") as archivo:
        destino = default_storage.save(destino, File(archivo))
    asegurar_permisos_storage(destino)
    return destino


def _archivo_tiene_ftyp_mp4(ruta: str) -> bool:
    try:
        with open(ruta, "rb") as archivo:
            data = archivo.read(12)
    except OSError:
        return False
    return len(data) >= 8 and data[4:8] == b"ftyp"


def _archivo_parece_ssy_dvr(ruta: str) -> bool:
    try:
        with open(ruta, "rb") as archivo:
            data = archivo.read(4096)
    except OSError:
        return False
    return b"SSY_DVR" in data


def _mp4_directo_seguro(ruta: str) -> bool:
    if not _archivo_tiene_ftyp_mp4(ruta):
        return False
    if _archivo_parece_ssy_dvr(ruta):
        return False
    try:
        return math.floor(calcular_duracion_video(ruta)) > 0
    except Exception:
        return False


def _leer_timing_video(ruta: str, *, count_frames: bool = False) -> dict:
    cmd = [
        "ffprobe",
        "-v",
        "error",
    ]
    if count_frames:
        cmd.append("-count_frames")
    cmd.extend(
        [
            "-select_streams",
            "v:0",
            "-show_entries",
            (
                "stream=codec_name,avg_frame_rate,r_frame_rate,"
                "nb_frames,nb_read_frames,duration:format=duration"
            ),
            "-of",
            "json",
            ruta,
        ]
    )
    result = run_command(cmd, error_prefix="No se pudo inspeccionar timing del video")
    try:
        data = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError("No se pudo inspeccionar timing del video: JSON invalido.") from exc

    stream = next(iter(data.get("streams") or []), {})
    formato = data.get("format") or {}

    def _float(valor) -> float:
        try:
            return float(valor or 0)
        except (TypeError, ValueError):
            return 0.0

    def _int(valor) -> int:
        try:
            return int(valor or 0)
        except (TypeError, ValueError):
            return 0

    duracion = _float(stream.get("duration")) or _float(formato.get("duration"))
    frames = _int(stream.get("nb_frames")) or _int(stream.get("nb_read_frames"))
    fps_declarado = (
        _parsear_fraccion_ffprobe(stream.get("avg_frame_rate"))
        or _parsear_fraccion_ffprobe(stream.get("r_frame_rate"))
        or 0.0
    )
    if frames <= 0 and not count_frames:
        return _leer_timing_video(ruta, count_frames=True)
    return {
        "duracion": duracion,
        "frames": frames,
        "fps_declarado": fps_declarado,
        "codec": stream.get("codec_name") or "",
    }


def _correccion_timing_necesaria(ruta: str, duracion_esperada: int | None) -> dict | None:
    if not MDVR_CORREGIR_TIMING_FPS or not duracion_esperada or duracion_esperada <= 0:
        return None
    if not str(ruta or "").lower().endswith(".mp4"):
        return None

    try:
        timing = _leer_timing_video(ruta)
    except Exception:
        return None

    duracion_real = float(timing.get("duracion") or 0.0)
    frames = int(timing.get("frames") or 0)
    if duracion_real <= 0 or frames <= 0:
        return None

    factor = float(duracion_esperada) / duracion_real
    fps_timeline = frames / float(duracion_esperada)
    if fps_timeline < MDVR_TIMING_MIN_FPS or fps_timeline > MDVR_TIMING_MAX_FPS:
        return None

    reconstruir_timeline = MDVR_RECONSTRUIR_TIMELINE
    if not reconstruir_timeline:
        if duracion_real + VIDEO_IMPORT_DURATION_TOLERANCE_SECONDS >= duracion_esperada:
            return None
        if factor < MDVR_TIMING_MIN_FACTOR or factor > MDVR_TIMING_MAX_FACTOR:
            return None

    return {
        **timing,
        "duracion_esperada": int(duracion_esperada),
        "factor": factor,
        "fps_timeline": fps_timeline,
        "modo": "reconstruir_timeline" if reconstruir_timeline else "estirar_duracion",
    }


def _duracion_archivo_segura(ruta: str) -> float:
    try:
        return float(calcular_duracion_video(ruta))
    except Exception:
        return 0.0


_BLACKDETECT_RE = re.compile(
    r"black_start:(?P<inicio>[0-9.]+)\s+"
    r"black_end:(?P<fin>[0-9.]+)\s+"
    r"black_duration:(?P<duracion>[0-9.]+)"
)


def _analizar_video_negro_completo_blackdetect(ruta: str, duracion: float) -> dict:
    filtro = (
        "blackdetect="
        f"d=0.5:pic_th={MDVR_BLACK_PIC_TH:.6f}:pix_th={MDVR_BLACK_PIX_TH:.6f}"
    )
    try:
        result = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostats",
                "-i",
                ruta,
                "-vf",
                filtro,
                "-an",
                "-f",
                "null",
                "-",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        return {
            "negro_total": False,
            "habilitado": True,
            "duracion": duracion,
            "modo": "full",
            "error": str(exc),
        }

    texto = f"{result.stderr or ''}\n{result.stdout or ''}"
    total_negro = 0.0
    for match in _BLACKDETECT_RE.finditer(texto):
        try:
            total_negro += float(match.group("duracion") or 0.0)
        except (TypeError, ValueError):
            continue
    ratio = total_negro / duracion if duracion > 0 else 0.0
    return {
        "negro_total": ratio >= MDVR_BLACK_MAX_RATIO,
        "habilitado": True,
        "modo": "full",
        "duracion": duracion,
        "segundos_negros": round(total_negro, 3),
        "ratio_negro": round(ratio, 6),
        "returncode": result.returncode,
    }


def _puntos_muestreo_negro(duracion: float) -> list[float]:
    cantidad = min(MDVR_BLACK_SAMPLE_COUNT, max(3, int(duracion // 300) + 3))
    cantidad = min(cantidad, MDVR_BLACK_SAMPLE_COUNT)
    return [
        max(0.5, min(duracion - 0.5, duracion * (indice + 1) / (cantidad + 1)))
        for indice in range(cantidad)
    ]


def _ratio_negro_frame_muestreado(ruta: str, segundo: float) -> dict:
    ancho = MDVR_BLACK_SAMPLE_WIDTH
    alto = MDVR_BLACK_SAMPLE_HEIGHT
    try:
        result = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{segundo:.3f}",
                "-i",
                ruta,
                "-frames:v",
                "1",
                "-vf",
                f"scale={ancho}:{alto}:flags=fast_bilinear,format=gray",
                "-f",
                "rawvideo",
                "-",
            ],
            check=False,
            capture_output=True,
            timeout=MDVR_BLACK_FRAME_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return {"segundo": round(segundo, 3), "error": "timeout"}
    except OSError as exc:
        return {"segundo": round(segundo, 3), "error": str(exc)}

    data = result.stdout or b""
    if result.returncode != 0 or len(data) < ancho * alto:
        detalle = (result.stderr or b"").decode("utf-8", errors="replace").strip()
        return {
            "segundo": round(segundo, 3),
            "error": detalle or f"frame incompleto ({len(data)} bytes)",
            "returncode": result.returncode,
        }

    umbral_pixel = int(round(MDVR_BLACK_PIX_TH * 255))
    pixeles_negros = sum(1 for pixel in data[: ancho * alto] if pixel <= umbral_pixel)
    ratio_pixeles_negros = pixeles_negros / float(ancho * alto)
    return {
        "segundo": round(segundo, 3),
        "ratio_pixeles_negros": round(ratio_pixeles_negros, 6),
        "negro": ratio_pixeles_negros >= MDVR_BLACK_PIC_TH,
    }


def _analizar_video_negro_por_muestras(ruta: str, duracion: float) -> dict:
    muestras = [_ratio_negro_frame_muestreado(ruta, punto) for punto in _puntos_muestreo_negro(duracion)]
    muestras_validas = [muestra for muestra in muestras if "negro" in muestra]
    min_validas = min(MDVR_BLACK_MIN_VALID_SAMPLES, len(muestras))
    if len(muestras_validas) < min_validas:
        return {
            "negro_total": False,
            "habilitado": True,
            "modo": "sample",
            "duracion": duracion,
            "motivo": "muestras_validas_insuficientes",
            "muestras_total": len(muestras),
            "muestras_validas": len(muestras_validas),
            "muestras_minimas": min_validas,
            "muestras": muestras,
        }

    muestras_negras = sum(1 for muestra in muestras_validas if muestra.get("negro"))
    ratio = muestras_negras / float(len(muestras_validas))
    return {
        "negro_total": ratio >= MDVR_BLACK_MAX_RATIO,
        "habilitado": True,
        "modo": "sample",
        "duracion": duracion,
        "muestras_total": len(muestras),
        "muestras_validas": len(muestras_validas),
        "muestras_negras": muestras_negras,
        "ratio_negro": round(ratio, 6),
        "muestras": muestras,
    }


def _analizar_video_totalmente_negro(ruta: str) -> dict:
    if not MDVR_RECHAZAR_VIDEO_NEGRO:
        return {"negro_total": False, "habilitado": False}
    duracion = _duracion_archivo_segura(ruta)
    if duracion < MDVR_BLACK_MIN_DURATION_SECONDS:
        return {
            "negro_total": False,
            "habilitado": True,
            "duracion": duracion,
            "motivo": "duracion_menor_al_minimo",
        }
    if MDVR_BLACK_ANALYSIS_MODE == "full":
        return _analizar_video_negro_completo_blackdetect(ruta, duracion)
    return _analizar_video_negro_por_muestras(ruta, duracion)


def _validar_video_con_imagen_util(ruta: str, *, contexto: str = "video") -> dict:
    info = _analizar_video_totalmente_negro(ruta)
    if info.get("negro_total"):
        ratio = float(info.get("ratio_negro") or 0.0) * 100
        unidad = "muestras" if info.get("modo") == "sample" else "duracion"
        raise VideoSinImagenUtilError(
            f"{contexto}: el video resultante quedo practicamente negro "
            f"({ratio:.1f}% de {unidad}). Se rechaza para no publicar un MP4 sin imagen util."
        )
    return info


def _estirar_mp4_a_duracion(
    ruta_origen: str,
    ruta_destino: str,
    factor: float,
    *,
    duracion_original: float = 0.0,
):
    ruta_remux = None
    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".mp4",
        dir=os.path.dirname(ruta_destino) or ".",
    ) as tmp:
        ruta_remux = tmp.name
    try:
        run_command(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-itsscale",
                f"{factor:.8f}",
                "-i",
                ruta_origen,
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                ruta_remux,
            ],
            error_prefix="No se pudo corregir timing/FPS MDVR por remux",
        )
        if (
            os.path.exists(ruta_remux)
            and os.path.getsize(ruta_remux) > 0
            and _duracion_archivo_segura(ruta_remux) > max(float(duracion_original or 0.0), 0.0)
        ):
            os.replace(ruta_remux, ruta_destino)
            asegurar_permisos_archivo(ruta_destino)
            return
    except Exception:
        pass
    finally:
        remove_if_exists(ruta_remux)

    filtro = f"setpts={factor:.8f}*PTS"
    run_command(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            ruta_origen,
            "-an",
            "-vf",
            filtro,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            ruta_destino,
        ],
        error_prefix="No se pudo corregir timing/FPS MDVR",
    )
    if not os.path.exists(ruta_destino) or os.path.getsize(ruta_destino) <= 0:
        raise ValidationError("No se pudo corregir timing/FPS MDVR: salida vacia.")
    asegurar_permisos_archivo(ruta_destino)


def _reconstruir_timeline_mp4(
    ruta_origen: str,
    ruta_destino: str,
    fps_timeline: float,
):
    filtro = f"setpts=N/({fps_timeline:.8f}*TB),format=yuv420p"
    run_command(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            ruta_origen,
            "-an",
            "-vf",
            filtro,
            "-vsync",
            "vfr",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-video_track_timescale",
            "90000",
            ruta_destino,
        ],
        error_prefix="No se pudo reconstruir timeline/FPS MDVR",
    )
    if not os.path.exists(ruta_destino) or os.path.getsize(ruta_destino) <= 0:
        raise ValidationError("No se pudo reconstruir timeline/FPS MDVR: salida vacia.")
    asegurar_permisos_archivo(ruta_destino)


def _actualizar_estado_video_por_duracion(
    video: Video,
    *,
    duracion_esperada: int | None,
    inicio_procesamiento: datetime.datetime | None = None,
):
    duracion_real = math.floor(calcular_duracion_video(video.ruta_archivo.path))
    video.duracion = duracion_real

    inicio = video.inicio_timestamp or datetime.time(0, 0)
    if isinstance(inicio, datetime.datetime):
        inicio = inicio.time()
    video.inicio_timestamp = inicio

    if video.fecha_inicio is not None:
        fecha_fin = video.fecha_inicio + datetime.timedelta(seconds=duracion_real)
        video.fecha_fin = fecha_fin
        video.fin_timestamp = fecha_fin.timetz().replace(tzinfo=None)
    else:
        video.fecha_fin = None
        video.fin_timestamp = (
            datetime.datetime.combine(datetime.date.today(), inicio)
            + datetime.timedelta(seconds=duracion_real)
        ).time()

    mensaje_incompleto = ""
    if (
        duracion_esperada is not None
        and duracion_real + VIDEO_IMPORT_DURATION_TOLERANCE_SECONDS < duracion_esperada
    ):
        mensaje_incompleto = _mensaje_video_incompleto(duracion_esperada, duracion_real)

    if mensaje_incompleto:
        video.estado = EstadoVideo.INCOMPLETO
        video.error_tipo = "incompleto"
        video.detalle_error = mensaje_incompleto
        video.ultimo_error = mensaje_incompleto[:2000]
    else:
        video.estado = EstadoVideo.LISTO
        video.error_tipo = ""
        video.detalle_error = ""
        video.ultimo_error = ""

    video.mimetype = mimetypes.guess_type(video.ruta_archivo.path)[0] or "video/mp4"
    video.procesamiento_finalizado_en = timezone.now()
    inicio_ref = inicio_procesamiento or video.procesamiento_iniciado_en
    if inicio_ref:
        video.tiempo_procesamiento_segundos = round(
            max((video.procesamiento_finalizado_en - inicio_ref).total_seconds(), 0),
            3,
        )
    video.reintentos = 0
    video.proximo_reintento_en = None
    video.save(
        update_fields=[
            "duracion",
            "estado",
            "inicio_timestamp",
            "fin_timestamp",
            "fecha_fin",
            "mimetype",
            "error_tipo",
            "detalle_error",
            "procesamiento_finalizado_en",
            "tiempo_procesamiento_segundos",
            "reintentos",
            "ultimo_error",
            "proximo_reintento_en",
        ]
    )


def _corregir_timing_video_si_corresponde(
    video: Video,
    *,
    duracion_esperada: int | None,
    inicio_procesamiento: datetime.datetime | None = None,
) -> dict:
    if not video.ruta_archivo or not getattr(video.ruta_archivo, "name", ""):
        return {"aplicada": False}
    try:
        ruta = video.ruta_archivo.path
    except Exception:
        return {"aplicada": False}

    plan = _correccion_timing_necesaria(ruta, duracion_esperada)
    if not plan:
        return {"aplicada": False}

    destino_dir = os.path.dirname(ruta) or "."
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4", dir=destino_dir) as tmp:
        ruta_corregida = tmp.name
    try:
        modo = str(plan.get("modo") or "")
        if modo == "reconstruir_timeline":
            _reconstruir_timeline_mp4(
                ruta,
                ruta_corregida,
                float(plan["fps_timeline"]),
            )
        else:
            _estirar_mp4_a_duracion(
                ruta,
                ruta_corregida,
                float(plan["factor"]),
                duracion_original=float(plan["duracion"] or 0.0),
            )
        duracion_corregida = math.floor(calcular_duracion_video(ruta_corregida))
        if (
            modo != "reconstruir_timeline"
            and duracion_corregida <= math.floor(float(plan["duracion"] or 0.0))
        ):
            raise ValidationError("La correccion de timing no aumento la duracion.")
        if (
            modo == "reconstruir_timeline"
            and abs(duracion_corregida - int(plan["duracion_esperada"]))
            > VIDEO_IMPORT_DURATION_TOLERANCE_SECONDS
        ):
            raise ValidationError(
                "La reconstruccion de timeline no alcanzo la duracion esperada "
                f"({duracion_corregida}s vs {plan['duracion_esperada']}s)."
            )
        validacion_negro = _analizar_video_totalmente_negro(ruta_corregida)
        if validacion_negro.get("negro_total"):
            return {
                "aplicada": False,
                "omitida": True,
                "motivo": "correccion_timing_genero_video_negro",
                "duracion_original": round(float(plan["duracion"]), 3),
                "duracion_corregida": duracion_corregida,
                "duracion_esperada": plan["duracion_esperada"],
                "fps_declarado": round(float(plan.get("fps_declarado") or 0.0), 3),
                "fps_timeline": round(float(plan["fps_timeline"]), 3),
                "factor": round(float(plan["factor"]), 6),
                "modo": modo,
                "validacion_negro": validacion_negro,
            }
        os.replace(ruta_corregida, ruta)
        asegurar_permisos_archivo(ruta)
        _actualizar_estado_video_por_duracion(
            video,
            duracion_esperada=duracion_esperada,
            inicio_procesamiento=inicio_procesamiento,
        )
        return {
            "aplicada": True,
            "duracion_original": round(float(plan["duracion"]), 3),
            "duracion_corregida": duracion_corregida,
            "duracion_esperada": plan["duracion_esperada"],
            "fps_declarado": round(float(plan.get("fps_declarado") or 0.0), 3),
            "fps_timeline": round(float(plan["fps_timeline"]), 3),
            "factor": round(float(plan["factor"]), 6),
            "modo": modo,
        }
    finally:
        remove_if_exists(ruta_corregida)


def _segmento_mp4_directo(segmentos: list[SegmentoVideo]) -> SegmentoVideo | None:
    if len(segmentos) != 1:
        return None
    segmento = segmentos[0]
    if segmento.extension != ".mp4":
        return None
    if not _mp4_directo_seguro(segmento.ruta):
        return None
    return segmento


def _mensaje_video_incompleto(duracion_esperada: int, duracion_real: int) -> str:
    return (
        "El video parece incompleto: "
        f"se esperaban al menos {duracion_esperada}s y solo se obtuvieron "
        f"{duracion_real}s."
    )


def _registrar_mp4_directo(
    video: Video,
    segmento: SegmentoVideo,
    *,
    nombre_video: str,
    duracion_esperada: int | None,
    ruta_previa: str = "",
):
    inicio_procesamiento = video.procesamiento_iniciado_en or timezone.now()
    destino_rel = _subir_archivo_temporal(segmento.ruta, f"{nombre_video}.mp4")
    try:
        ruta_final = default_storage.path(destino_rel)
        duracion_real = math.floor(calcular_duracion_video(ruta_final))
        if duracion_real <= 0:
            raise ValidationError("No se pudo calcular una duración válida para el MP4.")
        _validar_video_con_imagen_util(ruta_final, contexto=nombre_video)
    except Exception:
        try:
            default_storage.delete(destino_rel)
        except Exception:
            pass
        raise

    video.ruta_archivo = destino_rel
    video.duracion = duracion_real
    inicio = video.inicio_timestamp or segmento.inicio_dt.time()
    if isinstance(inicio, datetime.datetime):
        inicio = inicio.time()
    video.inicio_timestamp = inicio

    if video.fecha_inicio is not None:
        fecha_fin = video.fecha_inicio + datetime.timedelta(seconds=duracion_real)
        video.fecha_fin = fecha_fin
        video.fin_timestamp = fecha_fin.timetz().replace(tzinfo=None)
    else:
        video.fin_timestamp = (
            datetime.datetime.combine(datetime.date.today(), inicio)
            + datetime.timedelta(seconds=duracion_real)
        ).time()

    video.mimetype = mimetypes.guess_type(ruta_final)[0] or "video/mp4"
    video.procesamiento_iniciado_en = inicio_procesamiento
    video.procesamiento_finalizado_en = timezone.now()
    video.tiempo_procesamiento_segundos = round(
        max((video.procesamiento_finalizado_en - inicio_procesamiento).total_seconds(), 0),
        3,
    )

    mensaje_incompleto = ""
    if (
        duracion_esperada is not None
        and duracion_real + VIDEO_IMPORT_DURATION_TOLERANCE_SECONDS < duracion_esperada
    ):
        mensaje_incompleto = _mensaje_video_incompleto(duracion_esperada, duracion_real)

    if mensaje_incompleto:
        video.estado = EstadoVideo.INCOMPLETO
        video.error_tipo = "incompleto"
        video.detalle_error = mensaje_incompleto
        video.ultimo_error = mensaje_incompleto[:2000]
    else:
        video.estado = EstadoVideo.LISTO
        video.error_tipo = ""
        video.detalle_error = ""
        video.ultimo_error = ""

    video.reintentos = 0
    video.proximo_reintento_en = None
    video.save(
        update_fields=[
            "ruta_archivo",
            "duracion",
            "estado",
            "inicio_timestamp",
            "fin_timestamp",
            "fecha_fin",
            "mimetype",
            "error_tipo",
            "detalle_error",
            "procesamiento_iniciado_en",
            "procesamiento_finalizado_en",
            "tiempo_procesamiento_segundos",
            "reintentos",
            "ultimo_error",
            "proximo_reintento_en",
        ]
    )

    if ruta_previa and ruta_previa != destino_rel:
        try:
            default_storage.delete(ruta_previa)
        except Exception:
            pass
    return video


def _parsear_iso_datetime(valor: str | None) -> datetime.datetime | None:
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


def _duracion_segmento_real(seg: SegmentoVideo) -> float:
    try:
        duracion = float(calcular_duracion_video(seg.ruta))
    except Exception:
        duracion = 0.0
    if duracion > 0:
        return duracion

    duracion_nombre = float((seg.fin_dt - seg.inicio_dt).total_seconds())
    if 0 < duracion_nombre <= 6 * 3600:
        return duracion_nombre
    return 1.0


def _duracion_segmento_nombre(seg: SegmentoVideo) -> float:
    duracion_nombre = float((seg.fin_dt - seg.inicio_dt).total_seconds())
    if 0 < duracion_nombre <= 6 * 3600:
        return duracion_nombre
    return _duracion_segmento_real(seg)


def _construir_mapa_segmentos(
    segmentos: list[SegmentoVideo],
    duracion_video: int | None,
    *,
    usar_duracion_nombre: bool = False,
) -> list[dict]:
    total_video = int(duracion_video or 0)
    if not segmentos or total_video <= 0:
        return []

    if usar_duracion_nombre:
        plan = []
        cursor = 0.0
        total = len(segmentos)
        for idx, segmento in enumerate(segmentos, start=1):
            duracion_seg = max(1.0, _duracion_segmento_nombre(segmento))
            inicio_timeline = cursor
            fin_timeline = inicio_timeline + duracion_seg
            hueco_despues = 0.0
            if idx < total:
                siguiente = segmentos[idx]
                hueco_despues = max(
                    0.0,
                    float((siguiente.inicio_dt - segmento.fin_dt).total_seconds()),
                )
                if hueco_despues < GAP_PADDING_MIN_SEGUNDOS:
                    hueco_despues = 0.0
            plan.append(
                {
                    "segmento": segmento,
                    "duracion_segundos": duracion_seg,
                    "timeline_inicio": inicio_timeline,
                    "timeline_fin": fin_timeline,
                    "hueco_despues": hueco_despues,
                }
            )
            cursor = fin_timeline + hueco_despues
        total_real = cursor
    else:
        plan, total_real = _planificar_timeline_segmentos(segmentos)
    if not plan or total_real <= 0:
        return []

    tz_actual = timezone.get_current_timezone()
    mapa: list[dict] = []
    cantidad = len(plan)
    ultimo_fin_exclusivo = 0

    for idx, item in enumerate(plan, start=1):
        seg = item["segmento"]
        duracion_seg = float(item["duracion_segundos"])
        inicio_video = int(round((float(item["timeline_inicio"]) / total_real) * total_video))
        inicio_video = max(ultimo_fin_exclusivo, inicio_video)
        if inicio_video >= total_video:
            break
        if idx == cantidad:
            fin_video_exclusivo = total_video
        else:
            ideal = int(round((float(item["timeline_fin"]) / total_real) * total_video))
            restantes = cantidad - idx
            minimo = inicio_video + 1
            maximo = max(minimo, total_video - restantes)
            fin_video_exclusivo = min(maximo, max(minimo, ideal))
        if fin_video_exclusivo <= inicio_video:
            fin_video_exclusivo = min(total_video, inicio_video + 1)
        if fin_video_exclusivo <= inicio_video:
            continue

        inicio_real = seg.inicio_dt
        if timezone.is_naive(inicio_real):
            inicio_real = timezone.make_aware(inicio_real, tz_actual)
        fin_real = inicio_real + datetime.timedelta(seconds=duracion_seg)

        mapa.append(
            {
                "orden": idx,
                "archivo": os.path.basename(seg.ruta),
                "video_inicio_segundo": int(inicio_video),
                "video_fin_segundo": int(fin_video_exclusivo - 1),
                "real_inicio": inicio_real.isoformat(),
                "real_fin": fin_real.isoformat(),
            }
        )
        ultimo_fin_exclusivo = fin_video_exclusivo

    return mapa


def _recortar_mapa_segmentos(
    mapa_segmentos: list[dict] | None,
    *,
    inicio_offset: int,
    nueva_duracion: int,
) -> list[dict]:
    if not isinstance(mapa_segmentos, list):
        return []
    if nueva_duracion <= 0:
        return []

    inicio_offset = max(0, int(inicio_offset))
    limite = nueva_duracion - 1
    recortado: list[dict] = []

    for item in mapa_segmentos:
        if not isinstance(item, dict):
            continue
        try:
            viejo_inicio = int(item.get("video_inicio_segundo"))
            viejo_fin = int(item.get("video_fin_segundo"))
        except (TypeError, ValueError):
            continue
        if viejo_fin < viejo_inicio:
            continue

        nuevo_inicio = viejo_inicio - inicio_offset
        nuevo_fin = viejo_fin - inicio_offset
        if nuevo_fin < 0 or nuevo_inicio > limite:
            continue

        recorte_inicio_seg = max(0, -nuevo_inicio)
        nuevo_inicio = max(0, nuevo_inicio)
        nuevo_fin = min(limite, nuevo_fin)
        if nuevo_fin < nuevo_inicio:
            continue

        inicio_real = _parsear_iso_datetime(item.get("real_inicio"))
        fin_real = _parsear_iso_datetime(item.get("real_fin"))
        if inicio_real is not None:
            inicio_real = inicio_real + datetime.timedelta(seconds=recorte_inicio_seg)
            duracion_recortada = nuevo_fin - nuevo_inicio + 1
            fin_real = inicio_real + datetime.timedelta(seconds=duracion_recortada)

        nuevo = dict(item)
        nuevo["video_inicio_segundo"] = int(nuevo_inicio)
        nuevo["video_fin_segundo"] = int(nuevo_fin)
        if inicio_real is not None:
            nuevo["real_inicio"] = inicio_real.isoformat()
        if fin_real is not None:
            nuevo["real_fin"] = fin_real.isoformat()
        recortado.append(nuevo)

    def _safe_int(valor, default=0):
        try:
            return int(valor)
        except (TypeError, ValueError):
            return default

    recortado.sort(
        key=lambda item: (
            _safe_int(item.get("video_inicio_segundo"), 0),
            _safe_int(item.get("video_fin_segundo"), 0),
        )
    )
    return recortado


def _recortar_video(video: Video, segundos: int, inicio_offset: int = 0) -> bool:
    if segundos <= 0 or inicio_offset < 0:
        return False
    segundos = int(segundos)
    inicio_offset = int(inicio_offset)
    ruta = video.ruta_archivo.path
    base, ext = os.path.splitext(ruta)
    ruta_tmp = f"{base}.trim{ext}"
    base_cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", ruta]
    if inicio_offset > 0:
        # Con desplazamiento de inicio se transcodifica para evitar cortes en keyframe
        # que dejen cámaras aún desfasadas.
        comandos = [
            base_cmd
            + [
                "-ss",
                str(inicio_offset),
                "-t",
                str(segundos),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-pix_fmt",
                "yuv420p",
                "-an",
                "-y",
                ruta_tmp,
            ]
        ]
    else:
        comandos = [
            base_cmd + ["-t", str(segundos), "-c", "copy", "-y", ruta_tmp],
            base_cmd
            + [
                "-t",
                str(segundos),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-pix_fmt",
                "yuv420p",
                "-an",
                "-y",
                ruta_tmp,
            ],
        ]

    ejecutado = False
    for comando in comandos:
        try:
            subprocess.run(comando, capture_output=True, text=True, check=True)
            ejecutado = True
            break
        except (subprocess.CalledProcessError, FileNotFoundError):
            if os.path.exists(ruta_tmp):
                os.remove(ruta_tmp)

    if not ejecutado:
        return False

    os.replace(ruta_tmp, ruta)
    asegurar_permisos_archivo(ruta)
    nueva_duracion = math.floor(calcular_duracion_video(ruta))
    video.duracion = nueva_duracion
    inicio = video.inicio_timestamp or datetime.time(0, 0)
    if isinstance(inicio, datetime.datetime):
        inicio = inicio.time()
    inicio_dt = (
        datetime.datetime.combine(datetime.date.today(), inicio)
        + datetime.timedelta(seconds=inicio_offset)
    )
    video.inicio_timestamp = inicio_dt.time()
    fin = (
        datetime.datetime.combine(datetime.date.today(), video.inicio_timestamp)
        + datetime.timedelta(seconds=nueva_duracion)
    ).time()
    video.fin_timestamp = fin
    campos = ["duracion", "inicio_timestamp", "fin_timestamp"]
    if video.fecha_inicio is not None:
        video.fecha_inicio = video.fecha_inicio + datetime.timedelta(seconds=inicio_offset)
        campos.append("fecha_inicio")
    if getattr(video, "mapa_segmentos", None):
        video.mapa_segmentos = _recortar_mapa_segmentos(
            video.mapa_segmentos,
            inicio_offset=inicio_offset,
            nueva_duracion=nueva_duracion,
        )
        campos.append("mapa_segmentos")
    video.save(update_fields=campos)
    return True


def _alinear_por_duracion_minima(videos: list[Video]) -> int:
    duraciones_validas = [
        video.duracion
        for video in videos
        if video.duracion and video.duracion >= MIN_DURACION_ALINEACION_SEGUNDOS
    ]
    if len(duraciones_validas) < 2:
        return 0
    objetivo = min(duraciones_validas)
    recortados = 0
    for video in videos:
        if (
            video.duracion
            and video.duracion >= MIN_DURACION_ALINEACION_SEGUNDOS
            and video.duracion > objetivo
        ):
            if _recortar_video(video, objetivo):
                recortados += 1
    return recortados


def _alinear_por_solape(videos: list[Video]) -> tuple[bool, int]:
    ventanas = []
    for video in videos:
        if (
            not video.fecha_inicio
            or not video.duracion
            or video.duracion < MIN_DURACION_ALINEACION_SEGUNDOS
        ):
            continue
        inicio = video.fecha_inicio
        fin = inicio + datetime.timedelta(seconds=video.duracion)
        ventanas.append((video, inicio, fin))

    if len(ventanas) < 2:
        return False, 0

    inicios = [inicio for _, inicio, _ in ventanas]
    desfase_inicio = int((max(inicios) - min(inicios)).total_seconds())
    if desfase_inicio > MAX_DESFASE_INICIO_ALINEACION_SEGUNDOS:
        return False, 0

    inicio_objetivo = max(inicios)
    fin_objetivo = min(fin for _, _, fin in ventanas)
    duracion_objetivo = int((fin_objetivo - inicio_objetivo).total_seconds())
    if duracion_objetivo <= 0:
        return True, 0

    recortados = 0
    for video, inicio, _ in ventanas:
        inicio_offset = int((inicio_objetivo - inicio).total_seconds())
        inicio_offset = max(0, inicio_offset)
        if video.duracion == duracion_objetivo and inicio_offset == 0:
            continue
        if _recortar_video(video, duracion_objetivo, inicio_offset=inicio_offset):
            recortados += 1

    return True, recortados


def _alinear_duraciones(videos: list[Video]) -> int:
    aplicado_por_solape, recortados = _alinear_por_solape(videos)
    if aplicado_por_solape:
        return recortados
    return _alinear_por_duracion_minima(videos)


def _parse_fecha_objetivo(
    fecha_objetivo: datetime.date | str | None,
) -> datetime.date | None:
    if fecha_objetivo is None:
        return None
    if isinstance(fecha_objetivo, datetime.date):
        return fecha_objetivo
    valor = str(fecha_objetivo).strip()
    if not valor:
        return None
    try:
        return datetime.date.fromisoformat(valor)
    except ValueError as exc:
        raise ValidationError("Parametro 'fecha' invalido. Use formato YYYY-MM-DD.") from exc


def importar_videos_mdvr(
    base_dir: str | None = None,
    importar_velocidades: bool = True,
    fecha_objetivo: datetime.date | str | None = None,
    forzar_reproceso: bool = False,
):
    base = (base_dir or "").strip()
    if not base:
        base = getattr(settings, "VIDEOS_MDVR_DIR", "") or getattr(
            settings, "VIDEOS_IMPORT_DIR", ""
        )
    if not base:
        raise ValidationError("VIDEOS_MDVR_DIR o VIDEOS_IMPORT_DIR no está configurado.")

    base_real = os.path.realpath(base)
    if not os.path.isdir(base_real):
        raise ValidationError("La ruta de videos MDVR no es un directorio válido.")

    fecha_filtrada = _parse_fecha_objetivo(fecha_objetivo)

    camiones = Camion.objects.exclude(carpeta_id="").all()
    if not camiones:
        return {"camiones": 0, "videos_creados": 0, "errores": ["No hay camiones con carpeta_id."]}

    respuesta = {"camiones": 0, "videos_creados": 0, "errores": [], "detalles": []}

    for camion in camiones:
        detalle = _importar_camion_mdvr(
            camion,
            base_real,
            importar_velocidades=importar_velocidades,
            fecha_objetivo=fecha_filtrada,
            forzar_reproceso=forzar_reproceso,
        )
        respuesta["camiones"] += 1
        respuesta["videos_creados"] += detalle.get("videos_creados", 0)
        respuesta["errores"].extend(detalle.get("errores", []))
        respuesta["detalles"].append(detalle)

    return respuesta


def _importar_camion_mdvr(
    camion: Camion,
    base_dir: str,
    importar_velocidades: bool = True,
    fecha_objetivo: datetime.date | None = None,
    forzar_reproceso: bool = False,
):
    carpeta_id = (camion.carpeta_id or "").strip()
    if not carpeta_id:
        return {"camion_id": camion.id, "videos_creados": 0, "errores": ["carpeta_id vacío."]}

    carpeta_mdvr = _buscar_carpeta_mdvr(base_dir, carpeta_id)
    if not carpeta_mdvr:
        return {
            "camion_id": camion.id,
            "videos_creados": 0,
            "errores": [f"No se encontró carpeta MDVR para {carpeta_id}."],
        }

    xlsx_files = _listar_xlsx(base_dir, carpeta_id)

    detalles = {
        "camion_id": camion.id,
        "carpeta_id": carpeta_id,
        "videos_creados": 0,
        "videos_procesados": [],
        "videos_omitidos": 0,
        "archivos_omitidos": 0,
        "turnos_procesados": 0,
        "recortados": 0,
        "motivos_omision": {},
        "omisiones": [],
        "errores": [],
    }

    for nombre in sorted(os.listdir(carpeta_mdvr)):
        ruta_dia = os.path.join(carpeta_mdvr, nombre)
        if os.path.isfile(ruta_dia) and _es_extension_video_soportada(nombre):
            _registrar_omision(
                detalles,
                motivo=(
                    "archivo de video fuera de estructura esperada "
                    "(debe estar dentro de subcarpeta YYYY-MM-DD)."
                ),
                ruta_archivo=ruta_dia,
            )
            continue
        if not os.path.isdir(ruta_dia):
            continue
        if not _DIR_FECHA_RE.match(nombre):
            continue

        try:
            fecha = datetime.date.fromisoformat(nombre)
        except ValueError:
            continue
        if fecha_objetivo and fecha != fecha_objetivo:
            continue

        segmentos = []
        for archivo in os.listdir(ruta_dia):
            ruta_archivo = os.path.join(ruta_dia, archivo)
            if not os.path.isfile(ruta_archivo):
                if os.path.isdir(ruta_archivo):
                    _registrar_omision(
                        detalles,
                        motivo=(
                            "subdirectorio detectado dentro de carpeta día; "
                            "solo se procesan archivos en ese nivel."
                        ),
                        ruta_archivo=ruta_archivo,
                    )
                continue
            segmento, motivo_omision = _segmento_desde_archivo_con_motivo(ruta_archivo, fecha)
            if not segmento:
                _registrar_omision(
                    detalles,
                    motivo=motivo_omision or "archivo no reconocido para importación MDVR.",
                    ruta_archivo=ruta_archivo,
                )
                continue

            listo, motivo_listo = _archivo_listo_para_importar(ruta_archivo)
            if not listo:
                _registrar_omision(
                    detalles,
                    motivo=motivo_listo or "archivo no está listo para importación.",
                    ruta_archivo=ruta_archivo,
                )
                continue
            segmentos.append(segmento)

        segmentos = _deduplicar_segmentos_preferir_mp4(segmentos)

        if not segmentos:
            _registrar_omision(
                detalles,
                motivo=(
                    f"sin segmentos válidos para la fecha {fecha.isoformat()} "
                    "en carpeta de día."
                ),
            )
            continue

        grupos: dict[tuple[str, int], list[SegmentoVideo]] = {}
        for segmento in segmentos:
            tipo_turno = _tipo_turno_para_hora(segmento.inicio_dt.time())
            key = (tipo_turno, segmento.camara)
            grupos.setdefault(key, []).append(segmento)

        turnos_creados = {}
        videos_turno: dict[str, list[Video]] = {}
        videos_referencia_por_turno: dict[int, Video] = {}
        xlsx_por_turno: dict[int, str] = {}

        for (tipo_turno, camara), lista in grupos.items():
            lista.sort(key=lambda s: s.inicio_dt)
            lista_procesable, info_hueco = _segmentos_contiguos_hasta_primer_hueco(lista)
            if not lista_procesable:
                _registrar_omision(
                    detalles,
                    motivo="sin segmentos continuos procesables para turno y cámara.",
                    nombre_video=f"MDVR_{carpeta_id}_{fecha.isoformat()}_{tipo_turno}_C{camara}",
                    omision_video=True,
                )
                continue
            turno = turnos_creados.get(tipo_turno)
            if turno is None:
                turno = _obtener_o_crear_turno(
                    fecha=fecha,
                    camion=camion,
                    tipo_turno=tipo_turno,
                    errores=detalles["errores"],
                )
                turnos_creados[tipo_turno] = turno

            nombre_video = f"MDVR_{carpeta_id}_{fecha.isoformat()}_{tipo_turno}_C{camara}"
            duracion_esperada = sum(
                max(1, int((segmento.fin_dt - segmento.inicio_dt).total_seconds()))
                for segmento in lista_procesable
            )
            try:
                inspeccion_mdvr = _inspeccionar_segmentos_mdvr(
                    lista_procesable,
                    carpeta_id=carpeta_id,
                    fecha=fecha,
                    tipo_turno=tipo_turno,
                    camara=camara,
                )
            except Exception as exc:
                _registrar_omision(
                    detalles,
                    motivo=f"no se pudo inspeccionar origen MDVR ({_normalizar_error(exc)}).",
                    nombre_video=nombre_video,
                    omision_video=True,
                )
                continue
            grupo_origen = inspeccion_mdvr["grupo_origen"]
            ahora = timezone.now()
            video_listo = (
                Video.objects.filter(
                    id_turno=turno,
                    camara=camara,
                    estado=EstadoVideo.LISTO,
                    grupo_origen=grupo_origen,
                )
                .order_by("-id")
                .first()
            )
            if video_listo is None:
                video_listo = (
                    Video.objects.filter(
                        nombre=nombre_video,
                        id_turno=turno,
                        camara=camara,
                        estado=EstadoVideo.LISTO,
                    )
                    .order_by("-id")
                    .first()
                )
            video_incompleto = (
                Video.objects.filter(
                    id_turno=turno,
                    camara=camara,
                    estado=EstadoVideo.INCOMPLETO,
                    grupo_origen=grupo_origen,
                )
                .order_by("-id")
                .first()
            )
            if video_incompleto is None:
                video_incompleto = (
                    Video.objects.filter(
                        nombre=nombre_video,
                        id_turno=turno,
                        camara=camara,
                        estado=EstadoVideo.INCOMPLETO,
                    )
                    .order_by("-id")
                    .first()
                )
            if (
                not forzar_reproceso
                and video_listo
                and video_listo.origen_sha256 == inspeccion_mdvr["origen_sha256"]
                and video_listo.ruta_archivo
                and video_listo.ruta_archivo.name
                and default_storage.exists(video_listo.ruta_archivo.name)
            ):
                _rehidratar_metadata_video_existente(video_listo)
                cambios_listo = []
                if not getattr(video_listo, "mapa_segmentos", None):
                    mapa_segmentos = _construir_mapa_segmentos(
                        lista_procesable, video_listo.duracion
                    )
                    if mapa_segmentos:
                        video_listo.mapa_segmentos = mapa_segmentos
                        cambios_listo.append("mapa_segmentos")
                if not (video_listo.ruta_origen or ""):
                    video_listo.ruta_origen = inspeccion_mdvr["ruta_origen"]
                    cambios_listo.append("ruta_origen")
                if not (video_listo.grupo_origen or ""):
                    video_listo.grupo_origen = grupo_origen
                    cambios_listo.append("grupo_origen")
                if not getattr(video_listo, "segmentos_origen", None):
                    video_listo.segmentos_origen = inspeccion_mdvr["segmentos_origen"]
                    cambios_listo.append("segmentos_origen")
                if not (video_listo.origen_sha256 or ""):
                    video_listo.origen_sha256 = inspeccion_mdvr["origen_sha256"]
                    cambios_listo.append("origen_sha256")
                if not video_listo.origen_tamano_bytes:
                    video_listo.origen_tamano_bytes = inspeccion_mdvr["origen_tamano_bytes"]
                    cambios_listo.append("origen_tamano_bytes")
                if not video_listo.origen_modificado_en:
                    video_listo.origen_modificado_en = inspeccion_mdvr["origen_modificado_en"]
                    cambios_listo.append("origen_modificado_en")
                if cambios_listo:
                    video_listo.save(update_fields=cambios_listo)
                if (
                    importar_velocidades
                    and video_listo.estado_velocidades == EstadoVelocidadesVideo.PENDIENTE
                ):
                    inicio_para_xlsx = lista_procesable[0].inicio_dt
                    if video_listo.fecha_inicio is not None:
                        inicio_para_xlsx = video_listo.fecha_inicio
                        if timezone.is_aware(inicio_para_xlsx):
                            inicio_para_xlsx = timezone.localtime(inicio_para_xlsx).replace(
                                tzinfo=None
                            )
                    _programar_importacion_velocidades_turno(
                        turno=turno,
                        video_ref=video_listo,
                        xlsx_files=xlsx_files,
                        fecha_inicio=inicio_para_xlsx,
                        xlsx_por_turno=xlsx_por_turno,
                        videos_referencia_por_turno=videos_referencia_por_turno,
                    )
                    continue
                _registrar_omision(
                    detalles,
                    motivo="ya existe video LISTO para turno y cámara con la misma huella de origen.",
                    nombre_video=nombre_video,
                    omision_video=True,
                )
                continue
            if (
                not forzar_reproceso
                and video_incompleto
                and video_incompleto.origen_sha256 == inspeccion_mdvr["origen_sha256"]
                and video_incompleto.ruta_archivo
                and video_incompleto.ruta_archivo.name
                and default_storage.exists(video_incompleto.ruta_archivo.name)
            ):
                _rehidratar_metadata_video_existente(video_incompleto)
                cambios_incompleto = []
                if not getattr(video_incompleto, "mapa_segmentos", None):
                    mapa_segmentos = _construir_mapa_segmentos(
                        lista_procesable, video_incompleto.duracion
                    )
                    if mapa_segmentos:
                        video_incompleto.mapa_segmentos = mapa_segmentos
                        cambios_incompleto.append("mapa_segmentos")
                if not (video_incompleto.ruta_origen or ""):
                    video_incompleto.ruta_origen = inspeccion_mdvr["ruta_origen"]
                    cambios_incompleto.append("ruta_origen")
                if not (video_incompleto.grupo_origen or ""):
                    video_incompleto.grupo_origen = grupo_origen
                    cambios_incompleto.append("grupo_origen")
                if not getattr(video_incompleto, "segmentos_origen", None):
                    video_incompleto.segmentos_origen = inspeccion_mdvr["segmentos_origen"]
                    cambios_incompleto.append("segmentos_origen")
                if not (video_incompleto.origen_sha256 or ""):
                    video_incompleto.origen_sha256 = inspeccion_mdvr["origen_sha256"]
                    cambios_incompleto.append("origen_sha256")
                if not video_incompleto.origen_tamano_bytes:
                    video_incompleto.origen_tamano_bytes = inspeccion_mdvr["origen_tamano_bytes"]
                    cambios_incompleto.append("origen_tamano_bytes")
                if not video_incompleto.origen_modificado_en:
                    video_incompleto.origen_modificado_en = inspeccion_mdvr["origen_modificado_en"]
                    cambios_incompleto.append("origen_modificado_en")
                if cambios_incompleto:
                    video_incompleto.save(update_fields=cambios_incompleto)
                if (
                    importar_velocidades
                    and video_incompleto.estado_velocidades == EstadoVelocidadesVideo.PENDIENTE
                ):
                    inicio_para_xlsx = lista_procesable[0].inicio_dt
                    if video_incompleto.fecha_inicio is not None:
                        inicio_para_xlsx = video_incompleto.fecha_inicio
                        if timezone.is_aware(inicio_para_xlsx):
                            inicio_para_xlsx = timezone.localtime(
                                inicio_para_xlsx
                            ).replace(tzinfo=None)
                    _programar_importacion_velocidades_turno(
                        turno=turno,
                        video_ref=video_incompleto,
                        xlsx_files=xlsx_files,
                        fecha_inicio=inicio_para_xlsx,
                        xlsx_por_turno=xlsx_por_turno,
                        videos_referencia_por_turno=videos_referencia_por_turno,
                    )
                _registrar_omision(
                    detalles,
                    motivo="ya existe video INCOMPLETO para turno y cámara con la misma huella de origen.",
                    nombre_video=nombre_video,
                    omision_video=True,
                )
                continue

            reemplazar_video_listo = bool(video_listo or video_incompleto)
            video_existente = video_listo or video_incompleto or (
                Video.objects.filter(nombre=nombre_video, id_turno=turno)
                .exclude(estado__in=[EstadoVideo.LISTO, EstadoVideo.INCOMPLETO])
                .order_by("-id")
                .first()
            )
            if (
                video_existente
                and not reemplazar_video_listo
                and not _puede_reprocesarse(video_existente, ahora)
            ):
                if (
                    video_existente.estado == EstadoVideo.ERROR
                    and (video_existente.reintentos or 0) >= MAX_REINTENTOS_MDVR
                ):
                    video_existente.estado = EstadoVideo.ERROR_PERMANENTE
                    if not video_existente.ultimo_error:
                        video_existente.ultimo_error = (
                            f"Reintentos agotados ({MAX_REINTENTOS_MDVR}/{MAX_REINTENTOS_MDVR})."
                        )
                    video_existente.proximo_reintento_en = None
                    video_existente.save(
                        update_fields=["estado", "ultimo_error", "proximo_reintento_en"]
                    )
                motivo = (
                    f"video existente no reprocesable (estado={video_existente.estado}, "
                    f"reintentos={video_existente.reintentos or 0}, "
                    f"proximo_reintento_en={video_existente.proximo_reintento_en})."
                )
                _registrar_omision(
                    detalles,
                    motivo=motivo,
                    nombre_video=nombre_video,
                    omision_video=True,
                )
                continue

            video = video_existente
            tmp_dir = tempfile.mkdtemp(prefix="mdvr_")
            ruta_previa = ""
            ruta_generada_en_intento = ""
            try:
                ext_salida = ".mp4"
                inicio_dt = lista_procesable[0].inicio_dt
                lease_hasta = timezone.now() + datetime.timedelta(seconds=PROCESANDO_LEASE_SEGUNDOS)
                if video_existente:
                    ruta_previa = (video_existente.ruta_archivo.name or "").strip()
                    video_existente.camara = camara
                    video_existente.fecha_inicio = inicio_dt
                    video_existente.fecha_subida = fecha
                    video_existente.inicio_timestamp = inicio_dt.time()
                    video_existente.estado = EstadoVideo.PROCESANDO
                    video_existente.duracion = None
                    video_existente.fecha_fin = None
                    video_existente.fin_timestamp = None
                    video_existente.mimetype = ""
                    video_existente.ruta_origen = inspeccion_mdvr["ruta_origen"]
                    video_existente.grupo_origen = grupo_origen
                    video_existente.segmentos_origen = inspeccion_mdvr["segmentos_origen"]
                    video_existente.origen_sha256 = inspeccion_mdvr["origen_sha256"]
                    video_existente.origen_tamano_bytes = inspeccion_mdvr["origen_tamano_bytes"]
                    video_existente.origen_modificado_en = inspeccion_mdvr["origen_modificado_en"]
                    video_existente.error_tipo = ""
                    video_existente.detalle_error = ""
                    video_existente.procesamiento_iniciado_en = timezone.now()
                    video_existente.procesamiento_finalizado_en = None
                    video_existente.tiempo_procesamiento_segundos = None
                    video_existente.proximo_reintento_en = lease_hasta
                    video_existente.estado_velocidades = EstadoVelocidadesVideo.PENDIENTE
                    video_existente.velocidades_actualizadas_en = None
                    video_existente.velocidades_error = ""
                    video_existente.mapa_segmentos = []
                    video_existente.save(
                        update_fields=[
                            "camara",
                            "fecha_inicio",
                            "fecha_subida",
                            "inicio_timestamp",
                            "estado",
                            "duracion",
                            "fecha_fin",
                            "fin_timestamp",
                            "mimetype",
                            "ruta_origen",
                            "grupo_origen",
                            "segmentos_origen",
                            "origen_sha256",
                            "origen_tamano_bytes",
                            "origen_modificado_en",
                            "error_tipo",
                            "detalle_error",
                            "procesamiento_iniciado_en",
                            "procesamiento_finalizado_en",
                            "tiempo_procesamiento_segundos",
                            "proximo_reintento_en",
                            "estado_velocidades",
                            "velocidades_actualizadas_en",
                            "velocidades_error",
                            "mapa_segmentos",
                        ]
                    )
                    video = video_existente
                else:
                    ruta_pendiente = os.path.join(
                        "videos", "pendientes", f"{nombre_video}{ext_salida}"
                    )
                    video = Video.objects.create(
                        nombre=nombre_video,
                        camara=camara,
                        ruta_archivo=ruta_pendiente,
                        ruta_origen=inspeccion_mdvr["ruta_origen"],
                        grupo_origen=grupo_origen,
                        segmentos_origen=inspeccion_mdvr["segmentos_origen"],
                        origen_sha256=inspeccion_mdvr["origen_sha256"],
                        origen_tamano_bytes=inspeccion_mdvr["origen_tamano_bytes"],
                        origen_modificado_en=inspeccion_mdvr["origen_modificado_en"],
                        fecha_inicio=inicio_dt,
                        fecha_subida=fecha,
                        inicio_timestamp=inicio_dt.time(),
                        estado=EstadoVideo.PROCESANDO,
                        error_tipo="",
                        detalle_error="",
                        procesamiento_iniciado_en=timezone.now(),
                        proximo_reintento_en=lease_hasta,
                        estado_velocidades=EstadoVelocidadesVideo.PENDIENTE,
                        id_turno=turno,
                    )

                registro_directo_mp4 = False
                correccion_timing = {"aplicada": False}
                segmento_directo = _segmento_mp4_directo(lista_procesable)
                if segmento_directo is not None:
                    _registrar_mp4_directo(
                        video,
                        segmento_directo,
                        nombre_video=nombre_video,
                        duracion_esperada=duracion_esperada,
                        ruta_previa=ruta_previa if video_existente else "",
                    )
                    registro_directo_mp4 = True
                    correccion_timing = _corregir_timing_video_si_corresponde(
                        video,
                        duracion_esperada=duracion_esperada,
                        inicio_procesamiento=video.procesamiento_iniciado_en,
                    )
                else:
                    ruta_salida = os.path.join(tmp_dir, f"{nombre_video}{ext_salida}")
                    ok, error = _concatenar_segmentos(lista_procesable, ruta_salida)
                    if not ok:
                        raise ValidationError(f"No se pudo concatenar segmentos ({error}).")
                    _validar_video_con_imagen_util(ruta_salida, contexto=nombre_video)

                    destino_rel = _subir_archivo_temporal(
                        ruta_salida, f"{nombre_video}{ext_salida}"
                    )
                    ruta_generada_en_intento = destino_rel
                    video.ruta_archivo = destino_rel
                    video.save(update_fields=["ruta_archivo"])

                    procesar_video_subida(
                        video,
                        video.ruta_archivo,
                        duracion_esperada=duracion_esperada,
                    )
                    correccion_timing = _corregir_timing_video_si_corresponde(
                        video,
                        duracion_esperada=duracion_esperada,
                        inicio_procesamiento=video.procesamiento_iniciado_en,
                    )
                mapa_segmentos = _construir_mapa_segmentos(
                    lista_procesable,
                    video.duracion,
                    usar_duracion_nombre=bool(correccion_timing.get("aplicada")),
                )
                if mapa_segmentos:
                    video.mapa_segmentos = mapa_segmentos
                    video.save(update_fields=["mapa_segmentos"])
                if info_hueco and video.estado == EstadoVideo.LISTO:
                    mensaje_hueco = _mensaje_video_cortado_en_hueco(info_hueco)
                    video.estado = EstadoVideo.INCOMPLETO
                    video.error_tipo = "incompleto"
                    video.detalle_error = mensaje_hueco
                    video.ultimo_error = mensaje_hueco
                    video.reintentos = 0
                    video.proximo_reintento_en = None
                    video.save(
                        update_fields=[
                            "estado",
                            "error_tipo",
                            "detalle_error",
                            "ultimo_error",
                            "reintentos",
                            "proximo_reintento_en",
                        ]
                    )
                if video.estado in {EstadoVideo.LISTO, EstadoVideo.INCOMPLETO}:
                    _validar_video_con_imagen_util(
                        video.ruta_archivo.path,
                        contexto=nombre_video,
                    )
                if (
                    video_existente
                    and ruta_previa
                    and video.ruta_archivo
                    and ruta_previa != (video.ruta_archivo.name or "")
                ):
                    try:
                        default_storage.delete(ruta_previa)
                    except Exception:
                        pass
                    ruta_previa = ""
                if video.estado == EstadoVideo.LISTO:
                    _normalizar_video_exitoso(video)
                detalles["videos_creados"] += 1
                detalles["videos_procesados"].append(
                    {
                        "video_id": video.id,
                        "nombre": video.nombre,
                        "camara": video.camara,
                        "estado": video.estado,
                        "error_tipo": video.error_tipo,
                        "turno_id": turno.id,
                        "procesamiento_iniciado_en": (
                            video.procesamiento_iniciado_en.isoformat()
                            if video.procesamiento_iniciado_en
                            else None
                        ),
                        "procesamiento_finalizado_en": (
                            video.procesamiento_finalizado_en.isoformat()
                            if video.procesamiento_finalizado_en
                            else None
                        ),
                        "tiempo_procesamiento_segundos": video.tiempo_procesamiento_segundos,
                        "timing_fps": correccion_timing,
                    }
                )
                if video.estado == EstadoVideo.INCOMPLETO:
                    if info_hueco:
                        detalles["errores"].append(
                            f"{nombre_video}: se procesó hasta el primer hueco detectado; "
                            "se regenerará si aparece continuidad."
                        )
                    if importar_velocidades:
                        inicio_para_xlsx = inicio_dt
                        if video.fecha_inicio is not None:
                            inicio_para_xlsx = video.fecha_inicio
                            if timezone.is_aware(inicio_para_xlsx):
                                inicio_para_xlsx = timezone.localtime(
                                    inicio_para_xlsx
                                ).replace(tzinfo=None)
                        programado = _programar_importacion_velocidades_turno(
                            turno=turno,
                            video_ref=video,
                            xlsx_files=xlsx_files,
                            fecha_inicio=inicio_para_xlsx,
                            xlsx_por_turno=xlsx_por_turno,
                            videos_referencia_por_turno=videos_referencia_por_turno,
                        )
                        if programado:
                            detalles["errores"].append(
                                f"{nombre_video}: video guardado como incompleto; se intentará importar velocidades."
                            )
                    continue
                if not registro_directo_mp4:
                    videos_turno.setdefault(tipo_turno, []).append(video)

                if importar_velocidades:
                    _programar_importacion_velocidades_turno(
                        turno=turno,
                        video_ref=video,
                        xlsx_files=xlsx_files,
                        fecha_inicio=inicio_dt,
                        xlsx_por_turno=xlsx_por_turno,
                        videos_referencia_por_turno=videos_referencia_por_turno,
                    )
            except SoftTimeLimitExceeded as exc:
                if video is not None:
                    estado_error = _marcar_video_para_reintento(video, timezone.now(), exc)
                    detalles["errores"].append(f"{nombre_video}: {estado_error}.")
                raise
            except Exception as exc:
                if video is not None:
                    if isinstance(exc, VideoSinImagenUtilError):
                        ruta_actual = (video.ruta_archivo.name or "").strip()
                        if ruta_previa:
                            if ruta_actual and ruta_actual != ruta_previa:
                                try:
                                    default_storage.delete(ruta_actual)
                                except Exception:
                                    pass
                            video.ruta_archivo.name = ruta_previa
                            video.save(update_fields=["ruta_archivo"])
                        elif ruta_actual and ruta_actual == ruta_generada_en_intento:
                            try:
                                default_storage.delete(ruta_actual)
                            except Exception:
                                pass
                            video.ruta_archivo.name = ""
                            video.save(update_fields=["ruta_archivo"])
                    estado_error = _marcar_video_para_reintento(video, timezone.now(), exc)
                    detalles["errores"].append(f"{nombre_video}: {estado_error}.")
                else:
                    detalles["errores"].append(
                        f"{nombre_video}: error procesando video ({_normalizar_error(exc)})."
                    )
                detalles["videos_omitidos"] += 1
                continue
            finally:
                if os.path.exists(tmp_dir):
                    for root, _dirs, files in os.walk(tmp_dir):
                        for archivo in files:
                            try:
                                os.remove(os.path.join(root, archivo))
                            except OSError:
                                pass
                    try:
                        os.rmdir(tmp_dir)
                    except OSError:
                        pass

        for tipo_turno, videos in videos_turno.items():
            detalles["turnos_procesados"] += 1
            detalles["recortados"] += _alinear_duraciones(videos)

        if not importar_velocidades:
            continue

        for turno_id, video_ref in videos_referencia_por_turno.items():
            ruta_xlsx = xlsx_por_turno.get(turno_id)
            if not ruta_xlsx:
                _actualizar_estado_velocidades_turno(
                    video_ref.id_turno,
                    EstadoVelocidadesVideo.SIN_XLSX,
                    error="No se encontró XLSX asociado para este turno.",
                )
                continue
            try:
                with open(ruta_xlsx, "rb") as archivo:
                    importar_velocidades_xlsx(video_ref, archivo)
                _actualizar_estado_velocidades_turno(
                    video_ref.id_turno,
                    EstadoVelocidadesVideo.IMPORTADA,
                    actualizado_en=timezone.now(),
                )
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                _actualizar_estado_velocidades_turno(
                    video_ref.id_turno,
                    EstadoVelocidadesVideo.ERROR,
                    error=_normalizar_error(exc),
                )
                detalles["errores"].append(
                    f"{video_ref.nombre}: error importando velocidades ({exc})."
                )

    return detalles
