import hashlib
import os
import time as time_module
from datetime import datetime

from django.conf import settings
from django.core.files.storage import default_storage
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from dashboard.services.calcular_duracion_video import prevalidar_video_origen
from dashboard.services.video_commands import asegurar_permisos_archivo

_VIDEO_IMPORT_HASH_CHUNK_SIZE_DEFAULT = 1024 * 1024
_VIDEO_IMPORT_MIN_FILE_AGE_SECONDS_DEFAULT = 15
_VIDEO_IMPORT_STABILITY_CHECKS_DEFAULT = 2
_VIDEO_IMPORT_STABILITY_INTERVAL_MS_DEFAULT = 1000


def asegurar_permisos_storage(nombre_relativo: str) -> None:
    """Permite que Nginx sirva archivos guardados por Celery/Django."""
    try:
        ruta = default_storage.path(nombre_relativo)
    except (AttributeError, NotImplementedError, ValueError):
        return

    asegurar_permisos_archivo(ruta)


def _get_int_setting(name: str, default: int, *, minimum: int = 0) -> int:
    value = getattr(settings, name, os.environ.get(name, default))
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


VIDEO_IMPORT_HASH_CHUNK_SIZE = _get_int_setting(
    "VIDEO_IMPORT_HASH_CHUNK_SIZE",
    _VIDEO_IMPORT_HASH_CHUNK_SIZE_DEFAULT,
    minimum=1024,
)
VIDEO_IMPORT_MIN_FILE_AGE_SECONDS = _get_int_setting(
    "VIDEO_IMPORT_MIN_FILE_AGE_SECONDS",
    _VIDEO_IMPORT_MIN_FILE_AGE_SECONDS_DEFAULT,
)
VIDEO_IMPORT_STABILITY_CHECKS = _get_int_setting(
    "VIDEO_IMPORT_STABILITY_CHECKS",
    _VIDEO_IMPORT_STABILITY_CHECKS_DEFAULT,
    minimum=1,
)
VIDEO_IMPORT_STABILITY_INTERVAL_MS = _get_int_setting(
    "VIDEO_IMPORT_STABILITY_INTERVAL_MS",
    _VIDEO_IMPORT_STABILITY_INTERVAL_MS_DEFAULT,
)


def _stat_archivo(origen_real: str) -> os.stat_result:
    try:
        return os.stat(origen_real)
    except OSError as exc:
        raise ValidationError("No se pudo acceder al archivo indicado.") from exc


def _firma_stat(stat: os.stat_result) -> tuple[int, int]:
    return stat.st_size, getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))


def _asegurar_antiguedad_minima(stat: os.stat_result) -> None:
    edad_segundos = time_module.time() - stat.st_mtime
    if edad_segundos < VIDEO_IMPORT_MIN_FILE_AGE_SECONDS:
        raise ValidationError(
            "El archivo aún es demasiado reciente; espere a que termine la subida."
        )


def _asegurar_archivo_estable(origen_real: str) -> os.stat_result:
    ultimo_stat = None
    firmas = []
    for idx in range(VIDEO_IMPORT_STABILITY_CHECKS):
        stat = _stat_archivo(origen_real)
        if stat.st_size <= 0:
            raise ValidationError("El archivo está vacío o aún no terminó de subirse.")
        if idx == 0:
            _asegurar_antiguedad_minima(stat)
        firmas.append(_firma_stat(stat))
        ultimo_stat = stat
        if idx + 1 < VIDEO_IMPORT_STABILITY_CHECKS and VIDEO_IMPORT_STABILITY_INTERVAL_MS > 0:
            time_module.sleep(VIDEO_IMPORT_STABILITY_INTERVAL_MS / 1000)

    if len(set(firmas)) != 1:
        raise ValidationError("El archivo aún está cambiando; espere a que termine la subida.")
    return ultimo_stat


def _calcular_sha256_archivo(origen_real: str, firma_esperada: tuple[int, int]) -> str:
    hasher = hashlib.sha256()
    try:
        with open(origen_real, "rb") as archivo_origen:
            while True:
                chunk = archivo_origen.read(VIDEO_IMPORT_HASH_CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
    except OSError as exc:
        raise ValidationError("No se pudo leer el archivo indicado.") from exc

    firma_final = _firma_stat(_stat_archivo(origen_real))
    if firma_final != firma_esperada:
        raise ValidationError("El archivo cambió mientras se inspeccionaba; intente nuevamente.")
    return hasher.hexdigest()


def inspeccionar_origen_importacion(origen_real: str) -> dict:
    stat = _asegurar_archivo_estable(origen_real)
    firma = _firma_stat(stat)
    prevalidar_video_origen(origen_real)
    sha256 = _calcular_sha256_archivo(origen_real, firma)
    return {
        "firma": firma,
        "sha256": sha256,
        "tamano_bytes": stat.st_size,
        "modificado_en": datetime.fromtimestamp(
            stat.st_mtime,
            tz=timezone.get_current_timezone(),
        ),
    }
