import hashlib
import os
import subprocess
import tempfile

from django.conf import settings
from rest_framework.exceptions import ValidationError

PREVIEW_SECONDS = 5
PREVIEW_WIDTH = 320
PREVIEW_CRF = 28
PREVIEW_PRESET = "veryfast"
RAW_EXTENSIONS = {".h264", ".grec"}


def _hash_preview_key(ruta_relativa: str, stat: os.stat_result) -> str:
    key = f"{ruta_relativa}|{stat.st_size}|{int(stat.st_mtime)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _build_preview_commands(ruta_entrada: str, ruta_salida: str, es_raw: bool) -> list[list[str]]:
    filtro_video = f"scale={PREVIEW_WIDTH}:-2"
    base = [
        "ffmpeg",
        "-y",
        "-t",
        str(PREVIEW_SECONDS),
        "-i",
        ruta_entrada,
        "-an",
        "-sn",
        "-vf",
        filtro_video,
        "-c:v",
        "libx264",
        "-preset",
        PREVIEW_PRESET,
        "-crf",
        str(PREVIEW_CRF),
        "-movflags",
        "+faststart",
        ruta_salida,
    ]

    with_probe = [
        "ffmpeg",
        "-y",
        "-probesize",
        "50M",
        "-analyzeduration",
        "50M",
        "-fflags",
        "+genpts",
        "-t",
        str(PREVIEW_SECONDS),
        "-i",
        ruta_entrada,
        "-an",
        "-sn",
        "-vf",
        filtro_video,
        "-c:v",
        "libx264",
        "-preset",
        PREVIEW_PRESET,
        "-crf",
        str(PREVIEW_CRF),
        "-movflags",
        "+faststart",
        ruta_salida,
    ]

    if not es_raw:
        return [base, with_probe]

    raw = [
        "ffmpeg",
        "-y",
        "-f",
        "h264",
        "-probesize",
        "50M",
        "-analyzeduration",
        "50M",
        "-fflags",
        "+genpts",
        "-t",
        str(PREVIEW_SECONDS),
        "-i",
        ruta_entrada,
        "-an",
        "-sn",
        "-vf",
        filtro_video,
        "-c:v",
        "libx264",
        "-preset",
        PREVIEW_PRESET,
        "-crf",
        str(PREVIEW_CRF),
        "-movflags",
        "+faststart",
        ruta_salida,
    ]
    return [with_probe, raw]


def _generar_preview(ruta_entrada: str, ruta_salida: str, es_raw: bool) -> None:
    errores: list[str] = []
    for idx, cmd in enumerate(_build_preview_commands(ruta_entrada, ruta_salida, es_raw), start=1):
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            return
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or exc.stdout or str(exc)).strip()
            errores.append(f"Intento {idx}: {stderr}")
        except OSError as exc:
            raise ValidationError(f"No se pudo ejecutar ffmpeg: {exc}") from exc

    detalles = "\n\n".join(errores) if errores else "Sin detalles del error."
    raise ValidationError(f"No se pudo generar el preview del video:\n{detalles}")


def obtener_preview_video(ruta_entrada: str, ruta_relativa: str) -> tuple[str, bool]:
    try:
        stat = os.stat(ruta_entrada)
    except OSError as exc:
        raise ValidationError("No se pudo acceder al archivo indicado.") from exc

    ruta_relativa = ruta_relativa.replace(os.sep, "/")
    hash_name = _hash_preview_key(ruta_relativa, stat)
    preview_dir = os.path.join(settings.MEDIA_ROOT, "previews")
    try:
        os.makedirs(preview_dir, exist_ok=True)
    except OSError as exc:
        raise ValidationError("No se pudo crear el directorio de previews.") from exc

    preview_filename = f"{hash_name}.mp4"
    preview_abs = os.path.join(preview_dir, preview_filename)
    preview_rel = os.path.join("previews", preview_filename).replace(os.sep, "/")

    if os.path.exists(preview_abs):
        return preview_rel, True

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4", dir=preview_dir) as tmp:
        tmp_path = tmp.name

    try:
        extension = os.path.splitext(ruta_entrada)[1].lower()
        es_raw = extension in RAW_EXTENSIONS
        _generar_preview(ruta_entrada, tmp_path, es_raw)
        if not os.path.exists(preview_abs):
            os.replace(tmp_path, preview_abs)
        else:
            os.remove(tmp_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    return preview_rel, False
