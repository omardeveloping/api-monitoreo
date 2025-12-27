import datetime
import json
import math
import mimetypes
import os
import subprocess

from django.conf import settings
from rest_framework.exceptions import ValidationError

from dashboard.models import EstadoVideo

### Tengo que acordarme de poner constantes en mayusculas
FORMATO_VIDEO_VALIDO = {"video/mp4", "video/h264", "video/x-h264"}
EXTENSIONES_VALIDAS = {".mp4", ".h264"}

def validar_formato(video):
    content_type = (getattr(video, "content_type", "") or "").lower()
    nombre_archivo = getattr(video, "name", "") or ""
    extension = os.path.splitext(nombre_archivo)[1].lower()

    permitido_por_content_type = content_type in FORMATO_VIDEO_VALIDO
    permitido_por_extension = extension in EXTENSIONES_VALIDAS

    if not video or not (permitido_por_content_type or permitido_por_extension):
        raise ValidationError("Formato de video no válido. Solo se permiten archivos MP4 o H264.")


def envolver_h264_en_mp4(ruta_h264):
    if not ruta_h264:
        raise ValidationError("No se encontró la ruta del archivo H264.")

    ruta_salida = os.path.splitext(ruta_h264)[0] + ".mp4"
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                ruta_h264,
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                ruta_salida,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise ValidationError(f"No se pudo convertir el video a MP4: {exc.stderr}") from exc

    return ruta_salida


def calcular_duracion_video(video):
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", video],
        capture_output=True,
        text=True,
        check=True,
    )
    seconds = float(json.loads(probe.stdout)["format"]["duration"])
    return datetime.timedelta(seconds=seconds).total_seconds()


def procesar_video_subida(video_obj, archivo):
    """
    Valida, convierte H264 a MP4 si es necesario, calcula duración y persiste cambios.
    Elimina archivos y el registro si ocurre algún error para evitar residuos.
    """
    validar_formato(archivo)
    content_type = (getattr(archivo, "content_type", "") or "").lower()

    ruta_original = video_obj.ruta_archivo.path
    ruta_convertida = None

    try:
        if content_type in {"video/h264", "video/x-h264"} or ruta_original.lower().endswith(".h264"):
            ruta_convertida = envolver_h264_en_mp4(ruta_original)
            video_obj.ruta_archivo.name = os.path.relpath(ruta_convertida, settings.MEDIA_ROOT)

        video_obj.duracion = math.floor(calcular_duracion_video(video_obj.ruta_archivo.path))
        video_obj.estado = EstadoVideo.LISTO

        inicio = video_obj.inicio_timestamp or datetime.time(0, 0)
        if isinstance(inicio, datetime.datetime):
            inicio = inicio.time()
        fin = (
            datetime.datetime.combine(datetime.date.today(), inicio)
            + datetime.timedelta(seconds=video_obj.duracion or 0)
        ).time()
        video_obj.inicio_timestamp = inicio
        video_obj.fin_timestamp = fin

        final_mimetype = mimetypes.guess_type(video_obj.ruta_archivo.path)[0] or content_type or ""
        video_obj.mimetype = final_mimetype

        campos = ["duracion", "estado", "inicio_timestamp", "fin_timestamp", "mimetype"]
        if ruta_convertida:
            campos.append("ruta_archivo")
        video_obj.save(update_fields=campos)

        if ruta_convertida and ruta_convertida != ruta_original and os.path.exists(ruta_original):
            os.remove(ruta_original)
    except Exception:
        if ruta_convertida and ruta_convertida != ruta_original and os.path.exists(ruta_convertida):
            os.remove(ruta_convertida)
        video_obj.estado = EstadoVideo.ERROR
        video_obj.save(update_fields=["estado"])
        raise

    return video_obj
