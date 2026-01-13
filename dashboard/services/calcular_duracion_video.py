import datetime
import json
import math
import mimetypes
import os
import subprocess
import tempfile

from django.conf import settings
from rest_framework.exceptions import ValidationError

from dashboard.models import EstadoVideo

### Tengo que acordarme de poner constantes en mayusculas
FORMATO_VIDEO_VALIDO = {"video/mp4", "video/h264", "video/x-h264"}
EXTENSIONES_VALIDAS = {".mp4", ".h264"}
MAX_BYTES_START_CODES = 1024 * 1024
MAX_TAMANO_NAL = 50 * 1024 * 1024
LONGITUDES_NAL_H264 = (4, 3)


def _tiene_start_codes(ruta_h264):
    with open(ruta_h264, "rb") as archivo:
        data = archivo.read(MAX_BYTES_START_CODES)
    return b"\x00\x00\x00\x01" in data or b"\x00\x00\x01" in data


def _convertir_longitudes_a_annexb(ruta_h264, ruta_salida, longitud_nal):
    nals = 0
    try:
        with open(ruta_h264, "rb") as entrada, open(ruta_salida, "wb") as salida:
            while True:
                size_bytes = entrada.read(longitud_nal)
                if not size_bytes:
                    return nals > 0
                if len(size_bytes) != longitud_nal:
                    return False
                nal_size = int.from_bytes(size_bytes, "big")
                if nal_size <= 0 or nal_size > MAX_TAMANO_NAL:
                    return False
                nal = entrada.read(nal_size)
                if len(nal) != nal_size:
                    return False
                nal_type = nal[0] & 0x1F
                if not 1 <= nal_type <= 31:
                    return False
                salida.write(b"\x00\x00\x00\x01")
                salida.write(nal)
                nals += 1
    except OSError:
        return False

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

    def construir_comandos_ffmpeg(ruta_entrada):
        return [
            [
                "ffmpeg",
                "-y",
                "-i",
                ruta_entrada,
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                ruta_salida,
            ],
            [
                "ffmpeg",
                "-y",
                "-probesize",
                "50M",
                "-analyzeduration",
                "50M",
                "-fflags",
                "+genpts",
                "-i",
                ruta_entrada,
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                ruta_salida,
            ],
            [
                "ffmpeg",
                "-y",
                "-probesize",
                "50M",
                "-analyzeduration",
                "50M",
                "-fflags",
                "+genpts",
                "-i",
                ruta_entrada,
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
                ruta_salida,
            ],
        ]

    def intentar_conversion(ruta_entrada, etiqueta):
        for idx, cmd in enumerate(construir_comandos_ffmpeg(ruta_entrada), start=1):
            try:
                subprocess.run(
                    cmd,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return True
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or exc.stdout or str(exc)).strip()
                errores.append(f"{etiqueta} {idx}: {stderr}")
        return False

    errores = []
    if intentar_conversion(ruta_h264, "Intento"):
        return ruta_salida

    ruta_annexb = None
    if not _tiene_start_codes(ruta_h264):
        for longitud in LONGITUDES_NAL_H264:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".annexb.h264") as tmp:
                ruta_annexb = tmp.name
            if _convertir_longitudes_a_annexb(ruta_h264, ruta_annexb, longitud):
                if intentar_conversion(ruta_annexb, f"Intento annexb-{longitud}"):
                    os.remove(ruta_annexb)
                    return ruta_salida
            else:
                errores.append(
                    f"Intento annexb-{longitud}: no se pudo convertir longitudes a start codes"
                )
            if ruta_annexb and os.path.exists(ruta_annexb):
                os.remove(ruta_annexb)
            ruta_annexb = None

    if os.path.exists(ruta_salida):
        os.remove(ruta_salida)

    detalles = "\n\n".join(errores) if errores else "Sin detalles del error."
    raise ValidationError(
        f"No se pudo convertir el video a MP4 tras varios intentos:\n{detalles}"
    )


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
