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
MAX_BYTES_SCAN_LONGITUD = 8 * 1024 * 1024
CODECS_VIDEO_MP4_COMPATIBLES = {"h264"}
PIX_FMT_MP4_COMPATIBLES = {"yuv420p"}
CODEC_TAGS_VIDEO_MP4_COMPATIBLES = {"avc1"}
PERFILES_VIDEO_MP4_COMPATIBLES = {
    "baseline",
    "constrained baseline",
    "main",
    "high",
}
NIVEL_MP4_COMPATIBLE_MAX = 41
FORZAR_TRANSCODIFICACION_MP4 = os.environ.get("FORZAR_TRANSCODIFICACION_MP4", "0") == "1"
_AUDIO_SAMPLE_RATE_MP4_DEFAULT = 44100
_AUDIO_SAMPLE_RATE_MIN_DEFAULT = 22050
_FPS_DIFF_UMBRAL_DEFAULT = 0.02
MP4_VIDEO_PROFILE = (os.environ.get("MP4_VIDEO_PROFILE", "baseline") or "").strip().lower()
MP4_VIDEO_LEVEL = (os.environ.get("MP4_VIDEO_LEVEL", "") or "").strip()
MP4_TARGET_FPS = (os.environ.get("MP4_TARGET_FPS", "") or "").strip()
try:
    AUDIO_SAMPLE_RATE_MP4 = int(
        os.environ.get("MP4_AUDIO_SAMPLE_RATE", _AUDIO_SAMPLE_RATE_MP4_DEFAULT)
    )
except ValueError:
    AUDIO_SAMPLE_RATE_MP4 = _AUDIO_SAMPLE_RATE_MP4_DEFAULT
try:
    AUDIO_SAMPLE_RATE_MIN_MP4 = int(
        os.environ.get("MP4_AUDIO_SAMPLE_RATE_MIN", _AUDIO_SAMPLE_RATE_MIN_DEFAULT)
    )
except ValueError:
    AUDIO_SAMPLE_RATE_MIN_MP4 = _AUDIO_SAMPLE_RATE_MIN_DEFAULT
try:
    FPS_DIFF_UMBRAL = float(os.environ.get("MP4_FPS_DIFF_UMBRAL", _FPS_DIFF_UMBRAL_DEFAULT))
except ValueError:
    FPS_DIFF_UMBRAL = _FPS_DIFF_UMBRAL_DEFAULT


def _tiene_start_codes(ruta_h264):
    with open(ruta_h264, "rb") as archivo:
        data = archivo.read(MAX_BYTES_START_CODES)
    return b"\x00\x00\x00\x01" in data or b"\x00\x00\x01" in data


def _convertir_longitudes_a_annexb(ruta_h264, ruta_salida, longitud_nal, offset=0):
    nals = 0
    try:
        with open(ruta_h264, "rb") as entrada, open(ruta_salida, "wb") as salida:
            if offset:
                entrada.seek(offset)
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


def _buscar_offset_start_code(ruta_h264, max_bytes=None):
    patrones = (b"\x00\x00\x00\x01", b"\x00\x00\x01")
    chunk_size = 1024 * 1024
    offset = 0
    tail = b""
    try:
        with open(ruta_h264, "rb") as archivo:
            while True:
                if max_bytes is not None and offset >= max_bytes:
                    return None
                to_read = chunk_size
                if max_bytes is not None:
                    to_read = min(chunk_size, max_bytes - offset)
                    if to_read <= 0:
                        return None
                chunk = archivo.read(to_read)
                if not chunk:
                    return None
                data = tail + chunk
                for patron in patrones:
                    idx = data.find(patron)
                    if idx != -1:
                        return offset - len(tail) + idx
                offset += len(chunk)
                tail = data[-3:]
    except OSError:
        return None


def _copiar_desde_offset(ruta_entrada, ruta_salida, offset):
    try:
        with open(ruta_entrada, "rb") as entrada, open(ruta_salida, "wb") as salida:
            entrada.seek(offset)
            while True:
                chunk = entrada.read(1024 * 1024)
                if not chunk:
                    return True
                salida.write(chunk)
    except OSError:
        return False


def _parece_stream_longitudes(data, offset, longitud_nal, min_nals):
    idx = offset
    for _ in range(min_nals):
        if idx + longitud_nal > len(data):
            return False
        nal_size = int.from_bytes(data[idx : idx + longitud_nal], "big")
        if nal_size <= 0 or nal_size > MAX_TAMANO_NAL:
            return False
        idx += longitud_nal
        if idx + nal_size > len(data):
            return False
        nal_type = data[idx] & 0x1F
        if not 1 <= nal_type <= 31:
            return False
        idx += nal_size
    return True


def _buscar_offset_longitudes(ruta_h264, longitud_nal, max_scan=MAX_BYTES_SCAN_LONGITUD, min_nals=3):
    try:
        with open(ruta_h264, "rb") as archivo:
            data = archivo.read(max_scan)
    except OSError:
        return None
    data_len = len(data)
    for offset in range(0, data_len - (longitud_nal + 1)):
        if _parece_stream_longitudes(data, offset, longitud_nal, min_nals):
            return offset
    return None


def _obtener_streams(ruta_video):
    probe = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name,codec_tag_string,pix_fmt,profile,level,disposition,r_frame_rate,avg_frame_rate,sample_rate",
            "-of",
            "json",
            ruta_video,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(probe.stdout)
    return data.get("streams", [])


def _seleccionar_stream_video(streams):
    for stream in streams:
        disposition = stream.get("disposition") or {}
        if disposition.get("attached_pic"):
            continue
        if stream.get("codec_type") != "video":
            continue
        return stream
    return None


def _seleccionar_stream_audio(streams):
    for stream in streams:
        if stream.get("codec_type") == "audio":
            return stream
    return None


def _parsear_fraccion(valor):
    if not valor or valor == "0/0":
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    if "/" not in valor:
        try:
            return float(valor)
        except ValueError:
            return None
    num, den = valor.split("/", 1)
    try:
        num = float(num)
        den = float(den)
    except ValueError:
        return None
    if den == 0:
        return None
    return num / den


def _mp4_es_compatible(stream_info):
    codec = (stream_info.get("codec_name") or "").lower()
    pix_fmt = (stream_info.get("pix_fmt") or "").lower()
    codec_tag = (stream_info.get("codec_tag_string") or "").lower()
    profile = (stream_info.get("profile") or "").lower()
    level = stream_info.get("level")

    if codec not in CODECS_VIDEO_MP4_COMPATIBLES:
        return False
    if pix_fmt not in PIX_FMT_MP4_COMPATIBLES:
        return False
    if not codec_tag or codec_tag not in CODEC_TAGS_VIDEO_MP4_COMPATIBLES:
        return False
    if not profile or profile not in PERFILES_VIDEO_MP4_COMPATIBLES:
        return False
    if level is not None:
        try:
            if int(level) > NIVEL_MP4_COMPATIBLE_MAX:
                return False
        except (TypeError, ValueError):
            return False
    return True


def _debe_normalizar_fps(stream_info):
    if MP4_TARGET_FPS:
        return True
    r_fps = _parsear_fraccion(stream_info.get("r_frame_rate"))
    avg_fps = _parsear_fraccion(stream_info.get("avg_frame_rate"))
    if not r_fps or not avg_fps:
        return False
    if r_fps == 0:
        return False
    return abs(r_fps - avg_fps) / r_fps > FPS_DIFF_UMBRAL


def _obtener_fps_expr(stream_info):
    if MP4_TARGET_FPS:
        return MP4_TARGET_FPS
    r_rate = stream_info.get("r_frame_rate")
    avg_rate = stream_info.get("avg_frame_rate")
    r_fps = _parsear_fraccion(r_rate)
    avg_fps = _parsear_fraccion(avg_rate)
    if r_fps and avg_fps:
        if abs(r_fps - avg_fps) / r_fps > FPS_DIFF_UMBRAL:
            return r_rate
        return avg_rate
    return r_rate or avg_rate


def _audio_muy_baja(audio_stream):
    if not audio_stream:
        return False
    sample_rate = audio_stream.get("sample_rate")
    try:
        return int(sample_rate) < AUDIO_SAMPLE_RATE_MIN_MP4
    except (TypeError, ValueError):
        return False


def _mp4_requiere_transcodificacion(stream_info, audio_stream):
    if not _mp4_es_compatible(stream_info):
        return True
    if _debe_normalizar_fps(stream_info):
        return True
    if _audio_muy_baja(audio_stream):
        return True
    return False


def _transcodificar_mp4(ruta_mp4, stream_info):
    destino_dir = os.path.dirname(ruta_mp4) or "."
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4", dir=destino_dir) as tmp:
        ruta_salida = tmp.name

    video_index = stream_info.get("index")
    map_video = f"0:{video_index}" if video_index is not None else "0:v:0"

    fps_expr = _obtener_fps_expr(stream_info)
    filtro_video = "format=yuv420p"
    if fps_expr:
        filtro_video = f"fps={fps_expr},{filtro_video}"

    cmd = [
        "ffmpeg",
        "-y",
        "-fflags",
        "+genpts",
        "-i",
        ruta_mp4,
        "-map",
        map_video,
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-pix_fmt",
        "yuv420p",
        "-vf",
        filtro_video,
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ar",
        str(AUDIO_SAMPLE_RATE_MP4),
        "-movflags",
        "+faststart",
        ruta_salida,
    ]
    if MP4_VIDEO_PROFILE:
        cmd.extend(["-profile:v", MP4_VIDEO_PROFILE])
    if MP4_VIDEO_LEVEL:
        cmd.extend(["-level", MP4_VIDEO_LEVEL])
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or exc.stdout or str(exc)).strip()
        if os.path.exists(ruta_salida):
            os.remove(ruta_salida)
        raise ValidationError(f"No se pudo convertir el MP4 a un formato compatible: {stderr}")

    salida_stream = _seleccionar_stream_video(_obtener_streams(ruta_salida))
    if not salida_stream:
        if os.path.exists(ruta_salida):
            os.remove(ruta_salida)
        raise ValidationError("El MP4 convertido no contiene pista de video.")
    if not _mp4_es_compatible(salida_stream):
        if os.path.exists(ruta_salida):
            os.remove(ruta_salida)
        raise ValidationError("El MP4 convertido no es compatible con navegadores.")

    os.replace(ruta_salida, ruta_mp4)
    return True


def asegurar_mp4_compatible(ruta_mp4):
    streams = _obtener_streams(ruta_mp4)
    stream_info = _seleccionar_stream_video(streams)
    audio_stream = _seleccionar_stream_audio(streams)
    if not stream_info:
        raise ValidationError("El archivo MP4 no contiene pista de video.")
    if not FORZAR_TRANSCODIFICACION_MP4 and not _mp4_requiere_transcodificacion(
        stream_info, audio_stream
    ):
        return False
    return _transcodificar_mp4(ruta_mp4, stream_info)

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
            if _convertir_longitudes_a_annexb(ruta_h264, ruta_annexb, longitud, offset=0):
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

        offset_start = _buscar_offset_start_code(ruta_h264)
        ruta_recortada = None
        if offset_start is not None and offset_start > 0:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".h264") as tmp:
                ruta_recortada = tmp.name
            if _copiar_desde_offset(ruta_h264, ruta_recortada, offset_start):
                if intentar_conversion(
                    ruta_recortada, f"Intento recorte-startcode@{offset_start}"
                ):
                    os.remove(ruta_recortada)
                    return ruta_salida
            else:
                errores.append(
                    "Intento recorte-startcode: no se pudo copiar el stream desde el offset"
                )
            if ruta_recortada and os.path.exists(ruta_recortada):
                os.remove(ruta_recortada)
        else:
            errores.append("Intento recorte-startcode: no se encontró start code en el archivo")

        for longitud in LONGITUDES_NAL_H264:
            offset = _buscar_offset_longitudes(ruta_h264, longitud)
            if offset is None:
                errores.append(
                    f"Intento annexb-{longitud}-offset: no se encontró un offset válido"
                )
                continue
            if offset == 0:
                continue
            with tempfile.NamedTemporaryFile(delete=False, suffix=".annexb.h264") as tmp:
                ruta_annexb = tmp.name
            if _convertir_longitudes_a_annexb(ruta_h264, ruta_annexb, longitud, offset=offset):
                if intentar_conversion(
                    ruta_annexb, f"Intento annexb-{longitud}-offset@{offset}"
                ):
                    os.remove(ruta_annexb)
                    return ruta_salida
            else:
                errores.append(
                    f"Intento annexb-{longitud}-offset@{offset}: no se pudo convertir longitudes a start codes"
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

        ruta_final = video_obj.ruta_archivo.path
        if ruta_final.lower().endswith(".mp4"):
            asegurar_mp4_compatible(ruta_final)

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
