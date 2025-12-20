from rest_framework.exceptions import ValidationError
import datetime, json, subprocess

### Tengo que acordarme de poner constantes en mayusculas
FORMATO_VIDEO_VALIDO = ["video/mp4"]

def validar_formato(video):
    if not video or video.content_type not in FORMATO_VIDEO_VALIDO:
        raise ValidationError("Formato de video no válido. Solo se permiten archivos MP4.")


def calcular_duracion_video(video):
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", video],
        capture_output=True,
        text=True,
        check=True,
    )
    seconds = float(json.loads(probe.stdout)["format"]["duration"])
    return datetime.timedelta(seconds=seconds).total_seconds()
