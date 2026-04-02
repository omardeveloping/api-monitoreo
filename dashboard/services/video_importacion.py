import os

from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from django.utils import timezone
from django.utils.text import get_valid_filename
from rest_framework.exceptions import ValidationError

from dashboard.models import Video
from dashboard.services.calcular_duracion_video import procesar_video_subida


def obtener_base_importacion() -> str:
    base_dir = getattr(settings, "VIDEOS_IMPORT_DIR", "")
    if not base_dir:
        raise ValidationError("VIDEOS_IMPORT_DIR no está configurado en el servidor.")

    base_dir_real = os.path.realpath(base_dir)
    if not os.path.isdir(base_dir_real):
        raise ValidationError("VIDEOS_IMPORT_DIR no apunta a un directorio válido.")
    return base_dir_real


def resolver_ruta_importacion(base_dir_real: str, ruta_relativa: str) -> tuple[str, str]:
    ruta_relativa = (ruta_relativa or "").strip()
    if not ruta_relativa:
        raise ValidationError("Debe indicar 'ruta_origen'.")
    if os.path.isabs(ruta_relativa):
        raise ValidationError("La ruta debe ser relativa al directorio configurado.")

    origen_real = os.path.realpath(os.path.join(base_dir_real, ruta_relativa))
    if not (origen_real == base_dir_real or origen_real.startswith(base_dir_real + os.sep)):
        raise ValidationError("La ruta indicada sale del directorio permitido.")
    if not os.path.isfile(origen_real):
        raise ValidationError("El archivo indicado no existe.")
    return ruta_relativa.replace(os.sep, "/"), origen_real


def copiar_archivo_a_storage(origen_real: str, *, carpeta_destino: str = "videos") -> tuple[str, str]:
    nombre_archivo = get_valid_filename(os.path.basename(origen_real))
    if not nombre_archivo:
        raise ValidationError("Nombre de archivo inválido.")

    destino_rel = default_storage.get_available_name(os.path.join(carpeta_destino, nombre_archivo))
    with open(origen_real, "rb") as archivo_origen:
        destino_rel = default_storage.save(destino_rel, File(archivo_origen))
    return destino_rel, nombre_archivo


def crear_video_desde_serializer(serializer) -> Video:
    video = serializer.save()
    archivo = serializer.validated_data.get("ruta_archivo")
    procesar_video_subida(video, archivo)
    return video


def crear_video_desde_ruta_servidor(validated_data: dict, origen_real: str) -> Video:
    destino_rel, nombre_archivo = copiar_archivo_a_storage(origen_real)
    nombre = validated_data.get("nombre") or os.path.splitext(nombre_archivo)[0]

    video = Video.objects.create(
        nombre=nombre,
        camara=validated_data["camara"],
        ruta_archivo=destino_rel,
        fecha_inicio=validated_data.get("fecha_inicio"),
        fecha_subida=validated_data.get("fecha_subida") or timezone.localdate(),
        inicio_timestamp=validated_data.get("inicio_timestamp"),
        id_turno=validated_data["id_turno"],
    )
    procesar_video_subida(video, video.ruta_archivo)
    return video
