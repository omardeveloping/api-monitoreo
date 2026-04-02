import hashlib
import os
import re
import shutil
import tempfile
import time as time_module
from datetime import datetime, timedelta

from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from django.utils import timezone
from django.utils.text import get_valid_filename
from rest_framework.exceptions import ValidationError

from dashboard.models import EstadoVideo, Video
from dashboard.services.calcular_duracion_video import (
    EXTENSIONES_VALIDAS,
    H264_EXTENSIONS,
    envolver_h264_en_mp4,
    prevalidar_video_origen,
    procesar_video_subida,
)
from dashboard.services.video_commands import build_ffmpeg_command, remove_if_exists, run_command

_PATRON_NOMBRE_VIDEO = re.compile(
    r"^(?P<equipo>\d+)-(?P<fecha>\d{6})-(?P<inicio>\d{6})-(?P<fin>\d{6})-(?P<codigo>\d+)$"
)
_VIDEO_IMPORT_HASH_CHUNK_SIZE_DEFAULT = 1024 * 1024
_VIDEO_IMPORT_MIN_FILE_AGE_SECONDS_DEFAULT = 15
_VIDEO_IMPORT_STABILITY_CHECKS_DEFAULT = 2
_VIDEO_IMPORT_STABILITY_INTERVAL_MS_DEFAULT = 1000


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


def _parsear_nombre_video(nombre_archivo: str) -> dict | None:
    base, _ext = os.path.splitext(os.path.basename(nombre_archivo or ""))
    match = _PATRON_NOMBRE_VIDEO.match(base)
    if not match:
        return None

    fecha = match.group("fecha")
    try:
        dia = int(fecha[0:2])
        mes = int(fecha[2:4])
        ano = 2000 + int(fecha[4:6])
        fecha_obj = datetime(ano, mes, dia).date()
    except (ValueError, TypeError):
        return None

    def _parsear_hora(valor: str):
        try:
            hh = int(valor[0:2])
            mm = int(valor[2:4])
            ss = int(valor[4:6])
        except (ValueError, TypeError):
            return None
        if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
            return None
        return datetime(ano, mes, dia, hh, mm, ss).time()

    inicio = _parsear_hora(match.group("inicio"))
    fin = _parsear_hora(match.group("fin"))
    if not inicio or not fin:
        return None

    return {
        "equipo": match.group("equipo"),
        "fecha": fecha_obj,
        "inicio": inicio,
        "fin": fin,
        "codigo": match.group("codigo"),
    }


def _intervalo_segmento(parsed: dict) -> tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    inicio_dt = timezone.make_aware(datetime.combine(parsed["fecha"], parsed["inicio"]), tz)
    fin_dt = timezone.make_aware(datetime.combine(parsed["fecha"], parsed["fin"]), tz)
    if fin_dt <= inicio_dt:
        fin_dt += timedelta(days=1)
    return inicio_dt, fin_dt


def _intervalo_turno(turno) -> tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    inicio_dt = timezone.make_aware(datetime.combine(turno.fecha, turno.hora_inicio), tz)
    fin_dt = timezone.make_aware(datetime.combine(turno.fecha, turno.hora_fin), tz)
    if fin_dt <= inicio_dt:
        fin_dt += timedelta(days=1)
    return inicio_dt, fin_dt


def _segmento_corresponde_a_turno(parsed: dict, turno) -> bool:
    inicio_segmento, _ = _intervalo_segmento(parsed)
    inicio_turno, fin_turno = _intervalo_turno(turno)
    return inicio_turno <= inicio_segmento < fin_turno


def _duracion_esperada_segmento(parsed: dict) -> int:
    inicio_dt, fin_dt = _intervalo_segmento(parsed)
    return int((fin_dt - inicio_dt).total_seconds())


def formatear_nombre_video(nombre_archivo: str) -> str:
    parsed = _parsear_nombre_video(nombre_archivo)
    if not parsed:
        return nombre_archivo
    return (
        f"{parsed['equipo']} | {parsed['fecha'].isoformat()} | "
        f"{parsed['inicio'].strftime('%H:%M:%S')}-{parsed['fin'].strftime('%H:%M:%S')} | "
        f"{parsed['codigo']}"
    )


def inferir_metadatos_desde_nombre(nombre_archivo: str) -> dict:
    parsed = _parsear_nombre_video(nombre_archivo)
    if not parsed:
        return {}

    inicio_dt, _ = _intervalo_segmento(parsed)
    return {
        "fecha_inicio": inicio_dt,
        "inicio_timestamp": parsed["inicio"],
        "duracion_esperada_segundos": _duracion_esperada_segmento(parsed),
    }


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


def _nombre_video_grupo(parsed: dict, turno, camara: int) -> str:
    tipo_turno = (turno.tipo_turno or "turno").upper()
    return f"MDVR_{parsed['equipo']}_{parsed['fecha'].isoformat()}_{tipo_turno}_C{camara}"


def _ruta_relativa_candidata(ruta_origen: str, nombre_archivo: str) -> str:
    directorio_rel = os.path.dirname(ruta_origen)
    if not directorio_rel:
        return nombre_archivo
    return os.path.join(directorio_rel, nombre_archivo).replace(os.sep, "/")


def _grupo_segmentos_desde_nombre(validated_data: dict, origen_real: str, ruta_origen: str) -> dict | None:
    nombre_actual = os.path.basename(origen_real)
    parsed_actual = _parsear_nombre_video(nombre_actual)
    if not parsed_actual:
        return None

    turno = validated_data["id_turno"]
    camara = int(validated_data["camara"])
    directorio_abs = os.path.dirname(origen_real) or "."
    candidatos = []

    try:
        nombres = sorted(os.listdir(directorio_abs))
    except OSError as exc:
        raise ValidationError("No se pudo listar el directorio de origen.") from exc

    for nombre in nombres:
        ruta_abs = os.path.join(directorio_abs, nombre)
        if not os.path.isfile(ruta_abs):
            continue
        extension = os.path.splitext(nombre)[1].lower()
        if extension not in EXTENSIONES_VALIDAS:
            continue
        parsed = _parsear_nombre_video(nombre)
        if not parsed:
            continue
        if (
            parsed["equipo"] != parsed_actual["equipo"]
            or parsed["fecha"] != parsed_actual["fecha"]
            or parsed["codigo"] != parsed_actual["codigo"]
        ):
            continue
        if not _segmento_corresponde_a_turno(parsed, turno):
            continue

        inicio_dt, fin_dt = _intervalo_segmento(parsed)
        candidatos.append(
            {
                "nombre_archivo": nombre,
                "ruta_abs": ruta_abs,
                "ruta_rel": _ruta_relativa_candidata(ruta_origen, nombre),
                "extension": extension,
                "parsed": parsed,
                "inicio_dt": inicio_dt,
                "fin_dt": fin_dt,
            }
        )

    if not candidatos:
        return None

    candidatos.sort(key=lambda item: (item["inicio_dt"], item["ruta_rel"]))
    grupo_origen = (
        f"mdvr:{parsed_actual['equipo']}:{parsed_actual['fecha'].isoformat()}:"
        f"{parsed_actual['codigo']}:turno:{turno.pk}:camara:{camara}"
    )
    return {
        "grupo_origen": grupo_origen,
        "nombre_video": _nombre_video_grupo(parsed_actual, turno, camara),
        "segmentos": candidatos,
    }


def _inspeccionar_segmentos_grupo(grupo: dict) -> dict:
    segmentos = []
    total_bytes = 0
    ultima_modificacion = None
    duracion_esperada = 0
    hash_grupo = hashlib.sha256()

    for segmento in grupo["segmentos"]:
        inspeccion = inspeccionar_origen_importacion(segmento["ruta_abs"])
        segmentos.append({**segmento, **inspeccion})
        total_bytes += inspeccion["tamano_bytes"]
        duracion_esperada += _duracion_esperada_segmento(segmento["parsed"])
        if ultima_modificacion is None or inspeccion["modificado_en"] > ultima_modificacion:
            ultima_modificacion = inspeccion["modificado_en"]
        hash_grupo.update(segmento["ruta_rel"].encode("utf-8"))
        hash_grupo.update(b"|")
        hash_grupo.update(inspeccion["sha256"].encode("utf-8"))
        hash_grupo.update(b"\n")

    primer_segmento = segmentos[0]
    return {
        "grupo_origen": grupo["grupo_origen"],
        "nombre_video": grupo["nombre_video"],
        "segmentos": segmentos,
        "ruta_origen": primer_segmento["ruta_rel"],
        "origen_sha256": hash_grupo.hexdigest(),
        "origen_tamano_bytes": total_bytes,
        "origen_modificado_en": ultima_modificacion,
        "fecha_inicio": primer_segmento["inicio_dt"],
        "inicio_timestamp": primer_segmento["parsed"]["inicio"],
        "duracion_esperada": duracion_esperada,
    }


def _construir_manifest_individual(
    validated_data: dict,
    origen_real: str,
    ruta_origen: str,
) -> dict:
    inspeccion = inspeccionar_origen_importacion(origen_real)
    nombre_archivo = get_valid_filename(os.path.basename(origen_real))
    if not nombre_archivo:
        raise ValidationError("Nombre de archivo inválido.")

    metadatos_nombre = inferir_metadatos_desde_nombre(nombre_archivo)
    nombre_video = validated_data.get("nombre") or os.path.splitext(nombre_archivo)[0]
    extension = os.path.splitext(nombre_archivo)[1].lower()
    return {
        "grupo_origen": "",
        "nombre_video": nombre_video,
        "nombre_destino": f"{nombre_video}{extension}",
        "ruta_origen": ruta_origen,
        "segmentos_origen": [ruta_origen],
        "origen_sha256": inspeccion["sha256"],
        "origen_tamano_bytes": inspeccion["tamano_bytes"],
        "origen_modificado_en": inspeccion["modificado_en"],
        "fecha_inicio": validated_data.get("fecha_inicio") or metadatos_nombre.get("fecha_inicio"),
        "inicio_timestamp": (
            validated_data.get("inicio_timestamp") or metadatos_nombre.get("inicio_timestamp")
        ),
        "duracion_esperada": metadatos_nombre.get("duracion_esperada_segundos"),
        "artifact_abs": origen_real,
        "artifact_cleanup": [],
        "firma_copia": inspeccion["firma"],
    }


def _crear_temporal(suffix: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        return tmp.name


def _concatenar_segmentos_binarios(segmentos: list[dict]) -> tuple[str, list[str]]:
    extension_salida = ".grec" if any(seg["extension"] == ".grec" for seg in segmentos) else ".h264"
    ruta_salida = _crear_temporal(extension_salida)
    try:
        with open(ruta_salida, "wb") as salida:
            for segmento in segmentos:
                with open(segmento["ruta_abs"], "rb") as entrada:
                    shutil.copyfileobj(entrada, salida)
    except OSError as exc:
        remove_if_exists(ruta_salida)
        raise ValidationError("No se pudieron concatenar los segmentos raw.") from exc
    return ruta_salida, [ruta_salida]


def _escribir_concat_list(rutas_mp4: list[str]) -> str:
    ruta_lista = _crear_temporal(".ffconcat.txt")
    try:
        with open(ruta_lista, "w", encoding="utf-8") as archivo_lista:
            for ruta in rutas_mp4:
                ruta_escapada = ruta.replace("'", "\\'")
                archivo_lista.write(f"file '{ruta_escapada}'\n")
    except OSError as exc:
        remove_if_exists(ruta_lista)
        raise ValidationError("No se pudo preparar la lista de concatenación.") from exc
    return ruta_lista


def _concatenar_mp4(rutas_mp4: list[str]) -> tuple[str, list[str]]:
    ruta_lista = _escribir_concat_list(rutas_mp4)
    ruta_salida = _crear_temporal(".mp4")
    cleanup = [ruta_lista, ruta_salida]
    try:
        run_command(
            build_ffmpeg_command(
                ruta_lista,
                ruta_salida,
                input_args=["-f", "concat", "-safe", "0"],
                output_args=["-c", "copy", "-movflags", "+faststart"],
            ),
            error_prefix="No se pudieron concatenar los segmentos MP4",
        )
        return ruta_salida, cleanup
    except ValidationError:
        remove_if_exists(ruta_salida)
        ruta_salida = _crear_temporal(".mp4")
        cleanup[-1] = ruta_salida
        run_command(
            build_ffmpeg_command(
                ruta_lista,
                ruta_salida,
                input_args=["-f", "concat", "-safe", "0"],
                output_args=[
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-crf",
                    "23",
                    "-pix_fmt",
                    "yuv420p",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "128k",
                    "-movflags",
                    "+faststart",
                ],
            ),
            error_prefix="No se pudieron reconstruir los segmentos MP4",
        )
        return ruta_salida, cleanup


def _normalizar_segmentos_a_mp4(segmentos: list[dict]) -> tuple[list[str], list[str]]:
    rutas_mp4 = []
    cleanup = []
    for segmento in segmentos:
        if segmento["extension"] == ".mp4":
            rutas_mp4.append(segmento["ruta_abs"])
            continue
        ruta_temporal_raw = _crear_temporal(segmento["extension"])
        try:
            shutil.copyfile(segmento["ruta_abs"], ruta_temporal_raw)
            ruta_mp4 = envolver_h264_en_mp4(ruta_temporal_raw)
        except OSError as exc:
            remove_if_exists(ruta_temporal_raw)
            raise ValidationError("No se pudo preparar un segmento raw para reconstrucción.") from exc
        cleanup.extend([ruta_temporal_raw, ruta_mp4])
        rutas_mp4.append(ruta_mp4)
    return rutas_mp4, cleanup


def _materializar_artefacto_grupo(manifest: dict) -> tuple[str, list[str], str]:
    segmentos = manifest["segmentos"]
    if len(segmentos) == 1:
        extension = segmentos[0]["extension"]
        return segmentos[0]["ruta_abs"], [], f"{manifest['nombre_video']}{extension}"

    extensiones = {segmento["extension"] for segmento in segmentos}
    if extensiones.issubset(H264_EXTENSIONS):
        ruta_salida, cleanup = _concatenar_segmentos_binarios(segmentos)
        return ruta_salida, cleanup, f"{manifest['nombre_video']}.h264"

    if extensiones == {".mp4"}:
        ruta_salida, cleanup = _concatenar_mp4([segmento["ruta_abs"] for segmento in segmentos])
        return ruta_salida, cleanup, f"{manifest['nombre_video']}.mp4"

    rutas_mp4, cleanup_segmentos = _normalizar_segmentos_a_mp4(segmentos)
    ruta_salida, cleanup_concat = _concatenar_mp4(rutas_mp4)
    return ruta_salida, cleanup_segmentos + cleanup_concat, f"{manifest['nombre_video']}.mp4"


def _construir_manifest_grupo(
    validated_data: dict,
    origen_real: str,
    ruta_origen: str,
) -> dict | None:
    grupo = _grupo_segmentos_desde_nombre(validated_data, origen_real, ruta_origen)
    if not grupo:
        return None

    inspeccion = _inspeccionar_segmentos_grupo(grupo)
    return {
        "grupo_origen": inspeccion["grupo_origen"],
        "nombre_video": validated_data.get("nombre") or inspeccion["nombre_video"],
        "ruta_origen": inspeccion["ruta_origen"],
        "segmentos_origen": [segmento["ruta_rel"] for segmento in inspeccion["segmentos"]],
        "origen_sha256": inspeccion["origen_sha256"],
        "origen_tamano_bytes": inspeccion["origen_tamano_bytes"],
        "origen_modificado_en": inspeccion["origen_modificado_en"],
        "fecha_inicio": validated_data.get("fecha_inicio") or inspeccion["fecha_inicio"],
        "inicio_timestamp": (
            validated_data.get("inicio_timestamp") or inspeccion["inicio_timestamp"]
        ),
        "duracion_esperada": inspeccion["duracion_esperada"],
        "segmentos": inspeccion["segmentos"],
    }


def _construir_manifest_importacion(
    validated_data: dict,
    origen_real: str,
    ruta_origen: str,
) -> dict:
    return _construir_manifest_grupo(validated_data, origen_real, ruta_origen) or _construir_manifest_individual(
        validated_data,
        origen_real,
        ruta_origen,
    )


def copiar_archivo_a_storage(
    origen_real: str,
    *,
    carpeta_destino: str = "videos",
    nombre_destino: str | None = None,
    firma_esperada: tuple[int, int] | None = None,
) -> tuple[str, str]:
    nombre_archivo = get_valid_filename(nombre_destino or os.path.basename(origen_real))
    if not nombre_archivo:
        raise ValidationError("Nombre de archivo inválido.")

    if firma_esperada is not None and _firma_stat(_stat_archivo(origen_real)) != firma_esperada:
        raise ValidationError("El archivo cambió antes de copiarse; espere a que termine la subida.")

    destino_rel = default_storage.get_available_name(os.path.join(carpeta_destino, nombre_archivo))
    with open(origen_real, "rb") as archivo_origen:
        destino_rel = default_storage.save(destino_rel, File(archivo_origen))

    if firma_esperada is not None and _firma_stat(_stat_archivo(origen_real)) != firma_esperada:
        default_storage.delete(destino_rel)
        raise ValidationError("El archivo cambió mientras se copiaba; intente nuevamente.")
    return destino_rel, nombre_archivo


def eliminar_video_y_archivos(video: Video | None) -> None:
    if not video:
        return
    storage_name = ""
    if getattr(video, "ruta_archivo", None):
        storage_name = video.ruta_archivo.name or ""
    if storage_name:
        default_storage.delete(storage_name)
    if video.pk:
        video.delete()


def _buscar_video_existente(
    ruta_origen: str,
    validated_data: dict,
    *,
    grupo_origen: str = "",
    segmentos_origen: list[str] | None = None,
) -> Video | None:
    queryset = Video.objects.filter(
        id_turno=validated_data["id_turno"],
        camara=validated_data["camara"],
    ).order_by("-id")
    if grupo_origen:
        existente = queryset.filter(grupo_origen=grupo_origen).first()
        if existente:
            return existente
        if segmentos_origen:
            existente = queryset.filter(ruta_origen__in=segmentos_origen).first()
            if existente:
                return existente
    return queryset.filter(ruta_origen=ruta_origen).first()


def _actualizar_campos_video(video: Video, validated_data: dict, manifest: dict, destino_rel: str) -> None:
    video.nombre = manifest["nombre_video"]
    video.camara = validated_data["camara"]
    video.ruta_archivo.name = destino_rel
    video.ruta_origen = manifest["ruta_origen"]
    video.grupo_origen = manifest["grupo_origen"]
    video.segmentos_origen = manifest["segmentos_origen"]
    video.origen_sha256 = manifest["origen_sha256"]
    video.origen_tamano_bytes = manifest["origen_tamano_bytes"]
    video.origen_modificado_en = manifest["origen_modificado_en"]
    video.fecha_inicio = manifest["fecha_inicio"]
    video.fecha_subida = validated_data.get("fecha_subida") or timezone.localdate()
    video.inicio_timestamp = manifest["inicio_timestamp"]
    video.id_turno = validated_data["id_turno"]
    video.estado = EstadoVideo.PROCESANDO


def _guardar_metadata_video(video: Video) -> None:
    video.save(
        update_fields=[
            "nombre",
            "camara",
            "ruta_archivo",
            "ruta_origen",
            "grupo_origen",
            "segmentos_origen",
            "origen_sha256",
            "origen_tamano_bytes",
            "origen_modificado_en",
            "fecha_inicio",
            "fecha_subida",
            "inicio_timestamp",
            "id_turno",
            "estado",
        ]
    )


def crear_video_desde_serializer(serializer) -> Video:
    video = serializer.save()
    archivo = serializer.validated_data.get("ruta_archivo")
    try:
        procesar_video_subida(video, archivo)
    except Exception:
        eliminar_video_y_archivos(video)
        raise
    return video


def crear_video_desde_ruta_servidor(
    validated_data: dict,
    origen_real: str,
    *,
    ruta_origen: str,
) -> Video:
    manifest = _construir_manifest_importacion(validated_data, origen_real, ruta_origen)
    video_existente = _buscar_video_existente(
        manifest["ruta_origen"],
        validated_data,
        grupo_origen=manifest["grupo_origen"],
        segmentos_origen=manifest["segmentos_origen"],
    )
    if (
        video_existente
        and video_existente.origen_sha256 == manifest["origen_sha256"]
        and video_existente.estado == EstadoVideo.LISTO
        and video_existente.ruta_archivo.name
        and default_storage.exists(video_existente.ruta_archivo.name)
    ):
        return video_existente

    artifact_abs = manifest["artifact_abs"] if "artifact_abs" in manifest else None
    artifact_cleanup = list(manifest.get("artifact_cleanup", []))
    firma_copia = manifest.get("firma_copia")
    if artifact_abs is None:
        artifact_abs, cleanup_extra, nombre_destino = _materializar_artefacto_grupo(manifest)
        artifact_cleanup.extend(cleanup_extra)
        manifest["nombre_destino"] = nombre_destino
    try:
        destino_rel, _ = copiar_archivo_a_storage(
            artifact_abs,
            nombre_destino=manifest["nombre_destino"],
            firma_esperada=firma_copia,
        )
        if video_existente:
            ruta_anterior = video_existente.ruta_archivo.name or ""
            _actualizar_campos_video(video_existente, validated_data, manifest, destino_rel)
            try:
                procesar_video_subida(
                    video_existente,
                    video_existente.ruta_archivo,
                    duracion_esperada=manifest["duracion_esperada"],
                )
                _guardar_metadata_video(video_existente)
            except Exception:
                default_storage.delete(destino_rel)
                raise
            if ruta_anterior and ruta_anterior != video_existente.ruta_archivo.name:
                default_storage.delete(ruta_anterior)
            return video_existente

        video = Video.objects.create(
            nombre=manifest["nombre_video"],
            camara=validated_data["camara"],
            ruta_archivo=destino_rel,
            ruta_origen=manifest["ruta_origen"],
            grupo_origen=manifest["grupo_origen"],
            segmentos_origen=manifest["segmentos_origen"],
            origen_sha256=manifest["origen_sha256"],
            origen_tamano_bytes=manifest["origen_tamano_bytes"],
            origen_modificado_en=manifest["origen_modificado_en"],
            fecha_inicio=manifest["fecha_inicio"],
            fecha_subida=validated_data.get("fecha_subida") or timezone.localdate(),
            inicio_timestamp=manifest["inicio_timestamp"],
            id_turno=validated_data["id_turno"],
        )
        try:
            procesar_video_subida(
                video,
                video.ruta_archivo,
                duracion_esperada=manifest["duracion_esperada"],
            )
            _guardar_metadata_video(video)
        except Exception:
            eliminar_video_y_archivos(video)
            raise
        return video
    finally:
        for ruta in artifact_cleanup:
            remove_if_exists(ruta)
