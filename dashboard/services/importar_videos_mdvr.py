import datetime
import math
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass

from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from dashboard.models import Camion, TipoTurnoChoices, Turno, Video
from dashboard.services.calcular_duracion_video import (
    calcular_duracion_video,
    procesar_video_subida,
)
from dashboard.services.importar_velocidades_xlsx import importar_velocidades_xlsx


_DIR_FECHA_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ID_PREFIX_RE = re.compile(r"^(?P<id>\d+)")
_SEGMENTO_RE = re.compile(
    r"^(?P<equipo>\d+)-(?P<camara>\d{2})-(?P<inicio>\d{6})-(?P<fin>\d{6})-.*\.(?P<ext>h264|grec)$",
    re.IGNORECASE,
)
_SEGMENTO_GREC_NUEVO_RE = re.compile(
    r"^(?P<equipo>\d+)-(?P<fecha>\d{6})-(?P<inicio>\d{6})-(?P<fin>\d{6})-(?P<codigo>\d+)\.grec$",
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


@dataclass(frozen=True)
class SegmentoVideo:
    ruta: str
    camara: int
    inicio_dt: datetime.datetime
    fin_dt: datetime.datetime


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


def _segmento_desde_archivo(ruta: str, fecha: datetime.date) -> SegmentoVideo | None:
    nombre = os.path.basename(ruta)
    match = _SEGMENTO_RE.match(nombre)
    camara = None
    inicio_raw = None
    fin_raw = None

    if match:
        camara_raw = match.group("camara")
        try:
            camara = int(camara_raw)
        except ValueError:
            return None
        inicio_raw = match.group("inicio")
        fin_raw = match.group("fin")
    else:
        match_nuevo = _SEGMENTO_GREC_NUEVO_RE.match(nombre)
        if not match_nuevo:
            return None
        fecha_archivo = _parse_fecha_yymmdd(match_nuevo.group("fecha"))
        if not fecha_archivo or fecha_archivo != fecha:
            return None
        camara = _extraer_camara_desde_codigo_nuevo(match_nuevo.group("codigo"))
        if camara is None:
            return None
        inicio_raw = match_nuevo.group("inicio")
        fin_raw = match_nuevo.group("fin")

    if camara not in {1, 2, 3, 4}:
        return None
    inicio = _parse_hora_hhmmss(inicio_raw)
    fin = _parse_hora_hhmmss(fin_raw)
    if not inicio or not fin:
        return None
    inicio_dt = datetime.datetime.combine(fecha, inicio)
    fin_dt = datetime.datetime.combine(fecha, fin)
    if fin_dt <= inicio_dt:
        fin_dt += datetime.timedelta(days=1)
    return SegmentoVideo(ruta=ruta, camara=camara, inicio_dt=inicio_dt, fin_dt=fin_dt)


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


def _concat_h264_transcodificando(segmentos: list[str], salida: str) -> tuple[bool, str | None]:
    lista = _crear_lista_concat(segmentos)
    try:
        comando = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            lista,
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
            salida,
        ]
        subprocess.run(comando, capture_output=True, text=True, check=True)
        return True, None
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        return False, str(exc)
    finally:
        if os.path.exists(lista):
            os.remove(lista)


def _subir_archivo_temporal(ruta_local: str, nombre_base: str) -> str:
    destino = default_storage.get_available_name(os.path.join("videos", nombre_base))
    with open(ruta_local, "rb") as archivo:
        destino = default_storage.save(destino, File(archivo))
    return destino


def _recortar_video(video: Video, segundos: int) -> bool:
    if segundos <= 0:
        return False
    ruta = video.ruta_archivo.path
    base, ext = os.path.splitext(ruta)
    ruta_tmp = f"{base}.trim{ext}"
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                ruta,
                "-t",
                str(segundos),
                "-c",
                "copy",
                "-y",
                ruta_tmp,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        if os.path.exists(ruta_tmp):
            os.remove(ruta_tmp)
        return False

    os.replace(ruta_tmp, ruta)
    nueva_duracion = math.floor(calcular_duracion_video(ruta))
    video.duracion = nueva_duracion
    inicio = video.inicio_timestamp or datetime.time(0, 0)
    fin = (
        datetime.datetime.combine(datetime.date.today(), inicio)
        + datetime.timedelta(seconds=nueva_duracion)
    ).time()
    video.fin_timestamp = fin
    video.save(update_fields=["duracion", "fin_timestamp"])
    return True


def _alinear_duraciones(videos: list[Video]) -> int:
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
        "videos_omitidos": 0,
        "turnos_procesados": 0,
        "recortados": 0,
        "errores": [],
    }

    for nombre in sorted(os.listdir(carpeta_mdvr)):
        ruta_dia = os.path.join(carpeta_mdvr, nombre)
        if not os.path.isdir(ruta_dia) or not _DIR_FECHA_RE.match(nombre):
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
                continue
            segmento = _segmento_desde_archivo(ruta_archivo, fecha)
            if segmento:
                if os.path.getsize(ruta_archivo) <= 0:
                    continue
                segmentos.append(segmento)

        if not segmentos:
            continue

        grupos: dict[tuple[str, int], list[SegmentoVideo]] = {}
        for segmento in segmentos:
            tipo_turno = _tipo_turno_para_hora(segmento.inicio_dt.time())
            key = (tipo_turno, segmento.camara)
            grupos.setdefault(key, []).append(segmento)

        turnos_creados = {}
        videos_turno: dict[str, list[Video]] = {}

        for (tipo_turno, camara), lista in grupos.items():
            lista.sort(key=lambda s: s.inicio_dt)
            turno = turnos_creados.get(tipo_turno)
            if turno is None:
                turno = _obtener_o_crear_turno(
                    fecha=fecha,
                    camion=camion,
                    tipo_turno=tipo_turno,
                    errores=detalles["errores"],
                )
                turnos_creados[tipo_turno] = turno

            nombre_video = (
                f"MDVR_{carpeta_id}_{fecha.isoformat()}_{tipo_turno}_C{camara}"
            )
            if Video.objects.filter(nombre=nombre_video, id_turno=turno).exists():
                detalles["videos_omitidos"] += 1
                continue

            segmentos_paths = [seg.ruta for seg in lista]
            tmp_dir = tempfile.mkdtemp(prefix="mdvr_")
            try:
                ruta_salida = os.path.join(
                    tmp_dir, f"{nombre_video}.h264"
                )
                ok, error = _concat_h264(segmentos_paths, ruta_salida)
                if not ok:
                    ok, error = _concat_h264_transcodificando(segmentos_paths, ruta_salida)
                if not ok:
                    segmentos_validos = [p for p in segmentos_paths if _ffprobe_ok(p)]
                    if segmentos_validos and len(segmentos_validos) < len(segmentos_paths):
                        ok, error = _concat_h264_transcodificando(segmentos_validos, ruta_salida)
                    if not ok:
                        detalles["errores"].append(
                            f"{nombre_video}: no se pudo concatenar segmentos ({error})."
                        )
                        detalles["videos_omitidos"] += 1
                        continue

                destino_rel = _subir_archivo_temporal(
                    ruta_salida, f"{nombre_video}.h264"
                )

                inicio_dt = lista[0].inicio_dt
                video = Video.objects.create(
                    nombre=nombre_video,
                    camara=camara,
                    ruta_archivo=destino_rel,
                    fecha_inicio=inicio_dt,
                    fecha_subida=timezone.localdate(),
                    inicio_timestamp=inicio_dt.time(),
                    id_turno=turno,
                )

                procesar_video_subida(video, video.ruta_archivo)
                detalles["videos_creados"] += 1
                videos_turno.setdefault(tipo_turno, []).append(video)

                if importar_velocidades:
                    xlsx_info = _seleccionar_xlsx(xlsx_files, inicio_dt)
                    if xlsx_info:
                        try:
                            with open(xlsx_info.ruta, "rb") as archivo:
                                importar_velocidades_xlsx(video, archivo)
                        except Exception as exc:
                            detalles["errores"].append(
                                f"{nombre_video}: error importando velocidades ({exc})."
                            )
            except Exception as exc:
                detalles["errores"].append(
                    f"{nombre_video}: error procesando video ({exc})."
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

    return detalles
