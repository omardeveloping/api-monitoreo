import os
import re
import shutil
from celery import current_app
from celery.result import AsyncResult
from datetime import datetime, timedelta
from rest_framework import viewsets, status
from rest_framework.exceptions import ValidationError
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.permissions import DjangoModelPermissions, IsAuthenticated
from rest_framework.response import Response
from rest_framework.decorators import action
from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from django.utils import timezone
from django.utils.text import get_valid_filename
from .models import (
    Camion,
    EstadoVelocidadesVideo,
    Turno,
    Video,
    Incidente,
    VelocidadTurno,
)
from .serializers import (
    CamionSerializer,
    TurnoSerializer,
    VideoSerializer,
    VideoImportSerializer,
    VelocidadTurnoSerializer,
    IncidenteSerializer,
)
from dashboard.services.calcular_duracion_video import (
    procesar_video_subida,
)
from dashboard.services.importar_velocidades_csv import importar_velocidades_csv
from dashboard.services.importar_velocidades_xlsx import importar_velocidades_xlsx
from dashboard.services.monitor_mdvr_state import (
    guardar_task_id_monitor_mdvr,
    limpiar_task_id_monitor_mdvr,
    obtener_task_id_monitor_mdvr,
)
from dashboard.services.preview_video import obtener_preview_video
from dashboard.services.video_importacion import (
    crear_video_desde_serializer,
    crear_video_pendiente_desde_ruta_servidor,
    listar_archivos_servidor,
    obtener_base_importacion,
    resolver_ruta_importacion,
)
from dashboard.tasks import (
    CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID,
    cmsv6_monitor_mdvr_semanal_task,
    importar_video_desde_servidor_task,
    importar_videos_mdvr_task,
)

_PATRON_NOMBRE_VIDEO = re.compile(
    r"^(?P<equipo>\d+)-(?P<fecha>\d{6})-(?P<inicio>\d{6})-(?P<fin>\d{6})-(?P<codigo>\d+)$"
)

_FS_IGNORADOS_MONTAJES = {
    "autofs",
    "binfmt_misc",
    "bpf",
    "cgroup",
    "cgroup2",
    "configfs",
    "debugfs",
    "devpts",
    "devtmpfs",
    "efivarfs",
    "fusectl",
    "hugetlbfs",
    "mqueue",
    "nsfs",
    "overlay",
    "proc",
    "pstore",
    "ramfs",
    "rpc_pipefs",
    "securityfs",
    "squashfs",
    "sysfs",
    "tmpfs",
    "tracefs",
}


def _desescapar_mount(valor: str) -> str:
    return (
        (valor or "")
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _normalizar_rutas_unicas(rutas):
    vistas = set()
    resultado = []
    for ruta in rutas:
        if not ruta:
            continue
        ruta_real = os.path.realpath(ruta.strip())
        if not ruta_real or ruta_real in vistas:
            continue
        vistas.add(ruta_real)
        resultado.append(ruta_real)
    return resultado


def _listar_montajes_disponibles():
    montajes = []
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as archivo:
            for linea in archivo:
                partes = linea.split()
                if len(partes) < 3:
                    continue
                dispositivo = _desescapar_mount(partes[0])
                punto_montaje = _desescapar_mount(partes[1])
                fs_tipo = (partes[2] or "").lower()

                if fs_tipo in _FS_IGNORADOS_MONTAJES:
                    continue
                if dispositivo.startswith("/dev/loop"):
                    continue
                if not punto_montaje.startswith("/"):
                    continue
                if not os.path.isdir(punto_montaje):
                    continue

                montajes.append(punto_montaje)
    except OSError:
        return []
    return _normalizar_rutas_unicas(montajes)


def _formatear_nombre_archivo(nombre_archivo: str) -> str:
    base, _ext = os.path.splitext(nombre_archivo or "")
    match = _PATRON_NOMBRE_VIDEO.match(base)
    if not match:
        return nombre_archivo

    fecha = match.group("fecha")
    try:
        dia = int(fecha[0:2])
        mes = int(fecha[2:4])
        ano = 2000 + int(fecha[4:6])
        datetime(ano, mes, dia)
    except (ValueError, TypeError):
        return nombre_archivo

    def _formatear_hora(valor: str) -> str | None:
        try:
            hh = int(valor[0:2])
            mm = int(valor[2:4])
            ss = int(valor[4:6])
        except (ValueError, TypeError):
            return None
        if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
            return None
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    inicio = _formatear_hora(match.group("inicio"))
    fin = _formatear_hora(match.group("fin"))
    if not inicio or not fin:
        return nombre_archivo

    fecha_formateada = f"{ano:04d}-{mes:02d}-{dia:02d}"
    return (
        f"{match.group('equipo')} | {fecha_formateada} | {inicio}-{fin} | "
        f"{match.group('codigo')}"
    )


def _valor_request(request, nombre: str, default=None):
    if request.method in {"POST", "PUT", "PATCH"} and hasattr(request, "data"):
        if nombre in request.data:
            return request.data.get(nombre)
    return request.query_params.get(nombre, default)


def _bool_request(request, nombre: str, default=False) -> bool:
    valor = _valor_request(request, nombre, None)
    if valor is None:
        return default
    return str(valor).lower() in {"1", "true", "yes", "on"}


def _int_request(request, nombre: str, default: int, *, minimum: int = 0) -> int:
    valor = _valor_request(request, nombre, None)
    if valor in (None, ""):
        return default
    try:
        return max(minimum, int(valor))
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"Parametro '{nombre}' invalido.") from exc


def _float_request(request, nombre: str, default: float, *, minimum: float = 0.0) -> float:
    valor = _valor_request(request, nombre, None)
    if valor in (None, ""):
        return default
    try:
        return max(minimum, float(valor))
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"Parametro '{nombre}' invalido.") from exc


def _task_payload(task_id: str):
    task = AsyncResult(task_id)
    payload = {"task_id": task_id, "status": task.status}
    info = task.info
    if isinstance(info, dict):
        payload["info"] = info
    elif info:
        payload["info"] = {"message": str(info)}

    if task.ready():
        if task.successful():
            payload["resultado"] = task.result
        else:
            payload["error"] = str(task.result)
    return payload


def _normalizar_tarea_inspect(worker: str, origen: str, tarea: dict):
    request_info = tarea.get("request") if isinstance(tarea, dict) else None
    data = request_info if isinstance(request_info, dict) else tarea
    if not isinstance(data, dict):
        return None
    return {
        "worker": worker,
        "origen": origen,
        "id": data.get("id"),
        "name": data.get("name") or data.get("type"),
    }


def _buscar_monitor_mdvr_activo():
    task_name = "dashboard.tasks.cmsv6_monitor_mdvr_semanal_task"
    try:
        inspector = current_app.control.inspect(timeout=1.0)
        for origen, metodo in (
            ("active", inspector.active),
            ("reserved", inspector.reserved),
            ("scheduled", inspector.scheduled),
        ):
            data = metodo() or {}
            for worker, tareas in data.items():
                for tarea in tareas or []:
                    normalizada = _normalizar_tarea_inspect(worker, origen, tarea)
                    if not normalizada:
                        continue
                    if normalizada["id"] == CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID:
                        return normalizada
                    if normalizada["name"] == task_name:
                        return normalizada
    except Exception:
        return None
    return None


def _monitor_info_reciente(status: str, info: dict) -> bool:
    if status not in {"STARTED", "PROGRESS", "RETRY"}:
        return False
    actualizado_raw = info.get("actualizado_en") if isinstance(info, dict) else None
    if not actualizado_raw:
        return False
    try:
        actualizado = datetime.fromisoformat(str(actualizado_raw))
        if timezone.is_naive(actualizado):
            actualizado = timezone.make_aware(actualizado, timezone.get_current_timezone())
    except (TypeError, ValueError):
        return False
    try:
        stale_minutos = max(
            1,
            int(getattr(settings, "CMSV6_MONITOR_SEMANAL_STALE_MINUTES", 120)),
        )
    except (TypeError, ValueError):
        stale_minutos = 120
    return actualizado >= timezone.now() - timedelta(minutes=stale_minutos)


def _estado_monitor_mdvr():
    activo = _buscar_monitor_mdvr_activo()
    task_id_guardado = obtener_task_id_monitor_mdvr()
    task_id = (
        (activo or {}).get("id")
        or task_id_guardado
        or CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID
    )
    payload = _task_payload(task_id)
    payload["monitor_id"] = CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID
    info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
    payload["running"] = (
        activo is not None
        or bool(task_id_guardado and payload["status"] == "PENDING")
        or _monitor_info_reciente(
        payload["status"],
        info,
        )
    )
    if activo:
        payload["worker_task"] = activo
    return payload


def _params_monitor_mdvr(request):
    try:
        intervalo_default = max(
            1,
            int(getattr(settings, "CMSV6_MONITOR_SEMANAL_INTERVAL_MINUTES", 60)),
        )
    except (TypeError, ValueError):
        intervalo_default = 60
    params = {
        "output_dir": (_valor_request(request, "output_dir", "") or "").strip(),
        "intervalo_minutos": _int_request(
            request,
            "intervalo_minutos",
            intervalo_default,
            minimum=1,
        ),
        "incluir_futuro": _bool_request(request, "incluir_futuro", False),
        "excel_alarmas": _bool_request(request, "excel_alarmas", False),
        "testing": _bool_request(request, "testing", False),
        "test_order": _valor_request(request, "test_order", "size_asc") or "size_asc",
        "test_limit": _int_request(request, "test_limit", 1, minimum=1),
        "test_max_mb": _float_request(request, "test_max_mb", 0, minimum=0),
        "test_channel": _valor_request(request, "test_channel", "Todos") or "Todos",
        "test_30d_mode": _valor_request(request, "test_30d_mode", "off") or "off",
        "test_range_mode": _valor_request(request, "test_range_mode", "off") or "off",
        "max_logs": _int_request(request, "max_logs", 500, minimum=50),
    }
    ciclos = _valor_request(request, "ciclos", None)
    if ciclos not in (None, ""):
        params["ciclos"] = _int_request(request, "ciclos", 0, minimum=0)
    return params


class CamionViewSet(viewsets.ModelViewSet):
    queryset = Camion.objects.all()
    serializer_class = CamionSerializer

class TurnoViewSet(viewsets.ModelViewSet):
    queryset = Turno.objects.all()
    serializer_class = TurnoSerializer

    @action(detail=False, methods=["get"], url_path="estadisticas")
    def estadisticas(self, request):
        """Devuelve turnos activos."""
        activos = Turno.objects.filter(activo=True).count()
        return Response({"activos": activos})

    @action(detail=True, methods=["get"], url_path="videos-por-turno")
    def videos_por_turno(self, request, pk=None):
        """Devuelve la cantidad de videos asociados a un turno."""
        turno = self.get_object()
        total_videos = Video.objects.filter(id_turno=turno).count()
        return Response(
            {
                "turno_id": turno.id,
                "fecha": turno.fecha,
                "tipo_turno": turno.tipo_turno,
                "camion_id": turno.id_camion_id,
                "total_videos": total_videos,
            }
        )

    @action(detail=True, methods=["get"], url_path="videos")
    def videos(self, request, pk=None):
        """Devuelve todos los videos asociados a un turno."""
        turno = self.get_object()
        videos = Video.objects.filter(id_turno=turno).order_by("id")
        page = self.paginate_queryset(videos)
        serializer_context = {
            "request": request,
            "compat_playable_incomplete": True,
        }
        if page is not None:
            serializer = VideoSerializer(page, many=True, context=serializer_context)
            return self.get_paginated_response(serializer.data)
        serializer = VideoSerializer(videos, many=True, context=serializer_context)
        return Response(serializer.data)

    @action(detail=True, methods=["get"], url_path="velocidades")
    def velocidades(self, request, pk=None):
        """Devuelve velocidades del turno."""
        turno = self.get_object()
        queryset = VelocidadTurno.objects.filter(turno=turno).order_by("segundo")

        desde = request.query_params.get("desde")
        if desde is not None:
            try:
                desde = int(desde)
            except ValueError as exc:
                raise ValidationError("Parametro 'desde' invalido.") from exc
            queryset = queryset.filter(segundo__gte=desde)

        hasta = request.query_params.get("hasta")
        if hasta is not None:
            try:
                hasta = int(hasta)
            except ValueError as exc:
                raise ValidationError("Parametro 'hasta' invalido.") from exc
            queryset = queryset.filter(segundo__lte=hasta)

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = VelocidadTurnoSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = VelocidadTurnoSerializer(queryset, many=True)
        return Response(serializer.data)

    @action(detail=False, methods=["get"], url_path="por-dia")
    def por_dia(self, request):
        """
        Devuelve turnos por día.
        - Si se pasa 'fecha' (YYYY-MM-DD): devuelve los turnos de ese día.
        - Si se pasan 'desde'/'hasta': devuelve turnos agrupados por fecha.
        - Si no se pasa nada: usa la fecha de hoy.
        """
        fecha_param = (request.query_params.get("fecha") or "").strip()
        desde_param = (request.query_params.get("desde") or "").strip()
        hasta_param = (request.query_params.get("hasta") or "").strip()

        if fecha_param and (desde_param or hasta_param):
            raise ValidationError("Use 'fecha' o 'desde'/'hasta', no ambos.")

        def _parse_fecha(valor: str, nombre: str):
            try:
                return datetime.strptime(valor, "%Y-%m-%d").date()
            except ValueError as exc:
                raise ValidationError(
                    f"Parametro '{nombre}' invalido. Use formato YYYY-MM-DD."
                ) from exc

        if fecha_param:
            fecha = _parse_fecha(fecha_param, "fecha")
            queryset = Turno.objects.filter(fecha=fecha).order_by("hora_inicio", "id")
            serializer = TurnoSerializer(queryset, many=True)
            return Response(
                {
                    "fecha": fecha,
                    "count": queryset.count(),
                    "resultados": serializer.data,
                }
            )

        if not desde_param and not hasta_param:
            fecha = timezone.localdate()
            queryset = Turno.objects.filter(fecha=fecha).order_by("hora_inicio", "id")
            serializer = TurnoSerializer(queryset, many=True)
            return Response(
                {
                    "fecha": fecha,
                    "count": queryset.count(),
                    "resultados": serializer.data,
                }
            )

        desde = _parse_fecha(desde_param, "desde") if desde_param else None
        hasta = _parse_fecha(hasta_param, "hasta") if hasta_param else None

        if desde and hasta and desde > hasta:
            raise ValidationError("Parametro 'desde' no puede ser mayor que 'hasta'.")

        queryset = Turno.objects.all()
        if desde:
            queryset = queryset.filter(fecha__gte=desde)
        if hasta:
            queryset = queryset.filter(fecha__lte=hasta)
        queryset = queryset.order_by("fecha", "hora_inicio", "id")

        serializer = TurnoSerializer(queryset, many=True)
        agrupados = {}
        for item in serializer.data:
            agrupados.setdefault(item["fecha"], []).append(item)

        return Response(
            {
                "desde": desde,
                "hasta": hasta,
                "total": len(serializer.data),
                "resultados": agrupados,
            }
        )


class MonitoreoMDVRSemanalViewSet(viewsets.ViewSet):
    permission_classes = [IsAuthenticated]

    def list(self, request):
        return Response(_estado_monitor_mdvr())

    def create(self, request):
        estado = _estado_monitor_mdvr()
        if estado.get("running"):
            estado["celery_status"] = estado.get("status")
            estado["status"] = "running"
            return Response(estado, status=status.HTTP_200_OK)

        params = _params_monitor_mdvr(request)
        task = cmsv6_monitor_mdvr_semanal_task.apply_async(
            args=[params],
        )
        guardar_task_id_monitor_mdvr(task.id)
        return Response(
            {
                "task_id": task.id,
                "monitor_id": CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID,
                "status": "queued",
                "running": True,
                "params": params,
            },
            status=status.HTTP_202_ACCEPTED,
        )

    @action(detail=False, methods=["delete"], url_path="detener")
    def detener(self, request):
        task_ids = {
            CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID,
            obtener_task_id_monitor_mdvr(),
        }
        activo = _buscar_monitor_mdvr_activo()
        if activo and activo.get("id"):
            task_ids.add(activo["id"])
        task_ids.discard("")

        for task_id in task_ids:
            current_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            AsyncResult(task_id).forget()
        limpiar_task_id_monitor_mdvr()
        return Response(
            {
                "task_id": CMSV6_MONITOR_MDVR_SEMANAL_TASK_ID,
                "revoked_task_ids": sorted(task_ids),
                "status": "revoked",
                "running": False,
                "state_cleared": True,
            }
        )

class VideoViewSet(viewsets.ModelViewSet):
    queryset = Video.objects.all()
    serializer_class = VideoSerializer
    permission_classes = [IsAuthenticated, DjangoModelPermissions]

    def perform_create(self, serializer):
        return crear_video_desde_serializer(serializer)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        video = self.perform_create(serializer)
        response_serializer = self.get_serializer(video)
        headers = self.get_success_headers(response_serializer.data)
        return Response(response_serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    @action(detail=False, methods=["post"], url_path="importar-desde-servidor")
    def importar_desde_servidor(self, request):
        serializer = VideoImportSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        base_dir_real = obtener_base_importacion()
        ruta_origen, origen_real = resolver_ruta_importacion(
            base_dir_real,
            serializer.validated_data["ruta_origen"],
        )
        video = crear_video_pendiente_desde_ruta_servidor(
            serializer.validated_data,
            origen_real,
            ruta_origen=ruta_origen,
        )
        task = importar_video_desde_servidor_task.delay(
            video.pk,
            ruta_origen,
            serializer.validated_data.get("duracion_esperada_segundos"),
        )
        response_serializer = VideoSerializer(video, context={"request": request})
        data = dict(response_serializer.data)
        data["task_id"] = task.id
        data["encolado"] = True
        return Response(data, status=status.HTTP_202_ACCEPTED)

    @action(detail=False, methods=["get"], url_path="archivos-servidor")
    def archivos_servidor(self, request):
        base_dir_real = obtener_base_importacion()

        include_all = request.query_params.get("todo", "").lower() in {"1", "true", "yes"}
        exts_param = (request.query_params.get("extensiones") or "").strip()
        if exts_param:
            extensiones = {
                ext.strip().lower()
                for ext in exts_param.split(",")
                if ext.strip()
            }
        else:
            extensiones = {".mp4", ".h264", ".grec"}

        try:
            limit = int(request.query_params.get("limit", 500))
            offset = int(request.query_params.get("offset", 0))
        except ValueError as exc:
            raise ValidationError("Los parámetros 'limit' y 'offset' deben ser enteros.") from exc

        if limit < 0 or offset < 0:
            raise ValidationError("Los parámetros 'limit' y 'offset' deben ser >= 0.")

        limit = min(limit, 5000)
        return Response(
            listar_archivos_servidor(
                base_dir_real,
                include_all=include_all,
                extensiones=extensiones,
                limit=limit,
                offset=offset,
            )
        )

    @action(detail=False, methods=["get"], url_path="preview-servidor")
    def preview_servidor(self, request):
        base_dir_real = obtener_base_importacion()
        ruta_origen, origen_real = resolver_ruta_importacion(
            base_dir_real,
            request.query_params.get("ruta_origen"),
        )

        preview_rel, cached = obtener_preview_video(origen_real, ruta_origen)

        media_url = settings.MEDIA_URL or "/media/"
        if not media_url.endswith("/"):
            media_url = f"{media_url}/"
        preview_url = request.build_absolute_uri(f"{media_url}{preview_rel}")

        return Response(
            {
                "ruta_origen": ruta_origen.replace(os.sep, "/"),
                "preview_rel": preview_rel,
                "preview_url": preview_url,
                "cached": cached,
                "duracion_segundos": 5,
            }
        )

    @action(
        detail=True,
        methods=["post"],
        url_path="velocidades-csv",
        parser_classes=[MultiPartParser, FormParser],
    )
    def cargar_velocidades_csv(self, request, pk=None):
        video = self.get_object()
        archivo = request.FILES.get("archivo") or request.FILES.get("csv")
        if not archivo:
            raise ValidationError("Debe adjuntar un archivo CSV o XLSX en 'archivo'.")
        try:
            ext = os.path.splitext(getattr(archivo, "name", "") or "")[1].lower()
            if ext in {".xlsx", ".xls"}:
                resultado = importar_velocidades_xlsx(video, archivo)
            else:
                resultado = importar_velocidades_csv(video, archivo)
        except Exception as exc:
            Video.objects.filter(id_turno=video.id_turno).update(
                estado_velocidades=EstadoVelocidadesVideo.ERROR,
                velocidades_actualizadas_en=None,
                velocidades_error=(str(exc) or exc.__class__.__name__).strip()[:2000],
            )
            raise
        Video.objects.filter(id_turno=video.id_turno).update(
            estado_velocidades=EstadoVelocidadesVideo.IMPORTADA,
            velocidades_actualizadas_en=timezone.now(),
            velocidades_error="",
        )
        return Response(resultado, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["get"], url_path="velocidades")
    def velocidades(self, request, pk=None):
        video = self.get_object()
        queryset = VelocidadTurno.objects.filter(turno=video.id_turno).order_by("segundo")

        desde = request.query_params.get("desde")
        if desde is not None:
            try:
                desde = int(desde)
            except ValueError as exc:
                raise ValidationError("Parametro 'desde' invalido.") from exc
            queryset = queryset.filter(segundo__gte=desde)

        hasta = request.query_params.get("hasta")
        if hasta is not None:
            try:
                hasta = int(hasta)
            except ValueError as exc:
                raise ValidationError("Parametro 'hasta' invalido.") from exc
            queryset = queryset.filter(segundo__lte=hasta)

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = VelocidadTurnoSerializer(
                page, many=True, context={"video_id": video.id}
            )
            return self.get_paginated_response(serializer.data)
        serializer = VelocidadTurnoSerializer(
            queryset, many=True, context={"video_id": video.id}
        )
        return Response(serializer.data)

    @action(detail=False, methods=["get"], url_path="conteo-hoy")
    def conteo_hoy(self, request):
        """Devuelve la cantidad de videos del material de hoy y lo ingerido hoy."""
        hoy = timezone.localdate()
        cantidad_material = Video.objects.filter(fecha_subida=hoy).count()
        cantidad_ingestada = Video.objects.filter(creado_en__date=hoy).count()
        return Response(
            {
                "fecha": hoy,
                "cantidad_material_hoy": cantidad_material,
                "cantidad_ingestada_hoy": cantidad_ingestada,
            }
        )

    @action(detail=False, methods=["post"], url_path="importar-mdvr")
    def importar_mdvr(self, request):
        """Encola importación MDVR en Celery para no bloquear el API."""
        incluir_velocidades = request.query_params.get("velocidades", "1").lower() in {
            "1",
            "true",
            "yes",
        }
        fecha_param = (request.query_params.get("fecha") or "").strip()
        fecha_objetivo = None
        if fecha_param:
            try:
                datetime.strptime(fecha_param, "%Y-%m-%d")
            except ValueError as exc:
                raise ValidationError(
                    "Parametro 'fecha' invalido. Use formato YYYY-MM-DD."
                ) from exc
            fecha_objetivo = fecha_param

        task = importar_videos_mdvr_task.delay(
            importar_velocidades=incluir_velocidades,
            fecha_objetivo=fecha_objetivo,
        )
        return Response(
            {
                "task_id": task.id,
                "status": "queued",
                "importar_velocidades": incluir_velocidades,
                "fecha": fecha_objetivo,
            },
            status=status.HTTP_202_ACCEPTED,
        )

    @action(detail=False, methods=["get"], url_path="importar-mdvr-estado")
    def importar_mdvr_estado(self, request):
        task_id = (request.query_params.get("task_id") or "").strip()
        if not task_id:
            raise ValidationError("Debe indicar 'task_id'.")

        task = AsyncResult(task_id)
        response = {"task_id": task_id, "status": task.status}
        if task.ready():
            if task.successful():
                response["resultado"] = task.result
            else:
                response["error"] = str(task.result)
        return Response(response)


# class OperadorViewSet(viewsets.ModelViewSet):
#     queryset = Operador.objects.all()
#     serializer_class = OperadorSerializer

#     @action(detail=True, methods=["get"], url_path="estadisticas")
#     def estadisticas(self, request, pk=None):
#         """Devuelve total de turnos y horas trabajadas por un operador."""
#         operador = self.get_object()
#         turnos = Turno.objects.filter(operador=operador)

#         total_segundos = 0
#         for turno in turnos:
#             if turno.hora_inicio and turno.hora_fin:
#                 inicio = datetime.combine(timezone.localdate(), turno.hora_inicio)
#                 fin = datetime.combine(timezone.localdate(), turno.hora_fin)
#                 if fin <= inicio:
#                     fin += timedelta(days=1)  # Turnos que pasan medianoche
#                 total_segundos += (fin - inicio).total_seconds()

#         total_horas = round(total_segundos / 3600, 2)
#         return Response(
#             {
#                 "operador_id": operador.id,
#                 "total_turnos": turnos.count(),
#                 "total_horas": total_horas,
#                 "total_segundos": int(total_segundos),
#             }
#         )
# class MantenimientoViewSet(viewsets.ModelViewSet):
#     queryset = Mantenimiento.objects.all()
#     serializer_class = MantenimientoSerializer

class IncidenteViewSet(viewsets.ModelViewSet):
    queryset = Incidente.objects.all()
    serializer_class = IncidenteSerializer
    permission_classes = [IsAuthenticated, DjangoModelPermissions]

    def perform_create(self, serializer):
        incidente = serializer.save()
        velocidad_obj = (
            VelocidadTurno.objects.filter(
                turno=incidente.turno,
                segundo=incidente.tiempo_en_video,
            )
            .first()
        )
        if velocidad_obj is not None:
            incidente.velocidad_kmh = velocidad_obj.velocidad_kmh
            incidente.save(update_fields=["velocidad_kmh"])

    @action(detail=False, methods=["get"], url_path="contar-alta")
    def contar_alta(self, request):
        """Cuenta incidentes con severidad alta."""
        cantidad_incidentes = self.get_queryset().filter(
            severidad=Incidente.Severidad.ALTA
        ).count()
        return Response({"cantidad": cantidad_incidentes})

    @action(detail=False, methods=["get"], url_path="exportar")
    def exportar(self, request):
        """
        Devuelve incidentes en formato plano para exportación (Excel/CSV) desde frontend.
        """
        turno_map = {
            "manana": "Turno 1",
            "tarde": "Turno 2",
            "noche": "Turno 3",
        }

        incidentes = (
            self.get_queryset()
            .select_related("turno", "turno__id_camion")
            .order_by("id")
        )

        resultados = []
        for incidente in incidentes:
            turno = incidente.turno
            tipo_turno = (turno.tipo_turno or "").strip().lower()

            if tipo_turno in turno_map:
                turno_nombre = turno_map[tipo_turno]
            elif turno.hora_inicio is not None:
                if turno.hora_inicio.hour < 8:
                    turno_nombre = "Turno 3"
                elif turno.hora_inicio.hour < 16:
                    turno_nombre = "Turno 1"
                else:
                    turno_nombre = "Turno 2"
            else:
                turno_nombre = "Turno 1"

            segundos = int(incidente.tiempo_en_video or 0)
            if segundos < 0:
                segundos = 0
            minutos, rem_segundos = divmod(segundos, 60)
            fecha_hora = datetime.combine(turno.fecha, datetime.min.time()) + timedelta(
                seconds=segundos
            )

            velocidad_kmh = (
                None
                if incidente.velocidad_kmh is None
                else format(float(incidente.velocidad_kmh), "g")
            )

            resultados.append(
                {
                    "id": incidente.id,
                    "fecha_hora": fecha_hora.strftime("%Y-%m-%d %H:%M"),
                    "jornada_turno": turno.get_tipo_turno_display() if turno.tipo_turno else "",
                    "minuto_incidente": f"{minutos:02d}:{rem_segundos:02d}",
                    "tipo_incidente": incidente.get_tipo_incidente_display(),
                    "severidad": incidente.severidad,
                    "velocidad_kmh": velocidad_kmh,
                    "camionPatente": turno.id_camion.patente if turno.id_camion_id else None,
                    "turno": turno_nombre,
                    "descripcion": incidente.descripcion,
                }
            )

        return Response(resultados)


class EspacioDiscoViewSet(viewsets.ViewSet):
    """Devuelve el uso de disco del servidor."""

    def list(self, request):
        rutas_param = (request.query_params.get("rutas") or "").strip()
        rutas_env = (os.environ.get("ESPACIO_DISCO_RUTAS") or "").strip()
        auto_montajes = (
            request.query_params.get("auto_montajes", "1").strip().lower()
            in {"1", "true", "yes"}
        )
        rutas_detectadas = []
        if rutas_param:
            rutas = [ruta.strip() for ruta in rutas_param.split(",") if ruta.strip()]
        elif rutas_env:
            rutas = [ruta.strip() for ruta in rutas_env.split(",") if ruta.strip()]
        else:
            ruta = getattr(settings, "ESPACIO_DISCO_RUTA", "/")
            rutas = [ruta] if ruta else []
            if auto_montajes:
                rutas_detectadas = _listar_montajes_disponibles()
                rutas.extend(rutas_detectadas)

        rutas = _normalizar_rutas_unicas(rutas)

        if not rutas:
            raise ValidationError("No hay rutas configuradas para calcular espacio.")

        discos = []
        errores = []
        gb = 1024 ** 3
        dispositivos_vistos = set()
        total = usado = libre = 0

        for ruta in rutas:
            if not os.path.exists(ruta):
                errores.append({"ruta": ruta, "error": "La ruta no existe."})
                continue

            try:
                uso = shutil.disk_usage(ruta)
            except OSError as exc:
                errores.append({"ruta": ruta, "error": str(exc)})
                continue

            porcentaje_usado = round((uso.used / uso.total) * 100, 2) if uso.total else 0
            discos.append(
                {
                    "ruta": ruta,
                    "total_gb": round(uso.total / gb, 2),
                    "usado_gb": round(uso.used / gb, 2),
                    "libre_gb": round(uso.free / gb, 2),
                    "porcentaje_usado": porcentaje_usado,
                }
            )

            try:
                device_id = os.stat(ruta).st_dev
            except OSError:
                device_id = None

            if device_id in dispositivos_vistos:
                continue
            dispositivos_vistos.add(device_id)
            total += uso.total
            usado += uso.used
            libre += uso.free

        porcentaje_usado_total = round((usado / total) * 100, 2) if total else 0

        return Response(
            {
                "rutas": rutas,
                "rutas_detectadas_auto": rutas_detectadas,
                "count": len(discos),
                "discos": discos,
                "totales": {
                    "total_gb": round(total / gb, 2),
                    "usado_gb": round(usado / gb, 2),
                    "libre_gb": round(libre / gb, 2),
                    "porcentaje_usado": porcentaje_usado_total,
                },
                # Compatibilidad con el formato anterior (totales combinados).
                "ruta": ",".join(rutas),
                "total_gb": round(total / gb, 2),
                "usado_gb": round(usado / gb, 2),
                "libre_gb": round(libre / gb, 2),
                "porcentaje_usado": porcentaje_usado_total,
                "errores": errores,
            }
        )
