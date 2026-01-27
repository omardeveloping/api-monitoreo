import os
import re
import shutil
from datetime import datetime, timedelta
from rest_framework import viewsets, status
from rest_framework.exceptions import ValidationError
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.response import Response
from rest_framework.decorators import action
from django.conf import settings
from django.core.files import File
from django.core.files.storage import default_storage
from django.utils import timezone
from django.utils.text import get_valid_filename
from .models import Camion, Turno, Video, Operador, Incidente, AsignacionTurno, Mantenimiento
from .serializers import (
    CamionSerializer,
    TurnoSerializer,
    VideoSerializer,
    VideoImportSerializer,
    VelocidadVideoSerializer,
    OperadorSerializer,
    IncidenteSerializer,
    AsignacionTurnoSerializer,
    MantenimientoSerializer,
)
from dashboard.services.calcular_duracion_video import (
    procesar_video_subida,
)
from dashboard.services.importar_velocidades_csv import importar_velocidades_csv

_PATRON_NOMBRE_VIDEO = re.compile(
    r"^(?P<equipo>\d+)-(?P<fecha>\d{6})-(?P<inicio>\d{6})-(?P<fin>\d{6})-(?P<codigo>\d+)$"
)


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
        serializer_context = {"request": request}
        if page is not None:
            serializer = VideoSerializer(page, many=True, context=serializer_context)
            return self.get_paginated_response(serializer.data)
        serializer = VideoSerializer(videos, many=True, context=serializer_context)
        return Response(serializer.data)

class VideoViewSet(viewsets.ModelViewSet):
    queryset = Video.objects.all()
    serializer_class = VideoSerializer

    def perform_create(self, serializer):
        video = serializer.save()
        archivo = serializer.validated_data.get("ruta_archivo")
        procesar_video_subida(video, archivo)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    @action(detail=False, methods=["post"], url_path="importar-desde-servidor")
    def importar_desde_servidor(self, request):
        serializer = VideoImportSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        base_dir = getattr(settings, "VIDEOS_IMPORT_DIR", "")
        if not base_dir:
            raise ValidationError("VIDEOS_IMPORT_DIR no está configurado en el servidor.")

        base_dir_real = os.path.realpath(base_dir)
        if not os.path.isdir(base_dir_real):
            raise ValidationError("VIDEOS_IMPORT_DIR no apunta a un directorio válido.")

        ruta_origen = serializer.validated_data["ruta_origen"].strip()
        if not ruta_origen:
            raise ValidationError("Debe indicar 'ruta_origen'.")
        if os.path.isabs(ruta_origen):
            raise ValidationError("La ruta debe ser relativa al directorio configurado.")

        origen_real = os.path.realpath(os.path.join(base_dir_real, ruta_origen))
        if not (origen_real == base_dir_real or origen_real.startswith(base_dir_real + os.sep)):
            raise ValidationError("La ruta indicada sale del directorio permitido.")
        if not os.path.isfile(origen_real):
            raise ValidationError("El archivo indicado no existe.")

        nombre_archivo = get_valid_filename(os.path.basename(origen_real))
        if not nombre_archivo:
            raise ValidationError("Nombre de archivo inválido.")

        destino_rel = default_storage.get_available_name(os.path.join("videos", nombre_archivo))
        with open(origen_real, "rb") as archivo_origen:
            destino_rel = default_storage.save(destino_rel, File(archivo_origen))

        nombre = serializer.validated_data.get("nombre") or os.path.splitext(nombre_archivo)[0]
        video = Video.objects.create(
            nombre=nombre,
            camara=serializer.validated_data["camara"],
            ruta_archivo=destino_rel,
            fecha_inicio=serializer.validated_data.get("fecha_inicio"),
            fecha_subida=serializer.validated_data.get("fecha_subida") or timezone.localdate(),
            inicio_timestamp=serializer.validated_data.get("inicio_timestamp"),
            id_turno=serializer.validated_data["id_turno"],
        )

        procesar_video_subida(video, video.ruta_archivo)

        response_serializer = VideoSerializer(video, context={"request": request})
        return Response(response_serializer.data, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=["get"], url_path="archivos-servidor")
    def archivos_servidor(self, request):
        base_dir = getattr(settings, "VIDEOS_IMPORT_DIR", "")
        if not base_dir:
            raise ValidationError("VIDEOS_IMPORT_DIR no está configurado en el servidor.")

        base_dir_real = os.path.realpath(base_dir)
        if not os.path.isdir(base_dir_real):
            raise ValidationError("VIDEOS_IMPORT_DIR no apunta a un directorio válido.")

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

        archivos = []
        for raiz, _dirs, files in os.walk(base_dir_real):
            for nombre in files:
                ext = os.path.splitext(nombre)[1].lower()
                if not include_all and ext not in extensiones:
                    continue
                ruta_absoluta = os.path.join(raiz, nombre)
                ruta_relativa = os.path.relpath(ruta_absoluta, base_dir_real)
                try:
                    stat = os.stat(ruta_absoluta)
                except OSError:
                    continue
                archivos.append(
                    {
                        "ruta_origen": ruta_relativa.replace(os.sep, "/"),
                        "nombre_archivo": nombre,
                        "nombre_formateado": _formatear_nombre_archivo(nombre),
                        "tamano_bytes": stat.st_size,
                        "modificado_en": datetime.fromtimestamp(
                            stat.st_mtime, tz=timezone.get_current_timezone()
                        ).isoformat(),
                    }
                )

        archivos.sort(key=lambda item: item["ruta_origen"])
        total = len(archivos)
        resultados = archivos[offset : offset + limit] if limit else archivos[offset:]

        return Response(
            {
                "total": total,
                "count": len(resultados),
                "limit": limit,
                "offset": offset,
                "resultados": resultados,
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
            raise ValidationError("Debe adjuntar un archivo CSV en el campo 'archivo'.")
        resultado = importar_velocidades_csv(video, archivo)
        return Response(resultado, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["get"], url_path="velocidades")
    def velocidades(self, request, pk=None):
        video = self.get_object()
        queryset = video.velocidades.all().order_by("segundo")

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
            serializer = VelocidadVideoSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = VelocidadVideoSerializer(queryset, many=True)
        return Response(serializer.data)

    @action(detail=False, methods=["get"], url_path="conteo-hoy")
    def conteo_hoy(self, request):
        """Devuelve la cantidad de videos subidos hoy."""
        hoy = timezone.localdate()
        cantidad = Video.objects.filter(fecha_subida=hoy).count()
        return Response({"fecha": hoy, "cantidad": cantidad})


class OperadorViewSet(viewsets.ModelViewSet):
    queryset = Operador.objects.all()
    serializer_class = OperadorSerializer

    @action(detail=True, methods=["get"], url_path="estadisticas")
    def estadisticas(self, request, pk=None):
        """Devuelve total de turnos y horas trabajadas por un operador."""
        operador = self.get_object()
        turnos = Turno.objects.filter(operador=operador)

        total_segundos = 0
        for turno in turnos:
            if turno.hora_inicio and turno.hora_fin:
                inicio = datetime.combine(timezone.localdate(), turno.hora_inicio)
                fin = datetime.combine(timezone.localdate(), turno.hora_fin)
                if fin <= inicio:
                    fin += timedelta(days=1)  # Turnos que pasan medianoche
                total_segundos += (fin - inicio).total_seconds()

        total_horas = round(total_segundos / 3600, 2)
        return Response(
            {
                "operador_id": operador.id,
                "total_turnos": turnos.count(),
                "total_horas": total_horas,
                "total_segundos": int(total_segundos),
            }
        )


class AsignacionTurnoViewSet(viewsets.ModelViewSet):
    queryset = AsignacionTurno.objects.all()
    serializer_class = AsignacionTurnoSerializer


class MantenimientoViewSet(viewsets.ModelViewSet):
    queryset = Mantenimiento.objects.all()
    serializer_class = MantenimientoSerializer


class IncidenteViewSet(viewsets.ModelViewSet):
    queryset = Incidente.objects.all()
    serializer_class = IncidenteSerializer

    @action(detail=False, methods=["get"], url_path="contar-alta")
    def contar_alta(self, request):
        """Cuenta incidentes con severidad alta."""
        cantidad_incidentes = self.get_queryset().filter(
            severidad=Incidente.Severidad.ALTA
        ).count()
        return Response({"cantidad": cantidad_incidentes})


class EspacioDiscoViewSet(viewsets.ViewSet):
    """Devuelve el uso de disco del servidor."""

    def list(self, request):
        ruta = getattr(settings, "ESPACIO_DISCO_RUTA", "/")
        uso = shutil.disk_usage(ruta)
        total = uso.total
        usado = uso.used
        libre = uso.free
        porcentaje_usado = round((usado / total) * 100, 2) if total else 0
        gb = 1024 ** 3
        return Response(
            {
                "ruta": ruta,
                "total_gb": round(total / gb, 2),
                "usado_gb": round(usado / gb, 2),
                "libre_gb": round(libre / gb, 2),
                "porcentaje_usado": porcentaje_usado,
            }
        )
