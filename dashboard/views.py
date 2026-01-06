import shutil
from datetime import datetime, timedelta
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from django.conf import settings
from django.utils import timezone
from .models import Camion, Turno, Video, Operador, Incidente, AsignacionTurno, Mantenimiento
from .serializers import (
    CamionSerializer,
    TurnoSerializer,
    VideoSerializer,
    OperadorSerializer,
    IncidenteSerializer,
    AsignacionTurnoSerializer,
    MantenimientoSerializer,
)
from dashboard.services.calcular_duracion_video import (
    procesar_video_subida,
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
        if page is not None:
            serializer = VideoSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = VideoSerializer(videos, many=True)
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
