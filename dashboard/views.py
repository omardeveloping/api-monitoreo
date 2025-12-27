from rest_framework import viewsets, status
from rest_framework.response import Response
from .models import Camion, Turno, Video, Operador, Incidente
from .serializers import (
    CamionSerializer,
    TurnoSerializer,
    VideoSerializer,
    OperadorSerializer,
    IncidenteSerializer,
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


class OperadorViewSet(viewsets.ModelViewSet):
    queryset = Operador.objects.all()
    serializer_class = OperadorSerializer


class IncidenteViewSet(viewsets.ModelViewSet):
    queryset = Incidente.objects.all()
    serializer_class = IncidenteSerializer
