from rest_framework import viewsets, status
from rest_framework.response import Response
from .models import Camion, Turno, Video
from .serializers import CamionSerializer, TurnoSerializer, VideoSerializer
from dashboard.services.calcular_duracion_video import calcular_duracion_video, validar_formato


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
        archivo = serializer.validated_data.get("ruta_archivo")
        validar_formato(archivo)
        video = serializer.save()
        video.duracion = calcular_duracion_video(video.ruta_archivo.path)
        video.save(update_fields=["duracion"])

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)
