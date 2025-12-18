from rest_framework import serializers
from .models import Camion, Turno, Video

class CamionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Camion
        fields = ['id', 'patente']

class TurnoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Turno
        fields = ['id', 'hora_inicio', 'hora_fin', 'id_camion']

class VideoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Video
        fields = ['id', 'nombre', 'camara', 'ruta_archivo', 'hora_inicio', 'duracion', 'id_turno']