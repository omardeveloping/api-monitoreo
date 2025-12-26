from rest_framework import serializers
from .models import Camion, Turno, Video, EstadoVideo

class CamionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Camion
        fields = ['id', 'patente']

class TurnoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Turno
        fields = ['id', 'hora_inicio', 'hora_fin', 'id_camion']

class VideoSerializer(serializers.ModelSerializer):
    def to_representation(self, instance):
        data = super().to_representation(instance)
        if instance.estado != EstadoVideo.LISTO:
            data["duracion"] = None
            data["ruta_archivo"] = None
        return data

    class Meta:
        model = Video
        fields = [
            'id',
            'nombre',
            'camara',
            'ruta_archivo',
            'hora_inicio',
            'duracion',
            'inicio_timestamp',
            'fin_timestamp',
            'estado',
            'id_turno',
        ]
