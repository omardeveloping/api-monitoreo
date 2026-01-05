from rest_framework import serializers
from .models import Camion, Turno, Video, EstadoVideo, Operador, Incidente, AsignacionTurno

class CamionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Camion
        fields = ['id', 'patente']

class TurnoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Turno
        fields = ['id', 'fecha', 'hora_inicio', 'hora_fin', 'id_camion', 'operador', 'tipo_turno', 'activo', 'completado']
        read_only_fields = ['hora_inicio', 'hora_fin', 'completado']

class VideoSerializer(serializers.ModelSerializer):
    def to_representation(self, instance):
        data = super().to_representation(instance)
        if instance.estado != EstadoVideo.LISTO:
            data["duracion"] = None
            data["ruta_archivo"] = None
            data["fin_timestamp"] = None
            data["mimetype"] = None
        return data

    class Meta:
        model = Video
        fields = [
            'id',
            'nombre',
            'camara',
            'ruta_archivo',
            'fecha_subida',
            'hora_inicio',
            'duracion',
            'inicio_timestamp',
            'fin_timestamp',
            'mimetype',
            'estado',
            'id_turno',
        ]


class AsignacionTurnoSerializer(serializers.ModelSerializer):
    class Meta:
        model = AsignacionTurno
        fields = ['id', 'semana', 'turno', 'operador']


class OperadorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Operador
        fields = [
            'id',
            'nombre',
            'apellido',
            'licencia',
            'certificaciones',
            'correo',
            'telefono',
        ]


class IncidenteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Incidente
        fields = [
            'id',
            'tipo_incidente',
            'severidad',
            'tiempo_en_video',
            'descripcion',
            'turno',
        ]
