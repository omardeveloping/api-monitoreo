from rest_framework import serializers
from .models import (
    Camion,
    Turno,
    Video,
    EstadoVideo,
    Operador,
    Incidente,
    AsignacionTurno,
    Mantenimiento,
    VelocidadVideo,
)

class CamionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Camion
        fields = ['id', 'patente', 'marca', 'ano', 'disponible', 'ultimo_mantenimiento']

class TurnoSerializer(serializers.ModelSerializer):
    def validate(self, attrs):
        tipo_turno = attrs.get("tipo_turno")
        if self.instance is not None and tipo_turno is None:
            tipo_turno = self.instance.tipo_turno
        if tipo_turno:
            return attrs

        hora_inicio = attrs.get("hora_inicio")
        hora_fin = attrs.get("hora_fin")
        if self.instance is not None:
            if hora_inicio is None:
                hora_inicio = self.instance.hora_inicio
            if hora_fin is None:
                hora_fin = self.instance.hora_fin

        if not hora_inicio or not hora_fin:
            raise serializers.ValidationError(
                "Debe indicar tipo_turno o ambas horas (hora_inicio y hora_fin)."
            )
        return attrs

    class Meta:
        model = Turno
        fields = ['id', 'fecha', 'hora_inicio', 'hora_fin', 'id_camion', 'operador', 'tipo_turno', 'activo', 'completado']
        read_only_fields = ['completado']

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
            'fecha_inicio',
            'duracion',
            'inicio_timestamp',
            'fin_timestamp',
            'mimetype',
            'estado',
            'id_turno',
        ]


class VelocidadVideoSerializer(serializers.ModelSerializer):
    class Meta:
        model = VelocidadVideo
        fields = [
            "id",
            "video",
            "segundo",
            "velocidad_kmh",
            "timestamp_csv",
            "interpolado",
            "sin_datos",
        ]
        read_only_fields = ["id", "video"]


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
            'estado',
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


class MantenimientoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Mantenimiento
        fields = ['id', 'camion', 'fecha', 'descripcion', 'costo']
