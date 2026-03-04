from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from rest_framework import serializers
from django.utils import timezone
from .models import (
    Camion,
    Turno,
    Video,
    EstadoVideo,
    Incidente,
    VelocidadTurno,
    VelocidadVideo,
    NumeroCamara,
)


def _with_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query[key] = value
    return urlunparse(parsed._replace(query=urlencode(query)))


class CamionSerializer(serializers.ModelSerializer):
    def validate(self, attrs):
        if self.instance is None and Camion.objects.exists():
            raise serializers.ValidationError(
                "Solo puede existir un camión/maquinaria en el sistema."
            )
        return attrs

    class Meta:
        model = Camion
        fields = ['id', 'patente', 'marca', 'ano', 'disponible', 'carpeta_id']

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
        fields = ['id', 'fecha', 'hora_inicio', 'hora_fin', 'id_camion', 'tipo_turno', 'activo', 'completado']
        read_only_fields = ['completado']

class VideoSerializer(serializers.ModelSerializer):
    tiempo_procesamiento_segundos = serializers.SerializerMethodField()

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if instance.estado != EstadoVideo.LISTO:
            data["duracion"] = None
            data["ruta_archivo"] = None
            data["fin_timestamp"] = None
            data["mimetype"] = None
        else:
            ruta = data.get("ruta_archivo")
            if ruta:
                # Keep URL stable per object state while busting stale browser/CDN cache.
                token = f"{instance.id}-{instance.duracion or 0}-{instance.fin_timestamp or ''}"
                data["ruta_archivo"] = _with_query_param(ruta, "v", token)
        return data

    def get_tiempo_procesamiento_segundos(self, instance):
        inicio = instance.procesamiento_iniciado_en
        if inicio is None:
            return None

        fin = instance.procesamiento_finalizado_en
        if fin is None and instance.estado == EstadoVideo.PROCESANDO:
            fin = timezone.now()

        if fin is not None:
            return round(max((fin - inicio).total_seconds(), 0), 3)

        if instance.tiempo_procesamiento_segundos is None:
            return None
        return round(float(instance.tiempo_procesamiento_segundos), 3)

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
            'procesamiento_iniciado_en',
            'procesamiento_finalizado_en',
            'tiempo_procesamiento_segundos',
            'estado_velocidades',
            'velocidades_actualizadas_en',
            'velocidades_error',
            'reintentos',
            'ultimo_error',
            'proximo_reintento_en',
            'id_turno',
        ]
        read_only_fields = [
            'procesamiento_iniciado_en',
            'procesamiento_finalizado_en',
            'tiempo_procesamiento_segundos',
            'estado_velocidades',
            'velocidades_actualizadas_en',
            'velocidades_error',
            'reintentos',
            'ultimo_error',
            'proximo_reintento_en',
        ]


class VideoImportSerializer(serializers.Serializer):
    ruta_origen = serializers.CharField(max_length=500)
    nombre = serializers.CharField(max_length=100, required=False, allow_blank=True)
    camara = serializers.ChoiceField(choices=NumeroCamara.choices)
    id_turno = serializers.PrimaryKeyRelatedField(queryset=Turno.objects.all())
    fecha_inicio = serializers.DateTimeField(required=False, allow_null=True)
    fecha_subida = serializers.DateField(required=False, allow_null=True)
    inicio_timestamp = serializers.TimeField(required=False, allow_null=True)


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


class VelocidadTurnoSerializer(serializers.ModelSerializer):
    video = serializers.SerializerMethodField()

    class Meta:
        model = VelocidadTurno
        fields = [
            "id",
            "video",
            "turno",
            "segundo",
            "velocidad_kmh",
            "timestamp_csv",
            "interpolado",
            "sin_datos",
        ]
        read_only_fields = ["id", "video", "turno"]

    def get_video(self, _obj):
        return self.context.get("video_id")


# class OperadorSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Operador
#         fields = [
#             'id',
#             'nombre',
#             'apellido',
#             'licencia',
#             'certificaciones',
#             'correo',
#             'telefono',
#             'estado',
#         ]

# class MantenimientoSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Mantenimiento
#         fields = ['id', 'camion', 'fecha', 'descripcion', 'costo']


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
            'velocidad_kmh',
        ]
        read_only_fields = ['velocidad_kmh']
