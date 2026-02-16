from datetime import time
from django.db import models
from django.utils import timezone

# Create your models here.
class Camion(models.Model):
    patente = models.CharField(max_length=10, unique=True)
    marca = models.CharField(max_length=100, blank=True, default="")
    ano = models.PositiveIntegerField(null=True, blank=True)
    disponible = models.BooleanField(default=True)
    carpeta_id = models.CharField(max_length=64, blank=True, default="", db_index=True)

    def __str__(self):
        return self.patente


class TipoTurnoChoices(models.TextChoices):
    MANANA = "manana", "Día"
    TARDE = "tarde", "Tarde"
    NOCHE = "noche", "Noche"

class Turno(models.Model):
    HORARIO_TIPO_TURNO = {
        TipoTurnoChoices.NOCHE: (time(0, 0), time(8, 0)),
        TipoTurnoChoices.MANANA: (time(8, 0), time(16, 0)),
        TipoTurnoChoices.TARDE: (time(16, 0), time(0, 0)),
    }

    fecha = models.DateField(default=timezone.localdate)
    hora_inicio = models.TimeField()
    hora_fin = models.TimeField()
    id_camion = models.ForeignKey(Camion, on_delete=models.CASCADE)
    tipo_turno = models.CharField(max_length=10, choices=TipoTurnoChoices.choices, null=True, blank=True)
    activo = models.BooleanField(default=False)
    completado = models.BooleanField(default=False)

    def __str__(self):
        base = f"{self.fecha} {self.hora_inicio.strftime('%H:%M')} - {self.hora_fin.strftime('%H:%M')} ({self.id_camion.patente})"
        if self.tipo_turno:
            return f"{base} [{self.get_tipo_turno_display()}]"
        return base

    def save(self, *args, **kwargs):
        update_fields = set(kwargs.get("update_fields") or [])

        if self.tipo_turno:
            horario = self.HORARIO_TIPO_TURNO.get(self.tipo_turno)
            if horario:
                self.hora_inicio, self.hora_fin = horario
                update_fields.update({"hora_inicio", "hora_fin"})

        if kwargs.get("update_fields") is not None:
            kwargs["update_fields"] = list(update_fields)

        super().save(*args, **kwargs)


class NumeroCamara(models.IntegerChoices):
    CAMARA_1 = 1, "Cámara 1"
    CAMARA_2 = 2, "Cámara 2"
    CAMARA_3 = 3, "Cámara 3"
    CAMARA_4 = 4, "Cámara 4"


class EstadoVideo(models.TextChoices):
    PROCESANDO = "procesando", "Procesando"
    LISTO = "listo", "Listo"
    ERROR = "error", "Error"


class Video(models.Model):
    nombre = models.CharField(max_length=100)
    camara = models.IntegerField(choices=NumeroCamara.choices)
    ruta_archivo = models.FileField(upload_to='videos/')
    mimetype = models.CharField(max_length=100, blank=True, default="")
    fecha_inicio = models.DateTimeField(null=True, blank=True)
    duracion = models.IntegerField(null=True, blank=True)
    inicio_timestamp = models.TimeField(default=time(0, 0), null=True, blank=True)
    fin_timestamp = models.TimeField(null=True, blank=True)
    estado = models.CharField(
        max_length=20,
        choices=EstadoVideo.choices,
        default=EstadoVideo.PROCESANDO,
    )
    id_turno = models.ForeignKey(Turno, on_delete=models.CASCADE)
    creado_en = models.DateTimeField(auto_now_add=True)
    fecha_subida = models.DateField(default=timezone.localdate)

    def __str__(self):
        return self.nombre


class VelocidadVideo(models.Model):
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name="velocidades")
    segundo = models.PositiveIntegerField()
    velocidad_kmh = models.FloatField()
    timestamp_csv = models.DateTimeField(null=True, blank=True)
    interpolado = models.BooleanField(default=False)
    sin_datos = models.BooleanField(default=False)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("video", "segundo")
        indexes = [
            models.Index(fields=["video", "segundo"], name="velocidad_video_segundo_idx"),
        ]

    def __str__(self):
        return f"{self.video_id} @ {self.segundo}s: {self.velocidad_kmh} km/h"

class Incidente(models.Model):
    class TipoIncidente(models.TextChoices):
        FRENADO_BRUSCO = "frenado_brusco", "Frenado o Giro Brusco"
        EXCESO_VELOCIDAD = "exceso_velocidad", "Exceso de Velocidad"
        COLISION = "colision", "Impacto o Colisión"
        SOBRECARGA = "sobrecarga", "Sobrecarga"
        INGRESO_ZONA_RESTRINGIDA = "ingreso_zona_restringida", "Ingreso a Zona Restringida o Peatonal"
        FALLA_MECANICA_HIDRAULICA_ELECTRICA = (
            "falla_mecanica_hidraulica_electrica",
            "Falla Mecánica, Hidráulica o Eléctrica",
        )
        PERDIDA_GPS_COMUNICACION = "perdida_gps_comunicacion", "Pérdida de GPS o Comunicación"
        DISTRACCION = "distraccion", "Distracción"
        FATIGA_SUENO = "fatiga_sueno", "Fatiga / Sueño"
        OTRO = "otro", "Otro"

    class Severidad(models.TextChoices):
        BAJA = "baja", "Baja"
        MEDIA = "media", "Media"
        ALTA = "alta", "Alta"

    tipo_incidente = models.CharField(
        max_length=50,
        choices=TipoIncidente.choices,
    )
    severidad = models.CharField(
        max_length=20,
        choices=Severidad.choices,
    )
    tiempo_en_video = models.IntegerField(help_text="Segundos desde el inicio del video")
    descripcion = models.TextField(blank=True, default="")
    turno = models.ForeignKey(Turno, on_delete=models.CASCADE)

    def __str__(self):
        return f"{self.tipo_incidente} ({self.severidad})"


class EstadisticaVideoDiaria(models.Model):
    fecha = models.DateField(unique=True)
    cantidad_videos = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"{self.fecha}: {self.cantidad_videos}"


# Clases comentadas por ahora, serán necesarias más adelante.

# class Operador(models.Model):
#     class EstadoOperador(models.TextChoices):
#         ACTIVO = "activo", "Activo"
#         INACTIVO = "inactivo", "Inactivo"

#     nombre = models.CharField(max_length=100)
#     apellido = models.CharField(max_length=100)
#     licencia = models.CharField(max_length=50, blank=True, default="")
#     certificaciones = models.JSONField(default=list, blank=True)
#     correo = models.EmailField(unique=True)
#     telefono = models.CharField(max_length=30, blank=True, default="")
#     estado = models.CharField(
#         max_length=10,
#         choices=EstadoOperador.choices,
#         default=EstadoOperador.ACTIVO,
#     )

#     def __str__(self):
#         return f"{self.nombre} {self.apellido}"


# class Mantenimiento(models.Model):
#     camion = models.ForeignKey(Camion, on_delete=models.CASCADE, related_name="mantenimientos")
#     fecha = models.DateField(default=timezone.localdate)
#     descripcion = models.TextField(blank=True, default="")
#     costo = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)

#     def save(self, *args, **kwargs):
#         super().save(*args, **kwargs)
#         # Actualiza la referencia al último mantenimiento si corresponde.
#         camion = self.camion
#         if (
#             camion.ultimo_mantenimiento is None
#             or (camion.ultimo_mantenimiento.fecha <= self.fecha)
#         ):
#             camion.ultimo_mantenimiento = self
#             camion.save(update_fields=["ultimo_mantenimiento"])

#     def __str__(self):
#         return f"{self.camion} - {self.fecha}"
