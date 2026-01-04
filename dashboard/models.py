from datetime import time
from django.db import models
from django.utils import timezone

# Create your models here.
class Camion(models.Model):
    patente = models.CharField(max_length=10, unique=True)

    def __str__(self):
        return self.patente

class Turno(models.Model):
    fecha = models.DateField(default=timezone.localdate)
    hora_inicio = models.TimeField()
    hora_fin = models.TimeField()
    id_camion = models.ForeignKey(Camion, on_delete=models.CASCADE)
    operador = models.ForeignKey('Operador', on_delete=models.CASCADE, null=True, blank=True)
    tipo_turno = models.ForeignKey('TipoTurno', on_delete=models.SET_NULL, null=True, blank=True)
    activo = models.BooleanField(default=False)

    def __str__(self):
        base = f"{self.fecha} {self.hora_inicio.strftime('%H:%M')} - {self.hora_fin.strftime('%H:%M')} ({self.id_camion.patente})"
        if self.tipo_turno:
            return f"{base} [{self.tipo_turno.get_nombre_display()}]"
        return base
    
class NumeroCamara(models.IntegerChoices):
    CAMARA_1 = 1, "Cámara 1"
    CAMARA_2 = 2, "Cámara 2"
    CAMARA_3 = 3, "Cámara 3"
    CAMARA_4 = 4, "Cámara 4"


class NombreTurno(models.TextChoices):
    MANANA = "manana", "Mañana"
    TARDE = "tarde", "Tarde"
    NOCHE = "noche", "Noche"
    VARIABLE = "variable", "Variable"


class EstadoVideo(models.TextChoices):
    PROCESANDO = "procesando", "Procesando"
    LISTO = "listo", "Listo"
    ERROR = "error", "Error"


class Video(models.Model):
    nombre = models.CharField(max_length=100)
    camara = models.IntegerField(choices=NumeroCamara.choices)
    ruta_archivo = models.FileField(upload_to='videos/')
    mimetype = models.CharField(max_length=100, blank=True, default="")
    hora_inicio = models.TimeField(null=True, blank=True)
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


class Operador(models.Model):
    nombre = models.CharField(max_length=100)
    apellido = models.CharField(max_length=100)
    licencia = models.CharField(max_length=50, blank=True, default="")
    certificaciones = models.JSONField(default=list, blank=True)
    correo = models.EmailField(unique=True)
    telefono = models.CharField(max_length=30, blank=True, default="")

    def __str__(self):
        return f"{self.nombre} {self.apellido}"


class Incidente(models.Model):
    class TipoIncidente(models.TextChoices):
        EXCESO_VELOCIDAD = "exceso_velocidad", "Exceso de velocidad"
        FRENADO_BRUSCO = "frenado_brusco", "Frenado brusco"
        COLISION = "colision", "Colisión"
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


class TipoTurno(models.Model):
    nombre = models.CharField(max_length=20, choices=NombreTurno.choices, unique=True)
    hora_inicio = models.TimeField()
    hora_fin = models.TimeField()

    def __str__(self):
        return f"{self.get_nombre_display()} {self.hora_inicio.strftime('%H:%M')} - {self.hora_fin.strftime('%H:%M')}"


class EstadisticaVideoDiaria(models.Model):
    fecha = models.DateField(unique=True)
    cantidad_videos = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"{self.fecha}: {self.cantidad_videos}"
