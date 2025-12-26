from django.db import models

# Create your models here.
class Camion(models.Model):
    patente = models.CharField(max_length=10, unique=True)

    def __str__(self):
        return self.patente

class Turno(models.Model):
    hora_inicio = models.TimeField()
    hora_fin = models.TimeField()
    id_camion = models.ForeignKey(Camion, on_delete=models.CASCADE)

    def __str__(self):
        return self.hora_inicio.strftime("%H:%M") + " - " + self.hora_fin.strftime("%H:%M") + " (" + self.id_camion.patente + ")"
    
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
    hora_inicio = models.TimeField()
    duracion = models.IntegerField(null=True, blank=True)
    inicio_timestamp = models.DateTimeField(null=True, blank=True)
    fin_timestamp = models.DateTimeField(null=True, blank=True)
    estado = models.CharField(
        max_length=20,
        choices=EstadoVideo.choices,
        default=EstadoVideo.PROCESANDO,
    )
    id_turno = models.ForeignKey(Turno, on_delete=models.CASCADE)

    def __str__(self):
        return self.nombre
