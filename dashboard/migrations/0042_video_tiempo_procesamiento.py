from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0041_video_mapa_segmentos"),
    ]

    operations = [
        migrations.AddField(
            model_name="video",
            name="procesamiento_finalizado_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="video",
            name="procesamiento_iniciado_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="video",
            name="tiempo_procesamiento_segundos",
            field=models.FloatField(blank=True, null=True),
        ),
    ]
