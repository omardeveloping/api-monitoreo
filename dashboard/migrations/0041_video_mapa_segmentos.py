from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0040_velocidadturno"),
    ]

    operations = [
        migrations.AddField(
            model_name="video",
            name="mapa_segmentos",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
