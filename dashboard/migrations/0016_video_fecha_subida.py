from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0015_video_creado_en_estadisticavideodiaria'),
    ]

    operations = [
        migrations.AddField(
            model_name='video',
            name='fecha_subida',
            field=models.DateField(default=django.utils.timezone.localdate),
        ),
    ]
