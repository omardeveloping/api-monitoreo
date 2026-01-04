import datetime
from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0014_turno_fecha'),
    ]

    operations = [
        migrations.AddField(
            model_name='video',
            name='creado_en',
            field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
            preserve_default=False,
        ),
        migrations.CreateModel(
            name='EstadisticaVideoDiaria',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('fecha', models.DateField(unique=True)),
                ('cantidad_videos', models.PositiveIntegerField(default=0)),
            ],
        ),
    ]
