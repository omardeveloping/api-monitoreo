import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0016_video_fecha_subida'),
    ]

    operations = [
        migrations.CreateModel(
            name='TipoTurno',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('nombre', models.CharField(choices=[('manana', 'Mañana'), ('tarde', 'Tarde'), ('noche', 'Noche'), ('variable', 'Variable')], max_length=20, unique=True)),
                ('hora_inicio', models.TimeField()),
                ('hora_fin', models.TimeField()),
            ],
        ),
        migrations.AddField(
            model_name='turno',
            name='tipo_turno',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='dashboard.tipoturno'),
        ),
    ]
