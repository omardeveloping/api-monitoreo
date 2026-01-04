from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0017_tipoturno_turno_tipo_turno'),
    ]

    operations = [
        migrations.AddField(
            model_name='turno',
            name='activo',
            field=models.BooleanField(default=False),
        ),
    ]
