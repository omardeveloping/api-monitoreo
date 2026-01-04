from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0018_turno_activo'),
    ]

    operations = [
        migrations.AlterField(
            model_name='turno',
            name='fecha',
            field=models.DateField(default=django.utils.timezone.localdate),
        ),
    ]
