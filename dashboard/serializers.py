from rest_framework import serializers
from .models import Camion

class CamionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Camion
        fields = ['id', 'patente']