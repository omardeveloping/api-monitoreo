from rest_framework import viewsets

from .models import Camion
from .serializers import CamionSerializer


class CamionViewSet(viewsets.ModelViewSet):
    queryset = Camion.objects.all()
    serializer_class = CamionSerializer
