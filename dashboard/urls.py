from django.urls import path, include
from rest_framework import routers
from .views import (
    CamionViewSet,
    TurnoViewSet,
    VideoViewSet,
    IncidenteViewSet,
    EspacioDiscoViewSet,
)

router = routers.DefaultRouter()
router.register(r'camiones', CamionViewSet)
router.register(r'turnos', TurnoViewSet)
router.register(r'videos', VideoViewSet)
# router.register(r'operadores', OperadorViewSet)
# router.register(r'mantenimientos', MantenimientoViewSet)
router.register(r'incidentes', IncidenteViewSet)
router.register(r'espacio-disco', EspacioDiscoViewSet, basename='espacio-disco')


urlpatterns = [
    path('dashboard/', include(router.urls)),
]
