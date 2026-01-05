from django.urls import path, include
from rest_framework import routers
from .views import (
    CamionViewSet,
    TurnoViewSet,
    VideoViewSet,
    OperadorViewSet,
    IncidenteViewSet,
    EspacioDiscoViewSet,
    AsignacionTurnoViewSet,
)

router = routers.DefaultRouter()
router.register(r'camiones', CamionViewSet)
router.register(r'turnos', TurnoViewSet)
router.register(r'videos', VideoViewSet)
router.register(r'operadores', OperadorViewSet)
router.register(r'incidentes', IncidenteViewSet)
router.register(r'espacio-disco', EspacioDiscoViewSet, basename='espacio-disco')
router.register(r'asignaciones-turno', AsignacionTurnoViewSet)

urlpatterns = [
    path('dashboard/', include(router.urls)),
]
