from django.urls import path, include
from rest_framework import routers
from .views import CamionViewSet, TurnoViewSet, VideoViewSet, OperadorViewSet, IncidenteViewSet

router = routers.DefaultRouter()
router.register(r'camiones', CamionViewSet)
router.register(r'turnos', TurnoViewSet)
router.register(r'videos', VideoViewSet)
router.register(r'operadores', OperadorViewSet)
router.register(r'incidentes', IncidenteViewSet)

urlpatterns = [
    path('dashboard/', include(router.urls)),
]
