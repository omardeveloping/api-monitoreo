from django.urls import path, include
from rest_framework import routers
from .views import CamionViewSet, TurnoViewSet, VideoViewSet

router = routers.DefaultRouter()
router.register(r'camiones', CamionViewSet)
router.register(r'turnos', TurnoViewSet)
router.register(r'videos', VideoViewSet)

urlpatterns = [
    path('dashboard/', include(router.urls)),
]