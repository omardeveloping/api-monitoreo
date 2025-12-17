from django.urls import path, include
from rest_framework import routers
from .views import CamionViewSet

router = routers.DefaultRouter()
router.register(r'camiones', CamionViewSet)
urlpatterns = [
    path('dashboard/', include(router.urls)),
]