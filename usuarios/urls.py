from rest_framework import routers

from .views import UsuarioViewSet

router = routers.DefaultRouter()
router.register(r"usuarios", UsuarioViewSet, basename="usuarios")

urlpatterns = router.urls
