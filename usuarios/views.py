from dj_rest_auth.registration.views import RegisterView
from dj_rest_auth.views import LoginView
from .serializers import CustomRegisterSerializer
from .serializers import CustomLoginSerializer, CustomTokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView


class CustomRegisterView(RegisterView):
    serializer_class = CustomRegisterSerializer


class CustomLoginView(LoginView):
    serializer_class = CustomLoginSerializer


class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer
