from dj_rest_auth.views import LoginView
from django.contrib.auth import get_user_model
from rest_framework import status
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated

from .serializers import (
    CustomLoginSerializer,
    CustomRegisterSerializer,
    CustomTokenObtainPairSerializer,
    UsuarioSerializer,
)


class CustomRegisterAPIView(GenericAPIView):
    """
    Registration endpoint that returns JWT access/refresh (username + passwords only).
    Uses GenericAPIView so DRF browsable API renders a form.
    """

    serializer_class = CustomRegisterSerializer

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save(request)

        refresh = RefreshToken.for_user(user)
        return Response(
            {"access": str(refresh.access_token), "refresh": str(refresh)},
            status=status.HTTP_201_CREATED,
        )


class CustomLoginView(LoginView):
    serializer_class = CustomLoginSerializer


class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer


class UsuarioViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = get_user_model().objects.all()
    serializer_class = UsuarioSerializer
    permission_classes = [IsAuthenticated]
