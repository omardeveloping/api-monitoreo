from dj_rest_auth.views import LoginView
from dj_rest_auth.registration.views import RegisterView
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenObtainPairView

from .serializers import (
    CustomLoginSerializer,
    CustomRegisterSerializer,
    CustomTokenObtainPairSerializer,
)


class CustomRegisterAPIView(APIView):
    """
    Registration endpoint that returns JWT access/refresh (email + passwords only).
    """

    def post(self, request, *args, **kwargs):
        serializer = CustomRegisterSerializer(data=request.data)
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
