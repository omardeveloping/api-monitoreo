from dj_rest_auth.registration.serializers import RegisterSerializer
from django.contrib.auth import get_user_model
from dj_rest_auth.serializers import LoginSerializer
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework import serializers


class CustomRegisterSerializer(RegisterSerializer):
    """
    Registration with username only (no email).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Remove email field from the serializer/form
        self.fields.pop("email", None)
        if "username" in self.fields:
            self.fields["username"].required = True

    def get_cleaned_data(self):
        data = super().get_cleaned_data()
        return {
            "username": data.get("username", ""),
            "password1": data.get("password1", ""),
            "password2": data.get("password2", ""),
        }

    class Meta:
        model = get_user_model()
        fields = ("username", "password1", "password2")


class CustomLoginSerializer(LoginSerializer):
    """
    Login with username only (no email field).
    """

    def get_fields(self):
        fields = super().get_fields()
        fields.pop("email", None)
        if "username" not in fields:
            fields["username"] = serializers.CharField(required=True)
        return fields

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.pop("email", None)
        if "username" in self.fields:
            self.fields["username"].required = True

    def validate(self, attrs):
        if "email" in getattr(self, "initial_data", {}):
            raise serializers.ValidationError(
                {"email": "Campo no permitido. Use 'username'."}
            )
        return super().validate(attrs)


class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    """
    Issue JWT access/refresh using username.
    """

    username_field = "username"

    def validate(self, attrs):
        if "email" in getattr(self, "initial_data", {}):
            raise serializers.ValidationError(
                {"email": "Campo no permitido. Use 'username'."}
            )
        return super().validate(attrs)


class UsuarioSerializer(serializers.ModelSerializer):
    class Meta:
        model = get_user_model()
        fields = [
            "id",
            "username",
            "is_active",
            "is_staff",
            "is_superuser",
            "date_joined",
            "last_login",
        ]
