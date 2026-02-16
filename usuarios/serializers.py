from dj_rest_auth.registration.serializers import RegisterSerializer
from django.contrib.auth import get_user_model
from dj_rest_auth.serializers import LoginSerializer
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework import serializers


def _capabilities_for_user(user):
    return {
        "puede_visualizar_videos": user.has_perm("dashboard.view_video"),
        "puede_eliminar_videos": user.has_perm("dashboard.delete_video"),
        "puede_gestionar_incidentes": all(
            user.has_perm(perm)
            for perm in (
                "dashboard.view_incidente",
                "dashboard.add_incidente",
                "dashboard.change_incidente",
                "dashboard.delete_incidente",
            )
        ),
    }


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

    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        token["roles"] = list(user.groups.values_list("name", flat=True))
        token["capacidades"] = _capabilities_for_user(user)
        return token

    def validate(self, attrs):
        if "email" in getattr(self, "initial_data", {}):
            raise serializers.ValidationError(
                {"email": "Campo no permitido. Use 'username'."}
            )
        data = super().validate(attrs)
        data["roles"] = list(self.user.groups.values_list("name", flat=True))
        data["capacidades"] = _capabilities_for_user(self.user)
        return data


class UsuarioSerializer(serializers.ModelSerializer):
    rol = serializers.SerializerMethodField()
    roles = serializers.SerializerMethodField()
    permisos = serializers.SerializerMethodField()
    capacidades = serializers.SerializerMethodField()

    def get_roles(self, obj):
        return list(obj.groups.values_list("name", flat=True))

    def get_rol(self, obj):
        roles = set(self.get_roles(obj))
        if "Administrador" in roles:
            return "Administrador"
        if "Supervisor" in roles:
            return "Supervisor"
        return None

    def get_permisos(self, obj):
        return sorted(obj.get_all_permissions())

    def get_capacidades(self, obj):
        return _capabilities_for_user(obj)

    class Meta:
        model = get_user_model()
        fields = [
            "id",
            "username",
            "rol",
            "roles",
            "permisos",
            "capacidades",
            "is_active",
            "is_staff",
            "is_superuser",
            "date_joined",
            "last_login",
        ]
