from dj_rest_auth.registration.serializers import RegisterSerializer
from django.contrib.auth import get_user_model
from dj_rest_auth.serializers import LoginSerializer
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework import serializers


class CustomRegisterSerializer(RegisterSerializer):
    """
    Registration without username: only email + passwords.
    """

    username = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Remove username field from the serializer/form
        self.fields.pop("username", None)

    def get_cleaned_data(self):
        data = super().get_cleaned_data()
        return {
            "email": data.get("email", ""),
            "password1": data.get("password1", ""),
            "password2": data.get("password2", ""),
        }

    class Meta:
        model = get_user_model()
        fields = ("email", "password1", "password2")


class CustomLoginSerializer(LoginSerializer):
    """
    Login with email only (no username field).
    """

    username = None

    @property
    def username_field(self):
        return "email"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.pop("username", None)
        self.fields["email"] = serializers.EmailField(required=True)


class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    """
    Issue JWT access/refresh using email instead of username.
    """

    username_field = "email"
