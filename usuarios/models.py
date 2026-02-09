from django.contrib.auth.models import AbstractUser

class Usuario(AbstractUser):
    email = None

    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = []
    EMAIL_FIELD = None
