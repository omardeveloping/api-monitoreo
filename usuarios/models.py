from django.contrib.auth.models import AbstractUser
from django.db import models

class Usuario(AbstractUser):
    username = None
    email = models.EmailField(unique=True)

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []
