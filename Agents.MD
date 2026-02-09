# Agents

This repo is a Django REST API for monitoring operations (camiones, turnos, videos,
incidentes) with Celery jobs and video processing via ffmpeg.

## Stack
- Python 3.13 (Docker image)
- Django 5.1 + Django REST Framework
- dj-rest-auth + allauth + JWT (email-only auth)
- Celery + Redis
- SQLite by default
- ffmpeg/ffprobe for video processing

## Repo layout
- `apiMonitoreo/`: project settings, urls, WSGI/ASGI, Celery app
- `dashboard/`: core domain app (models, serializers, viewsets, tasks, services)
- `usuarios/`: custom user model + auth serializers/views
- `nginx/`: reverse proxy config for static/media
- `docker-compose.yml`, `Dockerfile`, `docker-entrypoint.sh`: containerized run

## Key data models (dashboard)
- `Camion`, `Turno` (tipo_turno auto-sets horas)
- `Video`, `VelocidadVideo`, `Incidente`, `EstadisticaVideoDiaria`

## API routes
- `/api/dashboard/` router: `camiones`, `turnos`, `videos`,
  `incidentes`, `espacio-disco`
- Auth endpoints in `apiMonitoreo/urls.py`:
  `/api/auth/` (dj-rest-auth), custom login/register, JWT refresh/verify

## Background jobs
- Celery tasks in `dashboard/tasks.py`
- Beat schedule in `apiMonitoreo/settings.py`:
  - `actualizar_turnos_activos` every minute
  - `generar_turnos_diarios` 00:05 mon-sat

## Video processing workflow
- `VideoViewSet.perform_create` calls `procesar_video_subida`
- Validates mp4/h264, converts h264 to mp4, ensures mp4 compatibility, computes
  duration, updates state
- Requires `ffmpeg` + `ffprobe` (installed in Dockerfile)

## Environment variables
- Core: `DJANGO_DEBUG`, `DJANGO_ALLOWED_HOSTS`, `DJANGO_SECRET_KEY`,
  `DJANGO_SQLITE_PATH`
- Celery: `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND`
- Disk usage: `ESPACIO_DISCO_RUTA`
- MP4 tuning: `FORZAR_TRANSCODIFICACION_MP4`, `MP4_VIDEO_PROFILE`,
  `MP4_VIDEO_LEVEL`, `MP4_TARGET_FPS`, `MP4_AUDIO_SAMPLE_RATE`,
  `MP4_AUDIO_SAMPLE_RATE_MIN`, `MP4_FPS_DIFF_UMBRAL`

## Local/dev notes
- Docker: `docker compose up --build` (entrypoint runs migrations/collectstatic)
- Local: `python manage.py migrate` then `python manage.py runserver`
- Tests: `python manage.py test` (currently no tests)
