# Agents

This repo is a Django REST API for monitoring operations (camiones, turnos, videos,
incidentes) with Celery jobs and video processing via ffmpeg.

## Stack
- Python 3.13 (Docker image)
- Django 5.1.4 + Django REST Framework 3.16.1
- dj-rest-auth + allauth + JWT (username-only auth, no email field)
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
- `Camion.carpeta_id` drives MDVR folder matching during automated imports

## Key behavior notes
- Only one `Camion` can exist at a time (`CamionSerializer.validate`)
- `Turno.save()` auto-updates `hora_inicio`/`hora_fin` when `tipo_turno` is set
- `VelocidadVideo` enforces uniqueness per `(video, segundo)`

## API routes
- `/api/dashboard/` router: `camiones`, `turnos`, `videos`,
  `incidentes`, `espacio-disco`
- `/api/usuarios/`: read-only authenticated users list
- Auth endpoints in `apiMonitoreo/urls.py`:
  `/api/auth/` (dj-rest-auth), custom register, JWT login/refresh/verify
- Notable custom actions:
  - `turnos`: `estadisticas`, `{id}/videos-por-turno`, `{id}/videos`, `por-dia`
  - `videos`: `importar-desde-servidor`, `archivos-servidor`,
    `preview-servidor`, `{id}/velocidades-csv`, `{id}/velocidades`,
    `conteo-hoy`, `importar-mdvr`
  - `incidentes`: `contar-alta`
  - `espacio-disco`: supports one or many paths (`rutas=...`)

## Background jobs
- Celery tasks in `dashboard/tasks.py`
- Beat schedule in `apiMonitoreo/settings.py`:
  - `actualizar_turnos_activos` every minute
  - `generar_turnos_diarios` 00:05 mon-sat
  - `importar_videos_mdvr_task` every 15 minutes

## Video processing workflow
- `VideoViewSet.perform_create` calls `procesar_video_subida`
- Validates mp4/h264, converts h264 to mp4, ensures mp4 compatibility, computes
  duration, updates state
- Manual import endpoint also exists (`videos/importar-desde-servidor`) using
  `VIDEOS_IMPORT_DIR`
- MDVR batch import service exists (`importar_videos_mdvr`) and can attach XLSX
  speed data when available
- Requires `ffmpeg` + `ffprobe` (installed in Dockerfile)

## Environment variables
- Core: `DJANGO_DEBUG`, `DJANGO_ALLOWED_HOSTS`, `DJANGO_SECRET_KEY`,
  `DJANGO_SQLITE_PATH`
- CORS: `CORS_ALLOW_ALL_ORIGINS`
- Celery: `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND`
- Video import roots: `VIDEOS_IMPORT_DIR`, `VIDEOS_MDVR_DIR`
- Disk usage: `ESPACIO_DISCO_RUTA` (single default), `ESPACIO_DISCO_RUTAS`
  (comma-separated override in endpoint)
- MP4 tuning: `FORZAR_TRANSCODIFICACION_MP4`, `MP4_VIDEO_PROFILE`,
  `MP4_VIDEO_LEVEL`, `MP4_TARGET_FPS`, `MP4_AUDIO_SAMPLE_RATE`,
  `MP4_AUDIO_SAMPLE_RATE_MIN`, `MP4_FPS_DIFF_UMBRAL`

## Local/dev notes
- Docker: `docker compose up --build` (entrypoint runs migrations/collectstatic)
- Local API: `python manage.py migrate` then `python manage.py runserver`
- Local workers:
  - `celery -A apiMonitoreo worker -l info`
  - `celery -A apiMonitoreo beat -l info`
- Tests: `python manage.py test` (currently no tests)
