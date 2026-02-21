#!/bin/sh
set -e

if [ -n "${DJANGO_SQLITE_PATH:-}" ]; then
  mkdir -p "$(dirname "$DJANGO_SQLITE_PATH")"
fi

if [ "${RUN_MAKEMIGRATIONS:-0}" = "1" ]; then
  python manage.py makemigrations --noinput
fi

if [ "${RUN_MIGRATIONS:-0}" = "1" ]; then
  python manage.py migrate --noinput
fi

if [ "${RUN_COLLECTSTATIC:-0}" = "1" ]; then
  python manage.py collectstatic --noinput
fi

exec "$@"
