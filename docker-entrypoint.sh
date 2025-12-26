#!/bin/sh
set -e

if [ -n "${DJANGO_SQLITE_PATH:-}" ]; then
  mkdir -p "$(dirname "$DJANGO_SQLITE_PATH")"
fi

python manage.py makemigrations --noinput
python manage.py migrate --noinput
python manage.py collectstatic --noinput

exec "$@"
