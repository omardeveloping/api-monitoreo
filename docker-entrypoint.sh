#!/bin/sh
set -e

if [ -n "${DJANGO_SQLITE_PATH:-}" ]; then
  mkdir -p "$(dirname "$DJANGO_SQLITE_PATH")"
fi

python manage.py migrate --noinput

exec "$@"
