# syntax=docker/dockerfile:1

FROM python:3.13-slim

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

COPY . .

EXPOSE 8000

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["gunicorn", "apiMonitoreo.wsgi:application", "--bind", "0.0.0.0:8000"]
