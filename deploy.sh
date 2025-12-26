#!/bin/bash
set -euo pipefail

# Deploy: update code, restart containers, restart host nginx

git pull origin main
docker compose down
docker compose up -d --build
sudo systemctl restart nginx
