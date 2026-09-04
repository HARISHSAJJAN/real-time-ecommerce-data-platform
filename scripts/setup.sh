#!/usr/bin/env bash
# Bring up the full platform from a clean checkout.
# Run from the repository root: ./scripts/setup.sh
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example (edit it if you need non-default settings)."
fi

echo "==> Building custom images (data-generator, spark-streaming, api, dashboard)..."
docker compose build

echo "==> Starting core infrastructure (Kafka, StarRocks, Postgres)..."
docker compose up -d kafka postgres starrocks
docker compose up kafka-init
docker compose up starrocks-init

echo "==> Starting Trino, the streaming job, the generator, the API, and the dashboard..."
docker compose up -d trino spark-streaming data-generator api dashboard kafka-ui

echo "==> Waiting for the API to report healthy..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
    echo "API is up."
    break
  fi
  sleep 5
done

echo ""
echo "Platform is starting. It will take a few minutes for meaningful data to"
echo "accumulate (events -> Spark micro-batches -> StarRocks -> dashboard)."
echo ""
echo "  Dashboard:  http://localhost:8501"
echo "  API docs:   http://localhost:8000/docs"
echo "  Kafka UI:   http://localhost:8081"
echo "  Trino UI:   http://localhost:8080"
echo ""
echo "Run ./scripts/health_check.sh to check every component's status."
