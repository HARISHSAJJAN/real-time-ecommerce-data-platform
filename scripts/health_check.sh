#!/usr/bin/env bash
# Checks the health of every service in the platform and prints a summary.
set -uo pipefail

cd "$(dirname "$0")/.."

check() {
  local name="$1"
  local cmd="$2"
  if eval "$cmd" >/dev/null 2>&1; then
    echo "[OK]   $name"
  else
    echo "[FAIL] $name"
  fi
}

echo "Checking platform health..."
echo ""

check "Kafka broker"      "docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092"
check "StarRocks FE"      "curl -sf http://localhost:8030/api/health"
check "PostgreSQL"        "docker compose exec -T postgres pg_isready -U \${POSTGRES_USER:-platform_admin}"
check "Trino"              "curl -sf http://localhost:8080/v1/info"
check "FastAPI"            "curl -sf http://localhost:8000/health"
check "Streamlit dashboard" "curl -sf http://localhost:8501/_stcore/health"

echo ""
echo "Detailed API health payload:"
curl -sf http://localhost:8000/health || echo "  (API unreachable)"
echo ""
