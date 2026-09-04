.PHONY: help setup up down restart logs ps build init-db seed-dimensions generate-sample test lint fmt clean

help:
	@echo "Available targets:"
	@echo "  setup             Copy .env.example to .env (if missing) and build all images"
	@echo "  up                Start the full platform with docker compose"
	@echo "  down              Stop and remove all containers"
	@echo "  restart           Restart all services"
	@echo "  logs              Tail logs for all services"
	@echo "  ps                Show running services"
	@echo "  build             Rebuild all custom images"
	@echo "  init-db           Re-apply StarRocks schema (idempotent)"
	@echo "  seed-dimensions   Seed StarRocks users/products dimension tables"
	@echo "  generate-sample   Generate a batch of sample events to local files (no Kafka needed)"
	@echo "  test              Run the pytest suite"
	@echo "  lint              Run ruff + black --check"
	@echo "  fmt               Auto-format with black"
	@echo "  clean             Remove containers and named volumes (destructive)"

setup:
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env from .env.example"; fi
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

build:
	docker compose build

init-db:
	docker compose run --rm starrocks-init

seed-dimensions:
	python -m starrocks.seed.seed_dimensions

generate-sample:
	python scripts/generate_sample_data.py

test:
	pytest -v

lint:
	ruff check .
	black --check .

fmt:
	black .

clean:
	docker compose down -v
