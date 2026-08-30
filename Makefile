.PHONY: install up down logs test test-unit test-integration psql

install:
	pip install -e ".[dev]"

up:
	docker compose up -d
	@echo "Waiting for Kafka and Postgres to become healthy..."

down:
	docker compose down

logs:
	docker compose logs -f

test-unit:
	pytest tests/unit -m "not integration"

test-integration:
	pytest tests/integration -m integration

test: test-unit

psql:
	docker exec -it heartbeat-postgres psql -U heartbeat_user -d heartbeat_monitoring
