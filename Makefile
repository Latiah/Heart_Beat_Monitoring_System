# Developer entrypoints for the Heart Beat Monitoring System.

.PHONY: install up down clean logs db-init db-reset psql \
        run-generator run-producer run-consumer grafana \
        test test-unit test-integration lint format

PG_CONTAINER ?= heartbeat-postgres
KAFKA_CONTAINER ?= heartbeat-kafka

# Python tools are invoked as `python -m <tool>` rather than as bare
# executables. pip installs console scripts into a Scripts/ directory that is
# often not on PATH (notably for Microsoft Store Python), and `python -m`
# additionally guarantees the tool comes from the same interpreter as the
# package under test. Override with `make PYTHON=.venv/bin/python <target>`.
PYTHON ?= python
LINT_PATHS ?= src scripts tests

# Load .env first so db-init/psql target the same database the app and
# docker-compose do. Deriving PG_USER/PG_DB from POSTGRES_* below (rather
# than hardcoding them) is what keeps these targets working when .env
# overrides the credentials.
ifneq (,$(wildcard .env))
include .env
export
endif

PG_USER ?= $(if $(POSTGRES_USER),$(POSTGRES_USER),heartbeat_user)
PG_DB   ?= $(if $(POSTGRES_DB),$(POSTGRES_DB),heartbeat_monitoring)


install:  ## Install the package (editable) plus dev extras
	$(PYTHON) -m pip install -e ".[dev]"

up:  ## Start Kafka and PostgreSQL, waiting until both are healthy
	docker compose up -d --wait
	@echo "Kafka and PostgreSQL are healthy."

down:  ## Stop containers, keeping stored data
	docker compose down

clean:  ## Stop containers AND delete the PostgreSQL volume
	docker compose down -v

logs:  ## Tail infrastructure logs
	docker compose logs -f

db-init:  ## Apply schema and indexes (idempotent; safe to re-run)
	docker exec -i $(PG_CONTAINER) psql -v ON_ERROR_STOP=1 -U $(PG_USER) -d $(PG_DB) \
		< sql/schema/create_tables.sql
	docker exec -i $(PG_CONTAINER) psql -v ON_ERROR_STOP=1 -U $(PG_USER) -d $(PG_DB) \
		< sql/indexes/create_indexes.sql
	@echo "Schema and indexes applied."

db-reset:  ## Drop and recreate the readings table (destroys all rows)
	docker exec -i $(PG_CONTAINER) psql -v ON_ERROR_STOP=1 -U $(PG_USER) -d $(PG_DB) \
		-c "DROP TABLE IF EXISTS heart_rate_readings;"
	$(MAKE) db-init

psql:  ## Open an interactive psql shell
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

run-generator:  ## Preview generated events on stdout (no Kafka needed)
	$(PYTHON) scripts/run_generator.py

run-producer:  ## Generate and publish events to Kafka
	$(PYTHON) scripts/run_producer.py

run-consumer:  ## Consume, validate, classify, and persist to PostgreSQL
	$(PYTHON) scripts/run_consumer.py

grafana:  ## Print the Grafana dashboard URL (it runs as part of `make up`)
	@echo "Grafana dashboard: http://localhost:$(or $(GRAFANA_PORT),3000)"
	@echo "No login required. Datasource and dashboard are provisioned from ./grafana."

test-unit:  ## Fast tests, no infrastructure required
	$(PYTHON) -m pytest tests/unit -m "not integration" -v

test-integration:  ## End-to-end tests (needs `make up` and `make db-init`)
	$(PYTHON) -m pytest tests/integration -m integration -v

test: test-unit  ## Alias for test-unit

lint:  ## Check formatting and lint rules
	$(PYTHON) -m ruff check $(LINT_PATHS)
	$(PYTHON) -m ruff format --check $(LINT_PATHS)

format:  ## Apply formatting and auto-fixable lint rules
	$(PYTHON) -m ruff check --fix $(LINT_PATHS)
	$(PYTHON) -m ruff format $(LINT_PATHS)
