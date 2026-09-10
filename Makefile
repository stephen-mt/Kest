SHELL := /bin/bash

BASE_COMPOSE := docker compose --project-directory . -f deploy/local/compose.yaml
CYBERMARKET_COMPOSE := $(BASE_COMPOSE) -f deploy/local/compose.cybermarket.yaml --profile cybermarket
PLATFORM_COMPOSE := $(BASE_COMPOSE) -f deploy/local/compose.platform.yaml --profile platform
DELIVERY_COMPOSE := $(BASE_COMPOSE) -f deploy/local/compose.platform.yaml -f deploy/local/compose.delivery-ops.yaml --profile platform --profile delivery --profile delivery-cdc
RUFF := UV_CACHE_DIR=.uv-cache UV_TOOL_DIR=.uv-cache/tools uvx --from ruff==0.16.6 ruff

.PHONY: doctor config env lint test check up down restart status logs demo demo-full
.PHONY: platform-up platform-cdc-up platform-down platform-check platform-sql
.PHONY: cybermarket-image cybermarket-setup cybermarket-history cybermarket-history-reset
.PHONY: cybermarket-cdc cybermarket-generate cybermarket-cdc-drain cybermarket-cdc-test
.PHONY: cybermarket-batch cybermarket-batch-check cybermarket-airflow
.PHONY: cybermarket-check cybermarket-history-check cybermarket-history-deep-check cybermarket-cdc-check
.PHONY: cybermarket-legacy-clean cybermarket-legacy-purge
.PHONY: delivery-up delivery-image delivery-setup delivery-seed delivery-source-check
.PHONY: delivery-live delivery-batch delivery-check delivery-cdc-up delivery-cdc-down
.PHONY: delivery-streaming-up delivery-streaming-check delivery-streaming-down
.PHONY: delivery-airflow-up delivery-airflow delivery-down airflow-dag-check

# Repository entry points -----------------------------------------------------

doctor:
	python3 scripts/doctor.py

config:
	$(CYBERMARKET_COMPOSE) config --quiet
	$(DELIVERY_COMPOSE) config --quiet

env:
	uv venv .venv
	uv pip sync requirements.txt --python .venv/bin/python

lint:
	$(RUFF) format --check src workloads orchestration tests scripts deploy/local/services
	$(RUFF) check src workloads orchestration tests scripts deploy/local/services

test: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm --no-deps cybermarket-job python -m unittest discover -s tests/unit

check: config lint test

up:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	$(BASE_COMPOSE) up -d --wait postgres-source postgres-airflow postgres-catalog minio
	$(BASE_COMPOSE) run --rm --no-deps lakekeeper migrate
	$(BASE_COMPOSE) up -d --wait --wait-timeout 300
	python3 deploy/local/services/lakekeeper/bootstrap.py

down:
	$(DELIVERY_COMPOSE) stop

restart: down up

status:
	$(DELIVERY_COMPOSE) ps -a

logs:
	$(DELIVERY_COMPOSE) logs -f --tail=100

# A small first-run path; the full fixture remains explicit because it is large.
demo: up cybermarket-image cybermarket-setup
	BRONZE_TARGET_BYTES=$${DEMO_BRONZE_TARGET_BYTES:-134217728} $(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.batch.history
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.batch.runner
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.state --phase batch

demo-full: up cybermarket-setup cybermarket-history cybermarket-batch cybermarket-batch-check

# Optional platform services -------------------------------------------------

platform-up:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	$(PLATFORM_COMPOSE) up -d --wait --wait-timeout 300 nifi trino
	$(PLATFORM_COMPOSE) exec -T trino trino --file /etc/trino/bootstrap.sql
	$(PLATFORM_COMPOSE) exec -T nifi python3 /opt/kest/nifi/bootstrap.py

platform-cdc-up: platform-up
	$(PLATFORM_COMPOSE) up -d --wait --wait-timeout 180 debezium-postgres

platform-down:
	$(PLATFORM_COMPOSE) stop debezium-postgres nifi trino

platform-check:
	$(PLATFORM_COMPOSE) exec -T nifi python3 /opt/kest/nifi/check.py
	$(PLATFORM_COMPOSE) exec -T trino trino --execute 'SHOW CATALOGS; SELECT 1 AS ready'

platform-sql:
	$(PLATFORM_COMPOSE) exec trino trino --catalog lakehouse

# CyberMarket workload -------------------------------------------------------

cybermarket-image:
	$(CYBERMARKET_COMPOSE) build cybermarket-job

cybermarket-setup: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.source.bootstrap

cybermarket-history: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.batch.history

cybermarket-history-reset: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.batch.history --reset

cybermarket-cdc: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.ingestion.cdc

cybermarket-generate: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.ingestion.generator

cybermarket-cdc-drain: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.ingestion.cdc --drain

cybermarket-cdc-test: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.smoke

cybermarket-batch: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.batch.runner

cybermarket-batch-check: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.state --phase batch

cybermarket-legacy-clean: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.maintenance

cybermarket-legacy-purge: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.maintenance --purge

cybermarket-airflow:
	$(BASE_COMPOSE) exec -T airflow airflow dags trigger cybermarket_batch

cybermarket-check: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.state --phase setup

cybermarket-history-check: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.state --phase history

cybermarket-history-deep-check: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.deep_history

cybermarket-cdc-check: cybermarket-image
	$(CYBERMARKET_COMPOSE) run --rm cybermarket-job python -m workloads.cybermarket.validation.state --phase cdc

# DeliveryOps workload -------------------------------------------------------

delivery-up:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	$(DELIVERY_COMPOSE) up -d --wait --wait-timeout 300 postgres-delivery nifi trino
	python3 deploy/local/services/lakekeeper/bootstrap.py --delivery
	$(DELIVERY_COMPOSE) exec -T trino trino --file /etc/trino/delivery-bootstrap.sql
	$(DELIVERY_COMPOSE) exec -T nifi sh -ec 'MINIO_BUCKET="$$DELIVERY_MINIO_BUCKET" LAKEKEEPER_WAREHOUSE="$$DELIVERY_LAKEKEEPER_WAREHOUSE" exec python3 /opt/kest/nifi/bootstrap.py'

delivery-image:
	$(DELIVERY_COMPOSE) build delivery-job

delivery-setup: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli setup

delivery-seed: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli seed

delivery-source-check: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli source-check

delivery-live: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli emit-live

delivery-batch: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli batch

delivery-check: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli validate

delivery-cdc-up: delivery-up
	$(DELIVERY_COMPOSE) up -d --wait --wait-timeout 180 debezium-delivery

delivery-cdc-down: delivery-image
	$(DELIVERY_COMPOSE) stop debezium-delivery
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli cdc-down

delivery-streaming-up: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli streaming-setup

delivery-streaming-check: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli streaming-check

delivery-streaming-down: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m workloads.delivery_ops.cli streaming-down

delivery-airflow-up:
	$(DELIVERY_COMPOSE) up -d --wait --wait-timeout 300 airflow

delivery-airflow:
	$(DELIVERY_COMPOSE) exec -T airflow airflow dags trigger delivery_ops_batch

delivery-down: delivery-cdc-down delivery-streaming-down
	$(DELIVERY_COMPOSE) stop postgres-delivery

airflow-dag-check:
	$(BASE_COMPOSE) exec -T airflow airflow dags list
	$(BASE_COMPOSE) exec -T airflow airflow dags list-import-errors
