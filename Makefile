SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose --env-file .env -f compose.yaml

.PHONY: help bootstrap-env up seed pipeline test test-unit test-integration smoke health \
	catalog-up catalog-down failure-test backup restore-test down dev-init dbt-build \
	credentials dependency-check lint config

help:
	@awk 'BEGIN {FS = ":.*## "; printf "Kest commands:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap-env: ## Generate local-only credentials and runtime configuration
	@python3 ops/bootstrap_env.py

config: bootstrap-env ## Validate the resolved Docker Compose model
	@$(COMPOSE) config --quiet

up: bootstrap-env ## Build and start the core platform
	@$(COMPOSE) up -d --build --wait postgres redis seaweed-master seaweed-volume seaweed-filer seaweed-s3 clickhouse source-api
	@$(COMPOSE) run --rm airflow-init
	@$(COMPOSE) run --rm superset-init
	@$(COMPOSE) up -d --no-build --wait airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer superset superset-worker

seed: bootstrap-env ## Seed deterministic ecommerce, event, REST and CSV source data
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli seed

pipeline: bootstrap-env ## Run the source-to-Gold pipeline synchronously
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli pipeline

test: test-unit test-integration ## Run unit and integration suites

test-unit: bootstrap-env ## Run fast unit and contract tests
	@$(COMPOSE) run --rm tooling pytest -q tests/unit

test-integration: bootstrap-env ## Run integration tests against the running core stack
	@$(COMPOSE) run --rm tooling pytest -q tests/integration tests/security

smoke: bootstrap-env ## Verify the end-to-end business outcome
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli smoke

health: bootstrap-env ## Run deep service, SQL and S3 health checks
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli health

catalog-up: bootstrap-env ## Start optional OpenMetadata catalog services
	@$(COMPOSE) --profile catalog up -d --wait opensearch openmetadata-migrate openmetadata-server

catalog-down: bootstrap-env ## Stop optional OpenMetadata catalog services
	@$(COMPOSE) --profile catalog stop openmetadata-server opensearch

failure-test: bootstrap-env ## Run replay, retry, contract, publish and workload failures
	@$(COMPOSE) run --rm tooling pytest -q tests/integration/test_failure_scenarios.py tests/security/test_rbac.py

backup: bootstrap-env ## Back up critical PostgreSQL, ClickHouse and raw data
	@bash ops/backup.sh

restore-test: bootstrap-env ## Restore backup into isolated test targets and compare checksums
	@bash ops/restore_test.sh

down: bootstrap-env ## Stop services while retaining persistent data
	@$(COMPOSE) --profile catalog down

dev-init: bootstrap-env ## Create an isolated ClickHouse database; DEVELOPER is required
	@test -n "$(DEVELOPER)" || (echo "DEVELOPER is required" >&2; exit 2)
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli dev-init --developer "$(DEVELOPER)"

dbt-build: bootstrap-env ## Build dbt in an isolated dev database; DEVELOPER is required
	@test -n "$(DEVELOPER)" || (echo "DEVELOPER is required" >&2; exit 2)
	@$(COMPOSE) run --rm -e DEVELOPER="$(DEVELOPER)" tooling /opt/venvs/dbt/bin/dbt build --project-dir transformations --profiles-dir transformations --target dev

credentials: bootstrap-env ## Show local UI endpoints and generated usernames/passwords
	@python3 ops/show_credentials.py

dependency-check: bootstrap-env ## Check pinned Python dependencies and container configuration
	@$(COMPOSE) run --rm tooling pip check
	@$(COMPOSE) config --quiet

lint: bootstrap-env ## Run Python linting and static configuration checks
	@$(COMPOSE) run --rm tooling ruff check ingestion orchestration tests ops
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli validate-config
