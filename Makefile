SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose --env-file .env -f compose.yaml

.PHONY: help bootstrap-env up demo-up seed pipeline demo de da source-new test test-unit test-integration smoke health type \
	catalog-up catalog-down failure-test backup restore-test down dev-init dbt-build \
	credentials dependency-check lint config

help:
	@awk 'BEGIN {FS = ":.*## "; printf "Kest commands:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bootstrap-env: ## Generate local-only credentials and runtime configuration
	@python3 ops/bootstrap_env.py

config: bootstrap-env ## Validate the resolved Docker Compose model
	@$(COMPOSE) config --quiet

up: bootstrap-env ## Build and start the core platform
	@$(COMPOSE) build tooling airflow-init superset-init
	@$(COMPOSE) run --rm --no-deps --user root tooling bash -ec 'chmod 0711 /opt/kest/.runtime; mkdir -p /opt/kest/.runtime/{batches,dlt,dbt/logs,dbt/target,backups}; chmod -R a+rwX /opt/kest/.runtime/{batches,dlt,dbt,backups}'
	@$(COMPOSE) up -d --no-build --wait postgres redis object-store clickhouse
	@$(COMPOSE) run --rm --no-deps object-store-init
	@$(COMPOSE) exec -T clickhouse bash -ec 'clickhouse-client --user default --password "$$CLICKHOUSE_PASSWORD" --multiquery < /docker-entrypoint-initdb.d/10-raw-tables.sql'
	@$(COMPOSE) run --rm --no-deps airflow-init
	@$(COMPOSE) run --rm --no-deps superset-init
	@$(COMPOSE) up -d --no-build --wait airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer superset superset-worker

demo-up: up ## Start the optional demo REST API
	@$(COMPOSE) --profile demo up -d --no-build --wait source-api

seed: bootstrap-env ## Seed deterministic ecommerce, event, REST and CSV source data
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli seed

pipeline: bootstrap-env ## Run the source-to-Gold pipeline synchronously
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli pipeline

demo: demo-up seed ## Run the isolated sample workload (not required by the platform)
	@$(COMPOSE) run --rm -e SOURCE_CONFIG_DIR=/opt/kest/ingestion/examples/config tooling python -m kest_ingestion.cli pipeline
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli smoke
	@echo "Demo ready: Superset http://127.0.0.1:8088 | Airflow http://127.0.0.1:8080"

de: dev-init dbt-build ## One-command DE workspace; use DEVELOPER=alice
	@echo "DE workspace ready: dev_$(DEVELOPER) (credentials: .runtime/developers/$(DEVELOPER).env)"

da: bootstrap-env ## Show the DA entry point (no infrastructure knowledge required)
	@python3 ops/show_credentials.py --audience da

source-new: bootstrap-env ## Add REST source; requires SOURCE, BASE_URL and ENDPOINT
	@test -n "$(SOURCE)" -a -n "$(BASE_URL)" -a -n "$(ENDPOINT)" || (echo "SOURCE, BASE_URL and ENDPOINT are required" >&2; exit 2)
	@$(COMPOSE) run --rm tooling python ops/new_source.py --name "$(SOURCE)" --base-url "$(BASE_URL)" --endpoint "$(ENDPOINT)" $(if $(TABLE),--table "$(TABLE)") $(if $(PRIMARY_KEY),--primary-key "$(PRIMARY_KEY)")

test: test-unit test-integration ## Run unit and integration suites

test-unit: bootstrap-env ## Run fast unit and contract tests
	@$(COMPOSE) run --rm tooling pytest -q tests/unit

test-integration: bootstrap-env ## Run integration tests against the running core stack
	@$(COMPOSE) run --rm tooling pytest -q -m "not demo" tests/integration tests/security

smoke: bootstrap-env ## Verify the end-to-end business outcome
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli smoke

health: bootstrap-env ## Run deep service, SQL and S3 health checks
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli health

catalog-up: bootstrap-env ## Start optional OpenMetadata catalog services
	@$(COMPOSE) --profile catalog up -d --wait opensearch openmetadata-migrate openmetadata-server

catalog-down: bootstrap-env ## Stop optional OpenMetadata catalog services
	@$(COMPOSE) --profile catalog stop openmetadata-server opensearch

failure-test: demo-up ## Run replay, retry, contract, publish and workload failures
	@$(COMPOSE) run --rm tooling pytest -q tests/integration/test_failure_scenarios.py tests/security/test_rbac.py

backup: bootstrap-env ## Back up critical PostgreSQL, ClickHouse and raw data
	@bash ops/backup.sh

restore-test: bootstrap-env ## Restore backup into isolated test targets and compare checksums
	@bash ops/restore_test.sh

down: bootstrap-env ## Stop services while retaining persistent data
	@$(COMPOSE) --profile catalog --profile demo --profile recovery down

dev-init: bootstrap-env ## Create an isolated ClickHouse database; DEVELOPER is required
	@test -n "$(DEVELOPER)" || (echo "DEVELOPER is required" >&2; exit 2)
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli dev-init --developer "$(DEVELOPER)"

dbt-build: bootstrap-env ## Build dbt in an isolated dev database; DEVELOPER is required
	@test -n "$(DEVELOPER)" || (echo "DEVELOPER is required" >&2; exit 2)
	@$(COMPOSE) run --rm -e DEVELOPER="$(DEVELOPER)" tooling bash -ec 'test -f "/opt/kest/.runtime/developers/$${DEVELOPER}.env" || { echo "Run make dev-init DEVELOPER=$${DEVELOPER} first" >&2; exit 2; }; set -a; source "/opt/kest/.runtime/developers/$${DEVELOPER}.env"; set +a; exec /opt/venvs/dbt/bin/dbt build --project-dir transformations --profiles-dir transformations --target dev'

credentials: bootstrap-env ## Show local UI endpoints and generated usernames/passwords
	@python3 ops/show_credentials.py

dependency-check: bootstrap-env ## Check pinned Python dependencies and container configuration
	@$(COMPOSE) run --rm tooling pip check
	@$(COMPOSE) config --quiet

lint: bootstrap-env ## Run Python linting and static configuration checks
	@$(COMPOSE) run --rm tooling ruff check ingestion orchestration tests ops serving
	@$(COMPOSE) run --rm tooling python -m kest_ingestion.cli validate-config

type: bootstrap-env ## Type-check platform Python code
	@$(COMPOSE) run --rm tooling mypy ingestion ops serving
