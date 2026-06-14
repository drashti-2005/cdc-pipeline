# ============================================================
# CDC Pipeline - Makefile
# Common commands for development and operations
# ============================================================

.PHONY: help up down restart logs status clean test lint

# Default target
help: ## Show this help message
	@echo "CDC Pipeline - Available Commands:"
	@echo "==================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- Docker Commands ---
up: ## Start all services
	docker compose -f docker/docker-compose.yml --env-file .env up -d

down: ## Stop all services
	docker compose -f docker/docker-compose.yml --env-file .env down

restart: ## Restart all services
	docker compose -f docker/docker-compose.yml --env-file .env restart

logs: ## Tail logs from all services
	docker compose -f docker/docker-compose.yml --env-file .env logs -f

status: ## Show status of all services
	docker compose -f docker/docker-compose.yml --env-file .env ps

# --- Individual Service Logs ---
logs-kafka: ## Tail Kafka logs
	docker compose -f docker/docker-compose.yml logs -f kafka

logs-source: ## Tail source PostgreSQL logs
	docker compose -f docker/docker-compose.yml logs -f postgres-source

logs-target: ## Tail target PostgreSQL logs
	docker compose -f docker/docker-compose.yml logs -f postgres-target

logs-consumer: ## Tail consumer logs
	docker compose -f docker/docker-compose.yml logs -f cdc-consumer

# --- Development ---
setup: ## Initial project setup (copy .env, install deps)
	@if not exist .env copy .env.example .env
	pip install -r requirements.txt

test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

test-integration: ## Run integration tests
	pytest tests/integration/ -v

lint: ## Run linting
	flake8 src/
	black --check src/

format: ## Format code
	black src/ tests/

# --- Database ---
psql-source: ## Connect to source PostgreSQL
	docker exec -it postgres-source psql -U cdc_user -d source_db

psql-target: ## Connect to target PostgreSQL
	docker exec -it postgres-target psql -U target_user -d target_db

# --- Pipeline Operations ---
simulate: ## Run traffic simulator
	python scripts/simulate_traffic.py

health: ## Run health check
	python scripts/health_check.py

reconcile: ## Run data reconciliation
	python scripts/reconcile.py

# --- dbt ---
dbt-run: ## Run dbt models
	cd dbt && dbt run

dbt-test: ## Run dbt tests
	cd dbt && dbt test

dbt-docs: ## Generate dbt docs
	cd dbt && dbt docs generate && dbt docs serve

# --- Cleanup ---
clean: ## Remove all containers, volumes, and generated files
	docker compose -f docker/docker-compose.yml --env-file .env down -v
	@echo "Cleaned up all containers and volumes"

clean-all: clean ## Remove everything including built images
	docker compose -f docker/docker-compose.yml --env-file .env down -v --rmi all
	@echo "Cleaned up all containers, volumes, and images"
