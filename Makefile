# Makefile for platformQ

.PHONY: help lint format test services-up services-down bootstrap-config bootstrap-gateway bootstrap-oidc-clients docs-build docs-serve

help:
	@echo "Commands:"
	@echo "  lint          : Check for style and errors in Python code."
	@echo "  format        : Automatically format Python code."
	@echo "  test          : Run all unit and integration tests."
	@echo "  services-up   : Start all infrastructure services with Docker Compose."
	@echo "  services-down : Stop all infrastructure services."
	@echo "  bootstrap-config : Populate Consul with initial configuration."
	@echo "  bootstrap-secrets : Populate Vault with initial secrets."
	@echo "  bootstrap-platform : Configure Kong, Minio, and other platform services."
	@echo "  bootstrap-oidc-clients : Register initial OIDC clients."
	@echo "  docs-build    : Generate all project documentation."
	@echo "  docs-serve    : Serve the documentation site locally."

lint:
	@echo "Running linter..."
	ruff check .

format:
	@echo "Formatting code..."
	black .
	ruff check . --fix

test:
	@echo "Running tests..."
	python3 -m pytest

services-up:
	@echo "Starting local infrastructure..."
	docker-compose -f infra/docker-compose/docker-compose.yml up -d

services-down:
	@echo "Stopping local infrastructure..."
	docker-compose -f infra/docker-compose/docker-compose.yml down

bootstrap-config:
	@echo "Bootstrapping platform configuration into Consul..."
	docker-compose -f infra/docker-compose/docker-compose.yml \
		run --rm auth-service python /app/scripts/bootstrap_config.py

bootstrap-secrets:
	@echo "Bootstrapping platform secrets into Vault..."
	docker-compose -f infra/docker-compose/docker-compose.yml \
		run --rm auth-service python /app/scripts/bootstrap_secrets.py

bootstrap-platform:
	@echo "Bootstrapping platform components (Kong, Minio, etc.)..."
	docker-compose -f infra/docker-compose/docker-compose.yml \
		run --rm auth-service python /app/scripts/bootstrap_platform.py

bootstrap-oidc-clients:
	@echo "Bootstrapping OIDC clients..."
	docker-compose -f infra/docker-compose/docker-compose.yml \
		run --rm auth-service python /app/scripts/bootstrap_oidc_clients.py

docs-build:
	@echo "Generating API documentation for shared library..."
	docker-compose -f infra/docker-compose/docker-compose.yml \
		run --rm auth-service pdoc --html --output-dir /app/docs/shared-lib-api /app/shared_lib 

docs-serve:
	@echo "Serving documentation at http://127.0.0.1:8000"
	mkdocs serve 