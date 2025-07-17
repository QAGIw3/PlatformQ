# Makefile for platformQ

.PHONY: help lint format test services-up services-down bootstrap-config bootstrap-gateway bootstrap-oidc-clients docs-build docs-serve package-private-cloud package-airgapped deps compile-deps clean scan sast-scan sca-scan container-scan install-knative uninstall-knative

help:
	@echo "Commands:"
	@echo "  lint          : Check for style and errors in Python code."
	@echo "  format        : Automatically format Python code."
	@echo "  test          : Run all unit and integration tests."
	@echo "  test-coverage : Run tests and generate an HTML coverage report."
	@echo "  services-up   : Start all infrastructure services with Docker Compose."
	@echo "  services-down : Stop all infrastructure services."
	@echo "  bootstrap     : Run all bootstrap steps in order."
	@echo "  bootstrap-config : Populate Consul with initial configuration."
	@echo "  bootstrap-secrets : Populate Vault with initial secrets."
	@echo "  bootstrap-platform : Configure Kong, Minio, and other platform services."
	@echo "  bootstrap-oidc-clients : Register initial OIDC clients."
	@echo "  docs-build    : Generate all project documentation."
	@echo "  docs-serve    : Serve the documentation site locally."
	@echo "  deps          : Install development dependencies like pip-tools."
	@echo "  compile-deps  : Compile all requirements.in files to requirements.txt."
	@echo "  clean         : Remove generated files (e.g., requirements.txt)."
	@echo "  package-private-cloud : Package the application for online distribution."
	@echo "  package-airgapped : Package the application for air-gapped distribution."
	@echo "  scan          : Run all security scans."
	@echo "  sast-scan     : Run Static Application Security Testing (SAST) scan."
	@echo "  sca-scan      : Run Software Composition Analysis (SCA) scan."
	@echo "  container-scan: Run container image scan."
	@echo "  install-knative : Install Knative Serving into the Kubernetes cluster."
	@echo "  uninstall-knative : Uninstall Knative Serving from the Kubernetes cluster."

lint:
	@echo "Running linter..."
	ruff check .

format:
	@echo "Formatting code..."
	black .
	ruff check . --fix

test:
	@echo "Installing shared library in editable mode..."
	pip install -e libs/shared
	@echo "Installing dependencies for all services..."
	@for dir in services/*/ ; do \
		if [ -f "$${dir}requirements.txt" ]; then \
			echo "--> Installing dependencies for $${dir}"; \
			pip install -r "$${dir}requirements.txt"; \
		fi \
	done
	@echo "Running tests..."
	python3 -m pytest

test-coverage:
	@echo "Installing shared library in editable mode..."
	pip install -e libs/shared
	@echo "Installing dependencies for all services..."
	@for dir in services/*/ ; do \
		if [ -f "$${dir}requirements.txt" ]; then \
			echo "--> Installing dependencies for $${dir}"; \
			pip install -r "$${dir}requirements.txt"; \
		fi \
	done
	@echo "Running tests with coverage..."
	coverage run -m pytest
	@echo "Generating HTML coverage report..."
	coverage html
	@echo "Coverage report generated at htmlcov/index.html"

services-up:
	@echo "Starting local infrastructure..."
	docker-compose -f infra/docker-compose/docker-compose.yml up -d

services-down:
	@echo "Stopping local infrastructure..."
	docker-compose -f infra/docker-compose/docker-compose.yml down

bootstrap: bootstrap-config bootstrap-secrets bootstrap-platform bootstrap-oidc-clients
	@echo "All bootstrap steps completed."

deps:
	@echo "Installing development dependencies..."
	pip install --upgrade pip-tools

compile-deps:
	@echo "Compiling base requirements..."
	pip-compile requirements/base.in -o requirements/base.txt
	@echo "Compiling service requirements..."
	@for dir in services/*/ ; do \
		if [ -f "$${dir}requirements.in" ]; then \
			echo "--> Compiling $${dir}requirements.in"; \
			pip-compile "$${dir}requirements.in" -o "$${dir}requirements.txt"; \
		fi \
	done

clean:
	@echo "Cleaning up generated files..."
	find . -name "requirements.txt" -delete
	rm -f requirements/base.txt
	@echo "Done."

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

package-private-cloud:
	@echo "Packaging platformQ Private Cloud Edition..."
	# First, we need to vendor all the Helm dependencies so they are included in the package.
	helm dependency build iac/kubernetes/charts/platformq-stack
	
	# Now, we create the distributable .kots file
	# In a real pipeline, a 'kots pull' would also be needed to vendor images
	# for a true air-gapped installation.
	kots release create --chart iac/kubernetes/charts/platformq-stack --namespace platformq --version 1.0.0
	@echo "Package created at ./platformq-private-cloud.kots" 

package-airgapped:
	@echo "Packaging platformQ Private Cloud Edition for air-gapped environments..."
	# This requires the 'kots' CLI to be installed.
	# The kots CLI will find all images in the Helm chart and vendor them.
	kots release create --chart iac/kubernetes/charts/platformq-stack \
		--namespace platformq --version 1.0.1 --airgap-bundle ./platformq-airgap.tar.gz
	@echo "Air-gapped package created at ./platformq-airgap.tar.gz" 

scan: sast-scan sca-scan container-scan
	@echo "All security scans complete."

sast-scan:
	@echo "Running SAST scan with Semgrep..."
	docker run --rm -v "$(CURDIR):/src" returntocorp/semgrep semgrep --config="p/python" --error .

sca-scan:
	@echo "Running SCA scan with pip-audit..."
	pip-audit -r requirements/base.txt
	@for dir in services/*/ ; do \
		if [ -f "$${dir}requirements.txt" ]; then \
			echo "--> Scanning $${dir}requirements.txt"; \
			pip-audit -r "$${dir}requirements.txt"; \
		fi \
	done

container-scan:
	@echo "Running container scan with Trivy..."
	@for dir in $(shell find . -name 'Dockerfile' -printf '%h\n'); do \
		if [ -f "$${dir}/Dockerfile" ]; then \
			IMAGE_NAME="platformq/$(shell basename $${dir}):latest"; \
			echo "--> Building $$IMAGE_NAME"; \
			docker build -t $$IMAGE_NAME $${dir}; \
			echo "--> Scanning $$IMAGE_NAME"; \
			docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
				-v "$(HOME)/.trivy/cache:/root/.cache/" \
				aquasec/trivy image --exit-code 1 --severity HIGH,CRITICAL $$IMAGE_NAME; \
		fi \
	done 

install-knative:
	@echo "Installing Knative Serving..."
	kubectl apply -f iac/kubernetes/knative/serving-crds.yaml
	kubectl apply -f iac/kubernetes/knative/serving-core.yaml
	kubectl apply -f iac/kubernetes/knative/serving-hpa.yaml
	@echo "Knative Serving installed."

uninstall-knative:
	@echo "Uninstalling Knative Serving..."
	kubectl delete -f iac/kubernetes/knative/serving-hpa.yaml
	kubectl delete -f iac/kubernetes/knative/serving-core.yaml
	kubectl delete -f iac/kubernetes/knative/serving-crds.yaml
	@echo "Knative Serving uninstalled." 