.PHONY: proto build run test clean docker-build docker-run

# Proto generation
proto:
	@echo "Generating protobuf files..."
	python -m grpc_tools.protoc \
		-I./proto \
		--python_out=./app/grpc/generated \
		--grpc_python_out=./app/grpc/generated \
		./proto/settlement.proto

# Build Docker image
docker-build:
	docker build -t settlement-coordinator:latest .

# Run with docker-compose
docker-run:
	docker-compose up -d

# Stop docker-compose
docker-stop:
	docker-compose down

# View logs
logs:
	docker-compose logs -f settlement-coordinator

# Run tests
test:
	pytest tests/ -v

# Run locally (development)
run:
	python -m app.main

# Install dependencies
install:
	pip install -r requirements.txt

# Clean generated files
clean:
	rm -rf app/grpc/generated/*.py
	rm -rf __pycache__
	rm -rf .pytest_cache
	find . -name "*.pyc" -delete

# Format code
format:
	black app/
	isort app/

# Lint code
lint:
	flake8 app/
	mypy app/

# Generate gRPC stub from proto
generate-grpc-stub:
	@mkdir -p app/grpc/generated
	@touch app/grpc/generated/__init__.py
	python -m grpc_tools.protoc \
		-I./proto \
		--python_out=./app/grpc/generated \
		--grpc_python_out=./app/grpc/generated \
		./proto/settlement.proto

# Development setup
dev-setup: install generate-grpc-stub
	@echo "Development environment ready!"

# Run Ignite locally
ignite-local:
	docker run -d \
		--name settlement-ignite-local \
		-p 10800:10800 \
		-p 11211:11211 \
		-e IGNITE_QUIET=false \
		apacheignite/ignite:2.14.0

# Health check
health:
	curl -f http://localhost:8092/health || exit 1

# Create sample settlement
sample-settlement:
	@echo "Creating sample settlement..."
	curl -X POST http://localhost:8092/api/v1/settlements \
		-H "Content-Type: application/json" \
		-d '{ \
			"trade_id": "trade-123", \
			"buyer_id": "buyer-456", \
			"seller_id": "seller-789", \
			"provider_id": "provider-001", \
			"resource_type": "cpu", \
			"quantity": 100, \
			"unit_price": 0.05, \
			"total_value": 5.0, \
			"trade_timestamp": "2024-01-01T00:00:00Z", \
			"delivery_start": "2024-01-01T01:00:00Z", \
			"delivery_end": "2024-01-02T01:00:00Z" \
		}' 