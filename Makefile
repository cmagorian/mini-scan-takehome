.PHONY: build test clean docker-up docker-down fmt vet help

# Default target
all: build test

# Build the binaries
build:
	go build -o processor ./cmd/processor/main.go
	go build -o scanner ./cmd/scanner/main.go

# Run pkg tests
test:
	go test -v ./pkg/...

# Clean up binaries and database
clean:
	rm -f processor scanner scans.db

# Run with docker-compose
docker-up: clean
	docker compose up --build

# Stop docker-compose
docker-down:
	docker compose down

# Run end-to-end tests
test-e2e:
	@echo "Running E2E tests..."
	@# Ensure pubsub emulator is running
	@docker compose up -d pubsub
	@# Wait for pubsub to be healthy
	@for i in {1..10}; do curl -s http://localhost:8085 && break || sleep 1; done
	PUBSUB_EMULATOR_HOST=localhost:8085 go test -v ./tests/e2e/...
	@docker compose stop pubsub

# Format code
fmt:
	go fmt ./...

# Run vet
vet:
	go vet ./...

# Show help
help:
	@echo "Available targets:"
	@echo "  build       - Build the processor and scanner binaries"
	@echo "  test        - Run all unit tests"
	@echo "  test-e2e    - Run end-to-end tests using docker-compose"
	@echo "  clean       - Remove binaries and scans.db"
	@echo "  docker-up   - Build and start the environment using docker-compose"
	@echo "  docker-down - Stop and remove docker-compose containers"
	@echo "  fmt         - Format Go code"
	@echo "  vet         - Run go vet"
	@echo "  help        - Show this help message"
