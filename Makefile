.PHONY: test clean-docker setup-docker teardown-docker

# Main test command
test: clean-docker setup-docker run-tests teardown-docker

# Bring down any running containers (gracefully handle if not running)
clean-docker:
	@echo "Stopping any running containers..."
	docker compose down 2>/dev/null || true

# Start the PostgreSQL container in the background
setup-docker:
	@echo "Starting PostgreSQL container..."
	docker compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3  # Give PostgreSQL a moment to initialize

# Run the tests
run-tests:
	@echo "Running tests..."
	uv run pytest test

# Tear down the containers
teardown-docker:
	@echo "Cleaning up containers..."
	docker compose down