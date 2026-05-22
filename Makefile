.PHONY: lint test test-integration build run-api run-worker run-scheduler migrate-up migrate-down infra-up infra-down

# Load environment variables from .env (if it exists) and export them to all
# subprocesses spawned by make. Production deployments use orchestrator-provided
# environment instead — this file is for local dev only.
-include .env
export

GOOSE_DIR := internal/store/migrations

# Fall back to a local-dev URL when DATABASE_URL is not provided by .env.
ifeq ($(strip $(DATABASE_URL)),)
DATABASE_URL := postgres://webhookd:webhookd@localhost:5432/webhookd?sslmode=disable
endif

lint:
	golangci-lint run ./...

test:
	go test -race -short ./...

test-integration:
	go test -race -tags integration ./tests/integration/...

build:
	go build -o bin/webhookd ./cmd/webhookd

run-api: build
	./bin/webhookd api

run-worker: build
	./bin/webhookd worker

run-scheduler: build
	./bin/webhookd scheduler

migrate-up:
	goose -dir $(GOOSE_DIR) postgres "$(DATABASE_URL)" up

migrate-down:
	goose -dir $(GOOSE_DIR) postgres "$(DATABASE_URL)" down

infra-up:
	docker compose up -d
	@echo "Waiting for Postgres..."
	@until docker compose exec -T postgres pg_isready -U webhookd >/dev/null 2>&1; do sleep 1; done
	@echo "Infra ready."

infra-down:
	docker compose down
