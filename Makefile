# Portage developer tasks. `make help` lists targets.

GO              ?= go
COMPOSE         ?= docker compose -f deploy/docker-compose.yml
GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT   ?= $(shell command -v golangci-lint 2>/dev/null || echo "$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)")

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.DEFAULT_GOAL := help
.PHONY: help build loadgen test itest up down lint tidy

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build bin/portage
	@test -d cmd/portage || { echo "build: cmd/portage does not exist yet" >&2; exit 1; }
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/portage ./cmd/portage

loadgen: ## Build bin/portage-loadgen (bench load generator)
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/portage-loadgen ./bench/cmd/portage-loadgen

test: ## Run unit tests
	$(GO) test -race ./...

up: ## Start local emulators (Postgres, Azurite, SeaweedFS)
	$(COMPOSE) up -d --wait

down: ## Stop local emulators and remove their volumes
	$(COMPOSE) down -v

itest: up ## Run unit + integration tests against the emulators
	$(GO) test -race -tags integration ./...

lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run ./...

tidy: ## Tidy go.mod/go.sum
	$(GO) mod tidy
