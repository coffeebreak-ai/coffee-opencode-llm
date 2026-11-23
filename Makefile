GO ?= go
BINARY ?= bin/coffee-opencode-llm
DATA_PATH ?= ./data
PID_FILE ?= $(DATA_PATH)/coffee-opencode-llm.pid
RETENTION_CONFIRM ?= no
RETENTION_BATCH_ROWS ?= 1000

.PHONY: build run stop test lint lint-fix lint-config mocks mocks-clean fmt vet migrate catalog retention retention-apply tidy clean

build:
	$(GO) build -trimpath -o $(BINARY) ./cmd/coffee-opencode-llm

run:
	$(GO) run ./cmd/coffee-opencode-llm serve

stop:
	@if [ -f "$(PID_FILE)" ]; then kill "$$(cat $(PID_FILE))" 2>/dev/null || true; rm -f "$(PID_FILE)"; fi

test:
	$(GO) test -race ./...

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix

lint-config:
	golangci-lint config verify
	mockery showconfig >/dev/null 2>&1

mocks:
	mockery

mocks-clean:
	rm -rf mocks

fmt:
	gofmt -l -w .

vet:
	$(GO) vet ./...

migrate:
	$(GO) run ./cmd/coffee-opencode-llm migrate

catalog:
	$(GO) run ./cmd/coffee-opencode-llm catalog show

retention:
	$(GO) run ./cmd/coffee-opencode-llm retention

retention-apply:
	@if [ "$(RETENTION_CONFIRM)" != "yes" ]; then \
		echo "retention-apply requires RETENTION_CONFIRM=yes"; \
		$(GO) run ./cmd/coffee-opencode-llm retention; \
		exit 1; \
	fi
	$(GO) run ./cmd/coffee-opencode-llm retention --apply -batch-rows $(RETENTION_BATCH_ROWS)

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin
