# Flowkit — top-level Makefile
# `make help` lists available targets.

GO              ?= go
# Match CI: module mode (repo vendor/ may be out of sync locally).
export GOFLAGS  ?= -mod=mod
PYTHON          ?= python3
VENV            ?= .venv
BIN_DIR         := bin
COVERAGE_FILE   := coverage.out
PKG_INTERNAL    := ./internal/...
PKG_CMD         := ./cmd/flowkit/...
PKG_ALL         := ./...
TEST_TIMEOUT    ?= 90s
TEST_RACE_FLAGS := -race -timeout $(TEST_TIMEOUT)
SDK_PY_DIR      := sdk/python

.DEFAULT_GOAL := help

## help: show available targets
help:
	@awk 'BEGIN {FS = ":.*?## "} /^## / {sub(/^## /,""); split($$0,a,": "); printf "\033[36m%-18s\033[0m %s\n", a[1], a[2]}' $(MAKEFILE_LIST) | sort

## tidy: go mod tidy
tidy:
	$(GO) mod tidy

## build: compile the flowkit binary into ./bin/
build: $(BIN_DIR)/flowkit

$(BIN_DIR)/flowkit: $(shell find cmd/flowkit internal -name '*.go' 2>/dev/null)
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/flowkit $(PKG_CMD)

## test: run all Go tests
test:
	$(GO) test $(PKG_ALL)

## test-race: run all Go tests with -race, with coverage (see also: test-race-ci)
test-race:
	$(GO) test $(TEST_RACE_FLAGS) -coverprofile=$(COVERAGE_FILE) -covermode=atomic $(PKG_ALL)

## test-race-ci: race tests with no coverage — atomic counters can hide races
test-race-ci:
	$(GO) test $(TEST_RACE_FLAGS) $(PKG_ALL)

## test-internal: just internal/ tests (fast)
test-internal:
	$(GO) test $(PKG_INTERNAL)

## build-check: stripped release build, then print its version
build-check:
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags="-s -w" -o $(BIN_DIR)/flowkit-release ./cmd/flowkit
	@$(BIN_DIR)/flowkit-release -version

## ci: mirror GitHub Actions go job (vet + race tests) — run before push
ci: check-guidelines vet test-race-ci build-check
	@echo "ci: passed (guidelines + vet + race + build)"

## ci-all: ci + golangci-lint (requires: brew install golangci-lint)
ci-all: ci lint
	@echo "ci-all: passed"

## sdk-test-ci: Python SDK tests with the test extras installed, as CI runs them
sdk-test-ci: venv
	$(VENV)/bin/pip install -q -e "$(SDK_PY_DIR)[test]"
	$(VENV)/bin/pytest $(SDK_PY_DIR)

## lint-ci: golangci-lint pinned to the CI version, run in Docker (advisory)
GOLANGCI_VERSION ?= v2.10.1
lint-ci:
	@docker run --rm -v "$(CURDIR)":/src -w /src \
	  golangci/golangci-lint:$(GOLANGCI_VERSION) golangci-lint run --timeout=5m \
	  || echo "lint-ci: findings are advisory — the lint job does not fail the build"

## preflight: every check GitHub Actions runs, in one command — run before push
preflight: ci sdk-test-ci lint-ci
	@echo "preflight: passed (go job + python sdk job + lint job)"

## preflight-linux: run the Go checks on Linux, where race timing differs from macOS
LINUX_IMAGE ?= golang:1.26
PLATFORM    ?=
preflight-linux:
	docker run --rm $(if $(PLATFORM),--platform $(PLATFORM),) \
	  -v "$(CURDIR)":/src -w /src \
	  -v "$(HOME)/go/pkg/mod":/go/pkg/mod \
	  -v flowkit-linux-gocache:/root/.cache/go-build \
	  -e GOFLAGS=-mod=mod \
	  $(LINUX_IMAGE) sh -c '\
	    go vet ./... && \
	    go test -race -timeout $(TEST_TIMEOUT) ./... && \
	    go build -ldflags="-s -w" -o /tmp/flowkit ./cmd/flowkit && \
	    /tmp/flowkit -version'

## sync-guidelines: regenerate editor rules from the contributor guidelines (maintainers only)
sync-guidelines:
	@if [ -f develop/sync-guidelines.sh ]; then bash develop/sync-guidelines.sh; \
	else echo "sync-guidelines: not available in this checkout"; fi

## check-guidelines: fail if editor rules are stale vs the contributor guidelines (skipped when absent)
check-guidelines:
	@if [ -f develop/sync-guidelines.sh ]; then bash develop/sync-guidelines.sh --check; \
	else echo "check-guidelines: skipped (no develop/sync-guidelines.sh)"; fi

## cover: print per-package coverage (after test-race)
cover: test-race
	$(GO) tool cover -func=$(COVERAGE_FILE) | tail -20

## vet: go vet
vet:
	$(GO) vet $(PKG_ALL)

## lint: run golangci-lint (install separately)
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "install: brew install golangci-lint"; exit 1; }
	golangci-lint run

## fmt: gofmt -w + goimports -w
fmt:
	gofmt -s -w cmd/flowkit internal sdk
	@command -v goimports >/dev/null 2>&1 && goimports -w cmd/flowkit internal sdk || true

## run: start flowkit with local config (loads .env.dev, else .env.local)
CONFIG ?= flowkit.yaml
ENV_FILE ?= $(if $(wildcard .env.dev),.env.dev,.env.local)
run: build
	@set -a && . ./$(ENV_FILE) && set +a && $(BIN_DIR)/flowkit -c $(CONFIG)

## venv: create Python venv if missing
venv:
	@[ -d $(VENV) ] || $(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip >/dev/null

## sdk-install: install SDK editable into venv
sdk-install: venv
	$(VENV)/bin/pip install -e $(SDK_PY_DIR)

## sdk-test: pytest the Python SDK
sdk-test: venv
	$(VENV)/bin/pytest $(SDK_PY_DIR)

## smoke: end-to-end smoke (build + start + python handshake)
smoke: build sdk-install
	@if [ -f develop/smoke.sh ]; then bash develop/smoke.sh; \
	else echo "smoke: not available in this checkout; run 'make e2e' instead"; fi

## e2e: Playwright headless browser tests (api_explorer)
e2e: build sdk-install
	$(VENV)/bin/pytest tests/e2e/test_api_explorer.py -v

## soak: Go-level concurrent-peer soak (5 sessions × ~2s each, no browser)
soak:
	$(GO) test -race -timeout 90s -run TestSoak ./internal/transport/webrtc/... -v

## sip-e2e: SIP transport end-to-end tests (inbound INVITE + outbound originate + soak)
sip-e2e:
	$(GO) test -race -timeout 120s -run "E2E|Outbound|Soak" ./internal/transport/sip/... -v

## capacity-s1: fixed-load SIP capacity test (requires running flowkit + SIP)
capacity-s1:
	bash tools/capacity/run.sh --profile profiles/local.yaml \
	  --mode fixed --scenarios s1 --fixed-n 10

## capacity-boundary: ramp SIP until boundary (requires running flowkit + SIP)
capacity-boundary:
	bash tools/capacity/run.sh --profile profiles/local.yaml \
	  --mode boundary --scenarios s1

## capacity-s2: fixed-load WebRTC capacity test (requires running flowkit WS)
capacity-s2:
	bash tools/capacity/run.sh --profile profiles/local.yaml \
	  --mode fixed --scenarios s2 --fixed-n 10

## capacity-quick: fast S1–S4 validation (~3–5 min)
capacity-quick:
	bash tools/capacity/run.sh --profile profiles/local-quick.yaml \
	  --mode boundary --scenarios s1,s2,s3,s4

## capacity-s3: fixed-load SIP+WebRTC bridge test
capacity-s3:
	bash tools/capacity/run.sh --profile profiles/local.yaml \
	  --mode fixed --scenarios s3 --fixed-n 5

## capacity-s4: fixed-load Mock CC workflow test
capacity-s4:
	bash tools/capacity/run.sh --profile profiles/local.yaml \
	  --mode fixed --scenarios s4 --fixed-n 5

## capacity-vertex: server press-to-limit (ignores scenario load.max)
capacity-vertex:
	bash tools/capacity/run.sh --profile profiles/server-vertex.yaml \
	  --mode boundary --scenarios all

## capacity-all: all four scenarios (dev mode, respects scenario load.max)
capacity-all:
	bash tools/capacity/run.sh --profile profiles/local.yaml \
	  --mode boundary --scenarios all

## resume: show next unchecked task + recent commits (maintainers only)
resume:
	@if [ -x develop/resume.sh ]; then ./develop/resume.sh; \
	else echo "resume: not available in this checkout"; fi

## docker-build: build the production Docker image
DOCKER_TAG ?= flowkit:m4
docker-build:
	docker build -t $(DOCKER_TAG) -f deploy/docker/Dockerfile .

## docker-run: run the freshly-built image (single instance, host port 9600; needs FLOWKIT_API_KEY)
docker-run: docker-build
	@test -n "$$FLOWKIT_API_KEY" || { echo "set FLOWKIT_API_KEY first, e.g. export FLOWKIT_API_KEY=\$$(openssl rand -hex 32)"; exit 1; }
	docker run --rm -p 9600:9600 -p 5060:5060/udp -e RPC_LISTEN=0.0.0.0:9600 -e FLOWKIT_API_KEY $(DOCKER_TAG)

## compose-up: start single-instance docker-compose stack (flowkit + jaeger)
compose-up:
	docker compose -f deploy/docker/docker-compose.single.yml up --build

## compose-down: stop the single-instance stack
compose-down:
	docker compose -f deploy/docker/docker-compose.single.yml down

## compose-multi: start 3-instance stack with nginx + jaeger
compose-multi:
	docker compose -f deploy/docker/docker-compose.multi.yml up --build

## compose-multi-down: stop the 3-instance stack
compose-multi-down:
	docker compose -f deploy/docker/docker-compose.multi.yml down

## clean: remove build/coverage/tmp artifacts
clean:
	rm -rf $(BIN_DIR) $(COVERAGE_FILE) coverage.html
	find . -name '*.test' -type f -delete

.PHONY: help tidy build test test-race test-race-ci test-internal build-check ci ci-all preflight preflight-linux lint-ci sdk-test-ci sync-guidelines check-guidelines cover vet lint fmt run venv sdk-install sdk-test smoke e2e soak sip-e2e capacity-s1 capacity-s2 capacity-s3 capacity-s4 capacity-boundary capacity-quick capacity-vertex capacity-all docker-build docker-run compose-up compose-down compose-multi compose-multi-down resume clean
