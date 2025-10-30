.PHONY: help build test bench clean install lint fmt docker-build docker-run release-local

# Variables
BINARY_NAME=amqp-server
SRC_DIR=src/amqp-go
MAIN_PKG=./cmd/amqp-server
VERSION?=dev
BUILD_DIR=build
DOCKER_IMAGE=amqp-go
PLATFORMS=darwin/amd64 darwin/arm64 linux/amd64 linux/arm64 linux/386 windows/amd64 windows/386

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

# Build flags
LDFLAGS=-ldflags "-s -w -X main.version=$(VERSION)"

help: ## Show this help
	@echo "AMQP-Go Build System"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build the binary
	@echo "Building $(BINARY_NAME)..."
	cd $(SRC_DIR) && $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) $(MAIN_PKG)
	@echo "Build complete: $(SRC_DIR)/$(BINARY_NAME)"

build-all: ## Build for all platforms
	@echo "Building for all platforms..."
	@mkdir -p $(BUILD_DIR)
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} GOARCH=$${platform#*/} ; \
		output=$(BUILD_DIR)/$(BINARY_NAME)-$$GOOS-$$GOARCH ; \
		if [ "$$GOOS" = "windows" ]; then output="$$output.exe"; fi ; \
		echo "Building $$platform -> $$output" ; \
		cd $(SRC_DIR) && GOOS=$$GOOS GOARCH=$$GOARCH $(GOBUILD) $(LDFLAGS) -o ../../$$output $(MAIN_PKG) ; \
	done
	@echo "All builds complete in $(BUILD_DIR)/"

test: ## Run tests
	@echo "Running tests..."
	cd $(SRC_DIR) && $(GOTEST) -v -race -coverprofile=coverage.out ./...

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	cd $(SRC_DIR) && $(GOTEST) -v -race -coverprofile=coverage.out -covermode=atomic ./...
	cd $(SRC_DIR) && $(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: $(SRC_DIR)/coverage.html"

bench: ## Run benchmarks
	@echo "Running benchmarks..."
	cd $(SRC_DIR) && $(GOTEST) -bench=. -benchmem ./...

bench-protocol: ## Run protocol benchmarks
	@echo "Running protocol benchmarks..."
	cd $(SRC_DIR) && $(GOTEST) -bench=. -benchmem ./protocol/

clean: ## Clean build artifacts
	@echo "Cleaning..."
	rm -f $(SRC_DIR)/$(BINARY_NAME)
	rm -rf $(BUILD_DIR)
	rm -f $(SRC_DIR)/coverage.out $(SRC_DIR)/coverage.html
	rm -f $(SRC_DIR)/*.test
	@echo "Clean complete"

install: ## Install binary to $GOPATH/bin
	@echo "Installing $(BINARY_NAME)..."
	cd $(SRC_DIR) && $(GOBUILD) $(LDFLAGS) -o $(GOPATH)/bin/$(BINARY_NAME) $(MAIN_PKG)
	@echo "Installed to $(GOPATH)/bin/$(BINARY_NAME)"

lint: ## Run linters
	@echo "Running linters..."
	cd $(SRC_DIR) && golangci-lint run --timeout=5m

fmt: ## Format code
	@echo "Formatting code..."
	cd $(SRC_DIR) && $(GOFMT) ./...
	cd $(SRC_DIR) && $(GOCMD) mod tidy
	@echo "Format complete"

vet: ## Run go vet
	@echo "Running go vet..."
	cd $(SRC_DIR) && $(GOVET) ./...

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	cd $(SRC_DIR) && $(GOMOD) download
	@echo "Dependencies downloaded"

deps-update: ## Update dependencies
	@echo "Updating dependencies..."
	cd $(SRC_DIR) && $(GOMOD) tidy
	cd $(SRC_DIR) && $(GOGET) -u ./...
	@echo "Dependencies updated"

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	cd $(SRC_DIR) && docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .
	@echo "Docker image built: $(DOCKER_IMAGE):$(VERSION)"

docker-run: ## Run Docker container
	@echo "Running Docker container..."
	docker run -p 5672:5672 $(DOCKER_IMAGE):latest

docker-push: ## Push Docker image
	@echo "Pushing Docker image..."
	docker push $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):latest

release-local: clean build-all ## Build release locally
	@echo "Creating release artifacts..."
	@cd $(BUILD_DIR) && for f in *; do \
		echo "Creating checksum for $$f" ; \
		shasum -a 256 "$$f" > "$$f.sha256" ; \
	done
	@echo "Release artifacts ready in $(BUILD_DIR)/"

run: ## Run the server
	@echo "Starting AMQP server..."
	cd $(SRC_DIR) && $(GOBUILD) -o $(BINARY_NAME) $(MAIN_PKG) && ./$(BINARY_NAME)

dev: ## Run in development mode
	@echo "Starting AMQP server in dev mode..."
	cd $(SRC_DIR) && $(GOBUILD) -o $(BINARY_NAME) $(MAIN_PKG) && ./$(BINARY_NAME) --log-level debug

check: fmt vet lint test ## Run all checks

all: clean deps build test ## Clean, download deps, build, and test

.DEFAULT_GOAL := help

