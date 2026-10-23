#!/bin/bash

# Build script for AMQP-Go Server

set -e  # Exit immediately if a command exits with a non-zero status

# Project variables
PROJECT_NAME="amqp-go"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/src/amqp-go"
BIN_DIR="${PROJECT_DIR}/src/amqp-go/bin"
BINARY_NAME="amqp-server"

# Create bin directory if it doesn't exist
mkdir -p "${BIN_DIR}"

# Optionally clean previous builds BEFORE building (this used to run after the
# build, deleting the binary it had just produced).
if [ "$1" == "--clean" ] || [ "$2" == "--clean" ]; then
    echo "Cleaning up previous builds..."
    rm -f "${BIN_DIR}/${BINARY_NAME}"*
fi

# Build the binary. The module root holds only test-only files, so build the
# command package. Version identity is stamped via -X (variables, not consts).
echo "Building ${BINARY_NAME}..."
cd "${BUILD_DIR}"
go build \
    -ldflags "-X main.version=${VERSION:-0.1.0-dev} -X main.commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o "${BIN_DIR}/${BINARY_NAME}" ./cmd/amqp-server

echo "Build completed successfully!"
echo "Binary location: ${BIN_DIR}/${BINARY_NAME}"

# Optionally, run tests before building
if [ "$1" == "--test" ]; then
    echo "Running tests..."
    go test ./...
fi

echo "Build script completed."