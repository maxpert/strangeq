#!/bin/bash

# Build script for AMQP-Go Server

set -e  # Exit immediately if a command exits with a non-zero status

# Project variables
PROJECT_NAME="amqp-go"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/src/amqp-go"
BIN_DIR="${PROJECT_DIR}/src/amqp-go/bin"
BINARY_NAME="${PROJECT_NAME}"

# Create bin directory if it doesn't exist
mkdir -p "${BIN_DIR}"

# Build the binary
echo "Building ${PROJECT_NAME}..."
cd "${BUILD_DIR}"
go build -o "${BIN_DIR}/${BINARY_NAME}" .

echo "Build completed successfully!"
echo "Binary location: ${BIN_DIR}/${BINARY_NAME}"

# Optionally, run tests before building
if [ "$1" == "--test" ]; then
    echo "Running tests..."
    go test ./...
fi

# Optionally, clean up previous builds
if [ "$1" == "--clean" ] || [ "$2" == "--clean" ]; then
    echo "Cleaning up previous builds..."
    rm -f "${BIN_DIR}/${BINARY_NAME}"*
fi

echo "Build script completed."