#!/bin/bash

# Run RabbitMQ interoperability tests
# This script starts RabbitMQ using Docker, runs the tests, and cleans up.

set -e

echo "==> Starting RabbitMQ with Docker..."
docker-compose up -d

echo "==> Waiting for RabbitMQ to be ready..."
max_wait=30
elapsed=0
while ! docker exec rabbitmq-test rabbitmq-diagnostics ping >/dev/null 2>&1; do
    if [ $elapsed -ge $max_wait ]; then
        echo "Error: RabbitMQ failed to start within ${max_wait} seconds"
        docker-compose logs
        docker-compose down
        exit 1
    fi
    echo "Waiting for RabbitMQ... ($elapsed/$max_wait)"
    sleep 2
    elapsed=$((elapsed + 2))
done

echo "==> RabbitMQ is ready!"
echo "==> Running RabbitMQ interoperability tests..."
echo ""

# Run the tests
if go test -v -run TestRabbitMQ; then
    echo ""
    echo "==> All tests passed!"
    exit_code=0
else
    echo ""
    echo "==> Tests failed!"
    exit_code=1
fi

echo ""
echo "==> Stopping RabbitMQ..."
docker-compose down

echo "==> Management UI was available at: http://localhost:15672 (guest/guest)"
echo ""

exit $exit_code
