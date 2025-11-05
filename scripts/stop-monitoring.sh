#!/bin/bash

# Stop monitoring stack

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITORING_DIR="$SCRIPT_DIR/monitoring"

echo "Stopping monitoring stack..."
cd "$MONITORING_DIR"
docker-compose down

echo ""
echo "✓ Monitoring stack stopped."
echo ""
echo "To remove all data volumes (metrics history), run:"
echo "  cd $MONITORING_DIR && docker-compose down -v"

