#!/bin/bash

# StrangeQ - Launch Server with Full Monitoring & Load Test
# This script starts the AMQP server, Prometheus, Grafana, and runs a performance test

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MONITORING_DIR="$SCRIPT_DIR/monitoring"
AMQP_DIR="$REPO_ROOT/src/amqp-go"
BENCHMARK_DIR="$AMQP_DIR/benchmark"

# Configuration
PUBLISHERS=${PUBLISHERS:-10}
CONSUMERS=${CONSUMERS:-10}
RATE=${RATE:-1000}
DURATION=${DURATION:-300s}
TELEMETRY_PORT=${TELEMETRY_PORT:-9419}
AMQP_PORT=${AMQP_PORT:-5672}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    
    # Kill AMQP server if running
    if [ ! -z "$AMQP_PID" ]; then
        echo "Stopping AMQP server (PID: $AMQP_PID)..."
        kill $AMQP_PID 2>/dev/null || true
    fi
    
    # Kill perftest if running
    if [ ! -z "$PERFTEST_PID" ]; then
        echo "Stopping performance test (PID: $PERFTEST_PID)..."
        kill $PERFTEST_PID 2>/dev/null || true
    fi
    
    echo -e "${GREEN}Cleanup complete. Monitoring stack is still running.${NC}"
    echo -e "${BLUE}View metrics at: http://localhost:3000${NC}"
    echo -e "${BLUE}To stop monitoring: cd $SCRIPT_DIR && ./stop-monitoring.sh${NC}"
}

trap cleanup EXIT INT TERM

print_header() {
    echo -e "${BLUE}"
    echo "=========================================="
    echo "$1"
    echo "=========================================="
    echo -e "${NC}"
}

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_dependencies() {
    print_header "Checking Dependencies"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    print_info "✓ Docker found"
    
    # Check docker-compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "docker-compose is not installed. Please install docker-compose first."
        exit 1
    fi
    print_info "✓ docker-compose found"
    
    # Check if AMQP server binary exists
    if [ ! -f "$AMQP_DIR/bin/amqp-server" ]; then
        print_warn "AMQP server binary not found. Building..."
        cd "$AMQP_DIR"
        go build -o bin/amqp-server ./cmd/amqp-server
        print_info "✓ AMQP server built"
    else
        print_info "✓ AMQP server binary found"
    fi
    
    # Check if perftest binary exists
    if [ ! -f "$BENCHMARK_DIR/perftest" ]; then
        print_warn "perftest binary not found. Building..."
        cd "$BENCHMARK_DIR"
        go build -o perftest perftest.go
        print_info "✓ perftest built"
    else
        print_info "✓ perftest binary found"
    fi
}

start_monitoring() {
    print_header "Starting Monitoring Stack"
    
    cd "$MONITORING_DIR"
    
    # Create directories if they don't exist
    mkdir -p grafana-dashboards grafana-datasources
    
    # Check if containers are already running
    if docker ps | grep -q amqp-prometheus; then
        print_warn "Prometheus already running, restarting..."
        docker-compose restart prometheus
    else
        print_info "Starting Prometheus..."
        docker-compose up -d prometheus
    fi
    
    if docker ps | grep -q amqp-grafana; then
        print_warn "Grafana already running, restarting..."
        docker-compose restart grafana
    else
        print_info "Starting Grafana..."
        docker-compose up -d grafana
    fi
    
    # Wait for services to be ready
    print_info "Waiting for Prometheus to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:9090/-/ready > /dev/null 2>&1; then
            print_info "✓ Prometheus is ready"
            break
        fi
        sleep 1
    done
    
    print_info "Waiting for Grafana to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
            print_info "✓ Grafana is ready"
            break
        fi
        sleep 1
    done
    
    print_info "Monitoring stack started successfully"
    print_info "  Prometheus: http://localhost:9090"
    print_info "  Grafana:    http://localhost:3000 (admin/admin)"
}

start_amqp_server() {
    print_header "Starting AMQP Server with Telemetry"
    
    cd "$AMQP_DIR"
    
    # Check if port is already in use
    if lsof -Pi :$AMQP_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        print_error "Port $AMQP_PORT is already in use. Please stop the existing AMQP server."
        exit 1
    fi
    
    # Start AMQP server with telemetry
    print_info "Starting server on port $AMQP_PORT with telemetry on port $TELEMETRY_PORT..."
    ./bin/amqp-server --enable-telemetry --telemetry-port $TELEMETRY_PORT > /tmp/amqp-server.log 2>&1 &
    AMQP_PID=$!
    
    # Wait for server to be ready
    print_info "Waiting for AMQP server to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:$TELEMETRY_PORT/health > /dev/null 2>&1; then
            print_info "✓ AMQP server is ready (PID: $AMQP_PID)"
            break
        fi
        if ! kill -0 $AMQP_PID 2>/dev/null; then
            print_error "AMQP server failed to start. Check /tmp/amqp-server.log for details."
            cat /tmp/amqp-server.log
            exit 1
        fi
        sleep 1
    done
    
    print_info "Server logs: /tmp/amqp-server.log"
}

run_performance_test() {
    print_header "Running Performance Test"
    
    print_info "Configuration:"
    print_info "  Publishers: $PUBLISHERS"
    print_info "  Consumers:  $CONSUMERS"
    print_info "  Rate:       $RATE msg/s"
    print_info "  Duration:   $DURATION"
    
    cd "$BENCHMARK_DIR"
    
    print_info "Starting performance test..."
    print_info "Performance test logs: /tmp/perftest.log"
    
    ./perftest \
        -publishers $PUBLISHERS \
        -consumers $CONSUMERS \
        -rate $RATE \
        -duration $DURATION \
        -host localhost:$AMQP_PORT \
        > /tmp/perftest.log 2>&1 &
    PERFTEST_PID=$!
    
    print_info "Performance test started (PID: $PERFTEST_PID)"
}

show_dashboard_info() {
    print_header "🎉 All Systems Running!"
    
    echo -e "${GREEN}"
    echo "Your AMQP server is now running with full observability!"
    echo ""
    echo "📊 Dashboards:"
    echo "   Grafana:    http://localhost:3000 (admin/admin)"
    echo "   Prometheus: http://localhost:9090"
    echo "   Metrics:    http://localhost:$TELEMETRY_PORT/metrics"
    echo ""
    echo "🚀 Performance Test:"
    echo "   Publishers:  $PUBLISHERS"
    echo "   Consumers:   $CONSUMERS"
    echo "   Rate:        $RATE msg/s"
    echo "   Duration:    $DURATION"
    echo ""
    echo "📝 Logs:"
    echo "   Server:      tail -f /tmp/amqp-server.log"
    echo "   Perftest:    tail -f /tmp/perftest.log"
    echo ""
    echo "💡 Tips:"
    echo "   - Open Grafana to see real-time metrics"
    echo "   - The dashboard auto-refreshes every 5 seconds"
    echo "   - Press Ctrl+C to stop everything"
    echo -e "${NC}"
}

monitor_progress() {
    print_header "Monitoring Test Progress"
    
    echo -e "${BLUE}Tailing performance test logs... (Ctrl+C to stop)${NC}"
    echo ""
    
    # Follow the perftest log
    tail -f /tmp/perftest.log 2>/dev/null || true
}

main() {
    print_header "StrangeQ - Launch with Monitoring"
    
    # Check dependencies
    check_dependencies
    
    # Start monitoring stack
    start_monitoring
    
    # Start AMQP server
    start_amqp_server
    
    # Wait a bit for metrics to start flowing
    sleep 3
    
    # Run performance test
    run_performance_test
    
    # Show info
    show_dashboard_info
    
    # Monitor progress
    monitor_progress
    
    # Wait for perftest to complete
    if [ ! -z "$PERFTEST_PID" ]; then
        wait $PERFTEST_PID 2>/dev/null || true
    fi
    
    print_info "Performance test completed!"
    print_info "Monitoring stack is still running. Visit http://localhost:3000 to review metrics."
    print_info "Press Ctrl+C to stop the AMQP server and exit."
    
    # Keep server running
    if [ ! -z "$AMQP_PID" ]; then
        wait $AMQP_PID 2>/dev/null || true
    fi
}

# Run main function
main

