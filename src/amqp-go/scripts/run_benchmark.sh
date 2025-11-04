#!/bin/bash
set -e

# Benchmark automation script for AMQP server performance testing
# Usage: ./scripts/run_benchmark.sh [version] [producers] [consumers] [duration]
# Example: ./scripts/run_benchmark.sh p1.2 50 50 30s

VERSION=${1:-test}
PRODUCERS=${2:-50}
CONSUMERS=${3:-50}
DURATION=${4:-30s}
PREFETCH=${5:-100}
SIZE=${6:-1024}
STORAGE_PATH="/tmp/amqp-storage-$VERSION"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="/tmp/benchmark_results"
PROFILE_DIR="$RESULTS_DIR/$VERSION"

echo "=================================================="
echo "AMQP Server Benchmark - Version: $VERSION"
echo "=================================================="
echo "Producers: $PRODUCERS"
echo "Consumers: $CONSUMERS"
echo "Duration: $DURATION"
echo "Prefetch: $PREFETCH"
echo "Size: $SIZE bytes"
echo ""

# Step 1: Cleanup
echo "[1/8] Cleaning up existing processes..."
pkill -9 amqp-server || true
pkill -9 perftest || true
rm -rf /tmp/amqp-storage-* || true
sleep 1

# Step 2: Build
echo "[2/8] Building server..."
cd "$PROJECT_DIR"
go build -o amqp-server ./cmd/amqp-server
if [ $? -ne 0 ]; then
    echo "ERROR: Build failed"
    exit 1
fi

# Step 3: Create results directory
echo "[3/8] Creating results directory..."
mkdir -p "$PROFILE_DIR"

# Step 4: Start server with telemetry and persistence enabled
echo "[4/8] Starting server with telemetry and persistence enabled..."
./amqp-server --storage badger --storage-path "$STORAGE_PATH" --enable-telemetry --telemetry-port 9419 > "$PROFILE_DIR/server.log" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 2

# Verify server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    cat "$PROFILE_DIR/server.log"
    exit 1
fi

# Step 5: Start CPU profiling (in background)
echo "[5/8] Starting CPU profiling (${DURATION})..."
# Extract numeric duration for profiling
DURATION_SECS=$(echo "$DURATION" | sed 's/[^0-9]*//g')
curl -s -o "$PROFILE_DIR/cpu.prof" "http://localhost:9419/debug/pprof/profile?seconds=$DURATION_SECS" &
CURL_PID=$!

# Give profiling a moment to start
sleep 1

# Step 6: Run benchmark
echo "[6/8] Running benchmark..."
cd "$PROJECT_DIR/benchmark"
./perftest \
    -producers $PRODUCERS \
    -consumers $CONSUMERS \
    -duration $DURATION \
    -size $SIZE \
    -prefetch $PREFETCH \
    2>&1 | tee "$PROFILE_DIR/benchmark.txt" &

PERFTEST_PID=$!
echo "Perftest PID: $PERFTEST_PID"

# Wait up to 40 seconds for perftest to complete
WAIT_COUNT=0
while kill -0 $PERFTEST_PID 2>/dev/null && [ $WAIT_COUNT -lt 40 ]; do
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

# Force kill if still running
if kill -0 $PERFTEST_PID 2>/dev/null; then
    echo "Perftest exceeded 40s timeout, force killing..."
    pkill -9 perftest
    BENCHMARK_EXIT_CODE=124
else
    wait $PERFTEST_PID
    BENCHMARK_EXIT_CODE=$?
fi

# Step 7: Capture all profiles
echo "[7/8] Capturing all profiles from telemetry endpoint..."
wait $CURL_PID  # Wait for CPU profiling to finish
curl -s -o "$PROFILE_DIR/heap.prof" "http://localhost:9419/debug/pprof/heap"
curl -s -o "$PROFILE_DIR/allocs.prof" "http://localhost:9419/debug/pprof/allocs"
curl -s -o "$PROFILE_DIR/mutex.prof" "http://localhost:9419/debug/pprof/mutex"
curl -s -o "$PROFILE_DIR/block.prof" "http://localhost:9419/debug/pprof/block"
curl -s -o "$PROFILE_DIR/goroutine.prof" "http://localhost:9419/debug/pprof/goroutine"
curl -s -o "$PROFILE_DIR/threadcreate.prof" "http://localhost:9419/debug/pprof/threadcreate"

# Step 8: Cleanup and results
echo "[8/8] Cleaning up..."
kill $SERVER_PID 2>/dev/null || true
sleep 1

echo ""
echo "=================================================="
echo "BENCHMARK COMPLETE - Version: $VERSION"
echo "=================================================="

# Extract and display results
if [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
    echo ""
    grep -A 5 "=== Performance Test Results ===" "$PROFILE_DIR/benchmark.txt" || echo "No results found"

    # Extract throughput numbers
    CONSUMED=$(grep "Consumed:" "$PROFILE_DIR/benchmark.txt" | awk '{print $2, $3}')
    PUBLISHED=$(grep "Published:" "$PROFILE_DIR/benchmark.txt" | awk '{print $2, $3}')

    echo ""
    echo "Quick Stats:"
    echo "  Published: $PUBLISHED"
    echo "  Consumed: $CONSUMED"
else
    echo "ERROR: Benchmark failed with exit code $BENCHMARK_EXIT_CODE"
    echo "Check logs at: $PROFILE_DIR/server.log"
fi

echo ""
echo "Results saved to: $PROFILE_DIR"
echo "  - benchmark.txt (full output)"
echo "  - server.log (server logs)"
echo "  - cpu.prof (CPU profile)"
echo "  - heap.prof (heap profile)"
echo "  - allocs.prof (allocations profile)"
echo "  - mutex.prof (mutex contention profile)"
echo "  - block.prof (blocking profile)"
echo "  - goroutine.prof (goroutine profile)"
echo "  - threadcreate.prof (thread creation profile)"
echo ""
echo "Analyze with:"
echo "  go tool pprof -top -cum $PROFILE_DIR/cpu.prof"
echo "  go tool pprof -top -sample_index=alloc_space $PROFILE_DIR/allocs.prof"
echo "  go tool pprof -top $PROFILE_DIR/mutex.prof"
echo "  go tool pprof -top $PROFILE_DIR/block.prof"
echo "  go tool pprof -top $PROFILE_DIR/goroutine.prof"
echo ""

exit $BENCHMARK_EXIT_CODE
