#!/bin/bash
set -e

# Simple crash recovery stress test using perftest
# Tests that durable messages survive server crash
# Usage: ./scripts/low_stress_test.sh [message_count]

MESSAGE_COUNT=${1:-1000}
STORAGE_PATH="/tmp/amqp-low-stress"
QUEUE_NAME="stress_queue"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=================================================="
echo "AMQP Crash Recovery Stress Test (Simple)"
echo "=================================================="
echo "Message Count: $MESSAGE_COUNT"
echo "Queue: $QUEUE_NAME"
echo ""

# Step 1: Cleanup
echo "[1/6] Cleaning up..."
pkill -9 amqp-server || true
pkill -9 perftest || true
rm -rf "$STORAGE_PATH" || true
sleep 1

# Step 2: Build server
echo "[2/6] Building server..."
cd "$PROJECT_DIR"
go build -o amqp-server ./cmd/amqp-server
if [ $? -ne 0 ]; then
    echo "ERROR: Server build failed"
    exit 1
fi

# Step 3: Start server (first session)
echo "[3/6] Starting server (first session)..."
./amqp-server --storage-path "$STORAGE_PATH" > /tmp/low-stress.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 2

# Verify server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    cat /tmp/low-stress.log
    exit 1
fi

# Step 4: Publish messages with perftest
echo "[4/6] Publishing $MESSAGE_COUNT durable messages with perftest..."
cd "$PROJECT_DIR/benchmark"
timeout 60 ./perftest \
    -producers 1 \
    -consumers 0 \
    -queue "$QUEUE_NAME" \
    -count $MESSAGE_COUNT \
    -size 100 \
    2>&1 | grep -E "(Published|Consumed|Error)" || true

sleep 2

# Check WAL and metadata
echo ""
echo "Checking storage files..."
ls -lh "$STORAGE_PATH/wal/shared/" 2>/dev/null || echo "No WAL files"
ls -lh "$STORAGE_PATH/metadata/queues/" 2>/dev/null || echo "No queue metadata"

# Step 5: CRASH! Force kill server
echo ""
echo "[5/6] CRASH! Force killing server (kill -9)..."
kill -9 $SERVER_PID
sleep 1

# Verify server is dead
if kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server still running after kill -9"
    exit 1
fi
echo "✓ Server killed"

# Step 6: Restart and check recovery
echo "[6/6] Restarting server and checking recovery..."
./amqp-server --storage-path "$STORAGE_PATH" > /tmp/low-stress-recovery.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 3

# Verify server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to restart"
    cat /tmp/low-stress-recovery.log
    exit 1
fi

# Check recovery logs
echo ""
echo "Recovery logs:"
grep -E "recovery|recovered|messages" /tmp/low-stress-recovery.log | head -20

# Cleanup
kill $SERVER_PID 2>/dev/null || true

echo ""
echo "=================================================="
echo "MANUAL VERIFICATION REQUIRED"
echo "=================================================="
echo "Please check:"
echo "  1. WAL files exist: ls -lh $STORAGE_PATH/wal/shared/"
echo "  2. Queue metadata exists: ls -lh $STORAGE_PATH/metadata/queues/"
echo "  3. Recovery logs show messages recovered"
echo ""
echo "Logs:"
echo "  - /tmp/low-stress.log (first session)"
echo "  - /tmp/low-stress-recovery.log (recovery session)"
echo ""
