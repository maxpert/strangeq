#!/bin/bash
set -e

# Crash Recovery Stress Test
# Tests WAL recovery by force-killing server and verifying message recovery
# Usage: ./scripts/crash_test.sh [message_count]
# Example: ./scripts/crash_test.sh 10000

MESSAGE_COUNT=${1:-1000}
STORAGE_PATH="/tmp/amqp-crash-test"
QUEUE_NAME="crash_test_queue"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_TOOL="$PROJECT_DIR/crash_test_tool"

echo "=================================================="
echo "AMQP Crash Recovery Test"
echo "=================================================="
echo "Message Count: $MESSAGE_COUNT"
echo "Queue: $QUEUE_NAME"
echo ""

# Step 1: Cleanup
echo "[1/7] Cleaning up..."
pkill -9 amqp-server || true
rm -rf "$STORAGE_PATH" || true
sleep 1

# Step 2: Build server
echo "[2/7] Building server..."
cd "$PROJECT_DIR"
go build -o amqp-server ./cmd/amqp-server
if [ $? -ne 0 ]; then
    echo "ERROR: Server build failed"
    exit 1
fi

# Step 3: Build test tool
echo "[3/7] Building crash test tool..."
go build -o crash_test_tool ./cmd/crash_test_tool
if [ $? -ne 0 ]; then
    echo "ERROR: Test tool build failed"
    exit 1
fi

# Step 4: Start server (first session)
echo "[4/7] Starting server (first session)..."
./amqp-server --storage-path "$STORAGE_PATH" > /tmp/amqp-crash-test.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 2

# Verify server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    cat /tmp/amqp-crash-test.log
    exit 1
fi

# Step 5: Publish messages
echo "[5/7] Publishing $MESSAGE_COUNT durable messages..."
$TEST_TOOL publish --count $MESSAGE_COUNT --queue "$QUEUE_NAME"
PUBLISH_EXIT=$?

if [ $PUBLISH_EXIT -ne 0 ]; then
    echo "ERROR: Failed to publish messages"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo "✓ Published $MESSAGE_COUNT messages successfully"

# Step 6: CRASH! Force kill server
echo "[6/7] CRASH! Force killing server (kill -9)..."
kill -9 $SERVER_PID
sleep 1

# Verify server is dead
if kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server still running after kill -9"
    exit 1
fi
echo "✓ Server killed"

# Step 7: Restart and verify recovery
echo "[7/7] Restarting server and verifying recovery..."
./amqp-server --storage-path "$STORAGE_PATH" > /tmp/amqp-crash-test-recovery.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 3

# Verify server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to restart"
    cat /tmp/amqp-crash-test-recovery.log
    exit 1
fi

# Verify all messages recovered
echo "Verifying $MESSAGE_COUNT messages recovered..."
$TEST_TOOL verify --count $MESSAGE_COUNT --queue "$QUEUE_NAME"
VERIFY_EXIT=$?

# Cleanup
kill $SERVER_PID 2>/dev/null || true

echo ""
echo "=================================================="
if [ $VERIFY_EXIT -eq 0 ]; then
    echo "✓ CRASH RECOVERY TEST PASSED"
    echo "=================================================="
    echo "All $MESSAGE_COUNT messages successfully recovered!"
    exit 0
else
    echo "✗ CRASH RECOVERY TEST FAILED"
    echo "=================================================="
    echo "Recovery verification failed. Check logs:"
    echo "  - /tmp/amqp-crash-test.log (initial run)"
    echo "  - /tmp/amqp-crash-test-recovery.log (recovery run)"
    exit 1
fi
