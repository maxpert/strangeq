#!/bin/bash
set -euo pipefail

# Crash Recovery Test
# Publishes durable messages, kills the broker with SIGKILL, then restarts it
# on the same storage directory and checks the messages are still consumable.
#
# Usage: ./scripts/crash_test.sh [message_count]
# Example: ./scripts/crash_test.sh 10000

MESSAGE_COUNT=${1:-1000}
QUEUE_NAME="crash_test_queue"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib/test_env.sh
source "$SCRIPT_DIR/lib/test_env.sh"

WORKDIR=$(mktemp -d)
STORAGE_PATH=${STORAGE_PATH:-$WORKDIR/data}
AMQP_PORT=$(pick_port)
TELEMETRY_PORT=$(pick_port)
CONFIG="$WORKDIR/config.yaml"
URL="amqp://guest:guest@127.0.0.1:${AMQP_PORT}/"

gen_config "$STORAGE_PATH" "$AMQP_PORT" "$TELEMETRY_PORT" >"$CONFIG"

cleanup() {
    stop_server "$WORKDIR" KILL
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "=================================================="
echo "AMQP Crash Recovery Test"
echo "=================================================="
echo "Message Count: $MESSAGE_COUNT"
echo "Queue: $QUEUE_NAME"
echo "Storage: $STORAGE_PATH"
echo "AMQP port: $AMQP_PORT  telemetry port: $TELEMETRY_PORT"
echo ""

# Step 1: Build the server and the test tool
echo "[1/7] Building server and test tool..."
cd "$PROJECT_DIR"
go build -o amqp-server ./cmd/amqp-server
go build -o crash_test_tool ./cmd/crash_test_tool
TEST_TOOL="$PROJECT_DIR/crash_test_tool"

# Step 2: Start the first session
echo "[2/7] Starting server (first session)..."
SERVER_PID=$(start_server "$CONFIG" "$WORKDIR/server.log")
echo "Server PID: $SERVER_PID"
if ! wait_healthy "$TELEMETRY_PORT" 120; then
    echo "ERROR: Server failed to start"
    cat "$WORKDIR/server.log"
    exit 1
fi

# Step 3: Publish durable messages
echo "[3/7] Publishing $MESSAGE_COUNT durable messages..."
if ! "$TEST_TOOL" publish --count "$MESSAGE_COUNT" --queue "$QUEUE_NAME" --url "$URL"; then
    echo "ERROR: Failed to publish messages"
    cat "$WORKDIR/server.log"
    exit 1
fi
echo "✓ Published $MESSAGE_COUNT messages successfully"

# Step 4: CRASH! Force kill the broker.
echo "[4/7] CRASH! Force killing server (kill -9)..."
kill -9 "$SERVER_PID"
rm -f "$WORKDIR/server.pid"
sleep 1
if kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: Server still running after kill -9"
    exit 1
fi
echo "✓ Server killed"

# Step 5: Restart on the same storage and wait for recovery to finish.
echo "[5/7] Restarting server and waiting for recovery..."
SERVER_PID=$(start_server "$CONFIG" "$WORKDIR/server-recovery.log")
echo "Server PID: $SERVER_PID"
if ! wait_healthy "$TELEMETRY_PORT" 120; then
    echo "ERROR: Server failed to restart"
    cat "$WORKDIR/server-recovery.log"
    exit 1
fi
echo "✓ Server restarted"

# Step 6: Verify every message is recoverable.
echo "[6/7] Verifying $MESSAGE_COUNT messages recovered..."
if ! "$TEST_TOOL" verify --count "$MESSAGE_COUNT" --queue "$QUEUE_NAME" --url "$URL"; then
    echo "ERROR: Recovery verification failed"
    echo "--- first session ---"
    cat "$WORKDIR/server.log"
    echo "--- recovery session ---"
    cat "$WORKDIR/server-recovery.log"
    exit 1
fi

# Step 7: Stop cleanly and report.
echo "[7/7] Stopping server..."
kill -TERM "$SERVER_PID" 2>/dev/null || true
for _ in $(seq 1 45); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 1
done
# Remove the pidfile only once the process is gone; if it outlived the wait,
# the EXIT trap's stop_server backstop still needs the pidfile to kill it.
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    rm -f "$WORKDIR/server.pid"
fi

echo ""
echo "=================================================="
echo "✓ CRASH RECOVERY TEST PASSED"
echo "=================================================="
echo "All $MESSAGE_COUNT messages successfully recovered!"
echo ""
echo "Logs:"
echo "--- first session ---"
cat "$WORKDIR/server.log"
echo "--- recovery session ---"
cat "$WORKDIR/server-recovery.log"
exit 0
