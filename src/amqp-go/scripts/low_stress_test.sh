#!/bin/bash
set -euo pipefail

# Low-stress crash recovery test.
# Publishes durable messages with the crash test tool, kills the broker with
# SIGKILL, restarts it, and verifies the messages via the same tool. The exit
# code is the verdict; nothing here requires manual inspection.
#
# Usage: ./scripts/low_stress_test.sh [message_count]

MESSAGE_COUNT=${1:-1000}
QUEUE_NAME="stress_queue"

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
echo "AMQP Crash Recovery Stress Test"
echo "=================================================="
echo "Message Count: $MESSAGE_COUNT"
echo "Queue: $QUEUE_NAME"
echo "Storage: $STORAGE_PATH"
echo "AMQP port: $AMQP_PORT  telemetry port: $TELEMETRY_PORT"
echo ""

# Step 1: Build the server and the test tool
echo "[1/6] Building server and test tool..."
cd "$PROJECT_DIR"
go build -o amqp-server ./cmd/amqp-server
go build -o crash_test_tool ./cmd/crash_test_tool
TEST_TOOL="$PROJECT_DIR/crash_test_tool"

# Step 2: Start the first session
echo "[2/6] Starting server (first session)..."
SERVER_PID=$(start_server "$CONFIG" "$WORKDIR/server.log")
echo "Server PID: $SERVER_PID"
if ! wait_healthy "$TELEMETRY_PORT" 120; then
    echo "ERROR: Server failed to start"
    cat "$WORKDIR/server.log"
    exit 1
fi

# Step 3: Publish durable messages
echo "[3/6] Publishing $MESSAGE_COUNT durable messages..."
if ! "$TEST_TOOL" publish --count "$MESSAGE_COUNT" --queue "$QUEUE_NAME" --url "$URL"; then
    echo "ERROR: Failed to publish messages"
    cat "$WORKDIR/server.log"
    exit 1
fi

echo ""
echo "Checking storage files..."
ls -lh "$STORAGE_PATH/wal/shared/" 2>/dev/null || echo "No WAL files"
ls -lh "$STORAGE_PATH/metadata/queues/" 2>/dev/null || echo "No queue metadata"

# Step 4: CRASH! Force kill the broker.
echo ""
echo "[4/6] CRASH! Force killing server (kill -9)..."
kill -9 "$SERVER_PID"
rm -f "$WORKDIR/server.pid"
sleep 1
if kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: Server still running after kill -9"
    exit 1
fi
echo "✓ Server killed"

# Step 5: Restart and wait for recovery.
echo "[5/6] Restarting server and waiting for recovery..."
SERVER_PID=$(start_server "$CONFIG" "$WORKDIR/server-recovery.log")
echo "Server PID: $SERVER_PID"
if ! wait_healthy "$TELEMETRY_PORT" 120; then
    echo "ERROR: Server failed to restart"
    cat "$WORKDIR/server-recovery.log"
    exit 1
fi

echo ""
echo "Recovery logs:"
grep -E "recovery|recovered|messages" "$WORKDIR/server-recovery.log" | head -20 || true

# Step 6: Verify and exit with the verdict.
echo ""
echo "[6/6] Verifying $MESSAGE_COUNT messages recovered..."
if ! "$TEST_TOOL" verify --count "$MESSAGE_COUNT" --queue "$QUEUE_NAME" --url "$URL"; then
    echo "✗ RECOVERY VERIFICATION FAILED"
    echo "--- first session ---"
    cat "$WORKDIR/server.log"
    echo "--- recovery session ---"
    cat "$WORKDIR/server-recovery.log"
    exit 1
fi

kill -TERM "$SERVER_PID" 2>/dev/null || true
for _ in $(seq 1 45); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 1
done
# Remove the pidfile only once the process is gone; the EXIT trap's
# stop_server backstop needs it if the broker outlived the wait.
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    rm -f "$WORKDIR/server.pid"
fi

echo ""
echo "=================================================="
echo "✓ LOW-STRESS CRASH RECOVERY TEST PASSED"
echo "=================================================="
echo "All $MESSAGE_COUNT messages successfully recovered!"
echo ""
echo "Logs:"
echo "--- first session ---"
cat "$WORKDIR/server.log"
echo "--- recovery session ---"
cat "$WORKDIR/server-recovery.log"
exit 0
