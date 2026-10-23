#!/bin/bash
set -euo pipefail

# systemd unit smoke test.
#
# Verifies that the shipped unit and config actually work together without
# systemd: every flag in ExecStart exists in the binary, the optional
# EnvironmentFile is declared optional, the broker starts with the shipped
# config plus env overrides, /health reports ready, SIGHUP does not kill it,
# and SIGTERM exits 0 within the unit's timeout.
#
# Usage: scripts/systemd_smoke.sh [path-to-amqp-server]

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT="$ROOT/deployment/systemd/amqp-server.service"
CONFIG="$ROOT/deployment/systemd/config.yaml"
BIN="${1:-}"

TMPDIR_SMOKE=$(mktemp -d)
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -KILL "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TMPDIR_SMOKE"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    if [ -f "$TMPDIR_SMOKE/server.log" ]; then
        echo "--- server log ---" >&2
        cat "$TMPDIR_SMOKE/server.log" >&2
    fi
    exit 1
}

if [ -z "$BIN" ]; then
    echo "Building amqp-server..."
    BIN="$TMPDIR_SMOKE/amqp-server"
    (cd "$ROOT/src/amqp-go" && go build -o "$BIN" ./cmd/amqp-server) || fail "build failed"
else
    case "$BIN" in
        /*) ;;
        *) BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")" ;;
    esac
    [ -x "$BIN" ] || fail "binary not executable: $BIN"
fi

[ -f "$UNIT" ] || fail "unit not found: $UNIT"
[ -f "$CONFIG" ] || fail "config not found: $CONFIG"

# Step 1: join the ExecStart continuation lines and check the binary path.
EXEC=$(awk '
    /^ExecStart=/ { inside=1; sub(/^ExecStart=/, ""); print; next }
    inside && /^[ \t]/ { print; next }
    inside { exit }
' "$UNIT" | tr '\n' ' ')
EXEC=$(echo "$EXEC" | sed -e 's/[[:space:]]\+/ /g' -e 's/^ //' -e 's/ $//')
[ -n "$EXEC" ] || fail "could not parse ExecStart"
FIRST_TOKEN=${EXEC%% *}
[ "$FIRST_TOKEN" = "/usr/local/bin/amqp-server" ] || fail "ExecStart must start with /usr/local/bin/amqp-server, got $FIRST_TOKEN"

# Step 2: every flag in ExecStart must exist in the binary's usage output.
HELP=$("$BIN" -h 2>&1 || true)
MISSING=""
for token in $EXEC; do
    case "$token" in
        --*)
            name=${token#--}
            name=${name%%=*}
            if ! printf '%s\n' "$HELP" | grep -Eq -- "(^|[[:space:]])-{1,2}${name}([=[:space:]]|$)"; then
                MISSING="$MISSING $token"
            fi
            ;;
    esac
done
[ -z "$MISSING" ] || fail "unit passes flags the binary does not define:$MISSING"

# Step 3: the environment file must be optional.
ENV_LINE=$(grep -E '^EnvironmentFile=' "$UNIT" || true)
case "$ENV_LINE" in
    EnvironmentFile=-*) ;;
    *) fail "EnvironmentFile must start with '-' so a missing file is not fatal" ;;
esac

if command -v systemd-analyze >/dev/null 2>&1; then
    UNIT_COPY="$TMPDIR_SMOKE/amqp-server.service"
    sed -e "s#/usr/local/bin/amqp-server#${BIN}#g" \
        -e "s#/etc/amqp/config.yaml#${CONFIG}#g" "$UNIT" >"$UNIT_COPY"
    if ! ANALYZE=$(systemd-analyze verify "$UNIT_COPY" 2>&1); then
        if printf '%s\n' "$ANALYZE" | grep -vqE 'user .*amqp|group .*amqp'; then
            fail "systemd-analyze verify failed: $ANALYZE"
        fi
        echo "WARN: systemd-analyze reported only the missing amqp user/group"
    fi
fi

# Step 4: run the unit's ExecStart with the shipped config, substituting paths
# and overriding storage/ports/auth via the environment (no systemd needed).
EXEC_RUN=${EXEC//\/usr\/local\/bin\/amqp-server/$BIN}
EXEC_RUN=${EXEC_RUN//\/etc\/amqp\/config.yaml/$CONFIG}

START_OK=false
for attempt in 1 2 3 4 5; do
    PORT=$(( ( RANDOM % 20000 ) + 20000 ))
    TPORT=$(( ( RANDOM % 20000 ) + 20000 ))
    export AMQP_STORAGE_PATH="$TMPDIR_SMOKE/data-$attempt"
    export AMQP_NETWORK_ADDRESS="127.0.0.1:$PORT"
    export AMQP_TELEMETRY_PORT="$TPORT"
    export AMQP_SECURITY_AUTHENTICATIONENABLED=false

    # Word splitting of EXEC_RUN is intentional here.
    # shellcheck disable=SC2086
    $EXEC_RUN >"$TMPDIR_SMOKE/server.log" 2>&1 &
    SERVER_PID=$!

    READY=false
    for _ in $(seq 1 60); do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            break
        fi
        BODY=$(curl -fsS "http://127.0.0.1:${TPORT}/health" 2>/dev/null || true)
        if [ -n "$BODY" ]; then
            case "$BODY" in
                OK*) READY=true; break ;;
            esac
        fi
        sleep 1
    done

    if [ "$READY" = true ]; then
        START_OK=true
        break
    fi

    kill -KILL "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
    if grep -qi "address already in use" "$TMPDIR_SMOKE/server.log" 2>/dev/null; then
        echo "Port collision on attempt $attempt; retrying..."
        continue
    fi
    break
done

[ "$START_OK" = true ] || fail "server did not become healthy with the shipped config"

# Step 5: the shipped config must load without unknown-key warnings.
if grep -q "unknown config key" "$TMPDIR_SMOKE/server.log"; then
    fail "shipped config produced unknown-key warnings"
fi

# Step 6: SIGHUP reloads (or no-ops) and must not terminate the process.
kill -HUP "$SERVER_PID"
sleep 1
kill -0 "$SERVER_PID" 2>/dev/null || fail "SIGHUP terminated the server"

# Step 7: SIGTERM shuts down gracefully and exits 0 within the unit timeout.
kill -TERM "$SERVER_PID"
EXITED=false
for _ in $(seq 1 45); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        EXITED=true
        break
    fi
    sleep 1
done
if [ "$EXITED" != true ]; then
    fail "server did not exit within 45s of SIGTERM"
fi
set +e
wait "$SERVER_PID"
STATUS=$?
set -e
SERVER_PID=""
[ "$STATUS" -eq 0 ] || fail "server exited with status $STATUS after SIGTERM (want 0)"

echo "OK: systemd unit smoke test passed (amqp :$PORT, telemetry :$TPORT)"
