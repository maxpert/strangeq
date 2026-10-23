#!/bin/bash
# Shared helpers for the crash/stress scripts. Source this file; it defines
# functions only and has no side effects.
#
# The scripts used to pass flags that no longer exist (--storage-path), sleep
# for a fixed time instead of waiting for readiness, and pkill every broker on
# the machine. These helpers build a config the loader accepts, pick free
# ports, wait for /health, and track only the process the script started.

# gen_config STORAGE_PATH AMQP_PORT TELEMETRY_PORT
# Prints a minimal YAML config on stdout.
gen_config() {
    local storage_path="$1"
    local amqp_port="$2"
    local telemetry_port="$3"
    # YAML double-quoted scalar: escape backslash and double quote only.
    local escaped
    escaped=$(printf '%s' "$storage_path" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')
    cat <<EOF
network:
  address: "127.0.0.1:${amqp_port}"
storage:
  path: "${escaped}"
  fsync: true
telemetry:
  enabled: true
  address: 127.0.0.1
  port: ${telemetry_port}
server:
  loglevel: info
EOF
}

# pick_port prints a random port in 20000-39999 that nothing is listening on.
pick_port() {
    local attempt port
    for attempt in $(seq 1 50); do
        port=$(( (RANDOM % 20000) + 20000 ))
        if command -v lsof >/dev/null 2>&1; then
            if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
                continue
            fi
        fi
        # Works in bash (including macOS 3.2) when compiled with /dev/tcp.
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
            exec 3>&- 2>/dev/null || true
            continue
        fi
        printf '%s\n' "$port"
        return 0
    done
    echo "ERROR: could not find a free port after 50 attempts" >&2
    return 1
}

# wait_healthy TPORT TIMEOUT_S
wait_healthy() {
    local port="$1"
    local timeout="${2:-60}"
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        if command -v curl >/dev/null 2>&1; then
            if curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
                return 0
            fi
        else
            if (exec 3<>"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
                exec 3>&- 2>/dev/null || true
                return 0
            fi
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "ERROR: server did not become healthy on telemetry port ${port} within ${timeout}s" >&2
    return 1
}

# start_server CONFIG LOG [BINARY]
# Starts the broker in the background, records its PID in $WORKDIR/server.pid,
# and prints the PID.
start_server() {
    local config="$1"
    local log="$2"
    local binary="${3:-./amqp-server}"
    "$binary" --config "$config" >"$log" 2>&1 &
    local pid=$!
    if [ -n "${WORKDIR:-}" ]; then
        printf '%s\n' "$pid" >"${WORKDIR}/server.pid"
    fi
    printf '%s\n' "$pid"
}

# stop_server WORKDIR [SIGNAL]
# Stops only the process recorded in WORKDIR/server.pid (never pkill).
stop_server() {
    local workdir="$1"
    local signal="${2:-TERM}"
    local pidfile="${workdir}/server.pid"
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "-${signal}" "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    fi
}
