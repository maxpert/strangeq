#!/usr/bin/env bash
set -euo pipefail

module_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/strangeq-cluster-demo-build.XXXXXXXX")
demo_pid=

cleanup() {
  if [[ -n "$demo_pid" ]]; then
    kill -TERM "$demo_pid" 2>/dev/null || true
    wait "$demo_pid" 2>/dev/null || true
  fi
  rm -rf -- "$build_dir"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "$module_dir"
go build -o "$build_dir/amqp-server" ./cmd/amqp-server
go build -o "$build_dir/cluster-demo" ./examples/cluster-demo
"$build_dir/cluster-demo" -broker "$build_dir/amqp-server" &
demo_pid=$!
result=0
wait "$demo_pid" || result=$?
demo_pid=
exit "$result"
