.PHONY: build build-server build-perftest test bench-baseline bench-throughput bench-durable bench-large clean

AMQP_URL ?= amqp://guest:guest@localhost:5672/

build: build-server build-perftest

build-server:
	go build -o amqp-server ./cmd/amqp-server

build-perftest:
	go build -o perftest ./cmd/perftest

test:
	go test ./...

# 1P-1C, 1KB messages, 30s — establishes baseline latency and throughput
bench-baseline: build-perftest
	./perftest -url $(AMQP_URL) -producers 1 -consumers 1 -duration 30s -size 1024

# 50P-50C saturation test — measures peak throughput under load
bench-throughput: build-perftest
	./perftest -url $(AMQP_URL) -producers 50 -consumers 50 -duration 60s -size 1024

# Durable queue + persistent messages + publisher confirms — measures durability overhead
bench-durable: build-perftest
	./perftest -url $(AMQP_URL) -producers 10 -consumers 10 -duration 60s -confirm -durable

# 64KB messages — measures fragmentation and large-message handling overhead
bench-large: build-perftest
	./perftest -url $(AMQP_URL) -producers 1 -consumers 1 -duration 30s -size 65536

clean:
	rm -f amqp-server perftest
