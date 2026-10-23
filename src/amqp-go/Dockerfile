# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=0.1.0-dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /out/amqp-server ./cmd/amqp-server \
 && mkdir -p /out/var/lib/amqp

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/amqp-server /usr/local/bin/amqp-server
COPY --from=build --chown=65532:65532 /out/var/lib/amqp /var/lib/amqp
ENV AMQP_NETWORK_ADDRESS=0.0.0.0:5672 \
    AMQP_STORAGE_PATH=/var/lib/amqp/data \
    AMQP_TELEMETRY_ENABLED=true \
    AMQP_TELEMETRY_ADDRESS=0.0.0.0 \
    AMQP_TELEMETRY_PORT=9419
VOLUME ["/var/lib/amqp"]
EXPOSE 5672 9419
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=3 \
  CMD ["/usr/local/bin/amqp-server", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/amqp-server"]
