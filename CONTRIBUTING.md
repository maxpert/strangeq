# Contributing

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR-USERNAME/strangeq.git
   cd strangeq
   ```
3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/maxpert/strangeq.git
   ```

## Development Setup

### Prerequisites
- Go 1.27.3 or later
- Git

### Build

Run development commands from the Go module directory. The build command below
starts at the repository root. Stay in `src/amqp-go` for the remaining Go commands.

```bash
cd src/amqp-go
go build ./cmd/amqp-server
```

### Run Tests

```bash
# All tests with race detector (the suite can take several minutes)
go test -race -timeout 25m ./...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Specific package
go test ./storage/...

# Benchmarks
go test -bench=. -benchmem ./storage/
```

## Making Changes

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes. Write tests for new functionality. Follow existing code style.

3. Test:
   ```bash
   go test -race -timeout 25m ./...
   gofmt -l .
   go vet ./...
   ```

4. Commit (see format below).

## Testing

- All new features must include tests
- Bug fixes should include regression tests
- Tests must be deterministic, not flaky
- Use table-driven tests when appropriate
- Use testify for assertions
- Clean up resources in defer blocks

## Submitting Changes

1. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Create a Pull Request on GitHub. Link related issues.

3. Address review feedback. Push updates to the same branch.

## Code Style

- Run `gofmt` before committing
- Follow standard Go conventions
- Write self-documenting code with clear variable names
- Keep functions focused and concise
- Return errors, don't panic
- Wrap errors with context: `fmt.Errorf("operation failed: %w", err)`

## Commit Messages

```
<type>(<scope>): <subject>

<body>
```

Types: `feat`, `fix`, `docs`, `test`, `perf`, `refactor`, `style`, `chore`

Example:
```
feat(protocol): add exchange-to-exchange binding

Implements Exchange.Bind and Exchange.Unbind per AMQP 0.9.1 spec.

Closes #123
```

## AMQP Compliance

When working on protocol features:
- Reference the AMQP 0.9.1 spec in code comments where relevant
- From `src/amqp-go`, run `make conformance-strangeq` against the in-process broker.
- Run `make conformance-rabbitmq` against a running RabbitMQ at `AMQP_URL`
  (default `amqp://guest:guest@localhost:5672/`). See the separate
  [low-level interoperability tests](src/amqp-go/docs/rabbitmq-tests.md) for their
  `RABBITMQ_*` settings. Those settings do not configure the conformance suite.
- Add conformance tests for new protocol features

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
