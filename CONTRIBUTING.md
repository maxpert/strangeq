# Contributing to AMQP-Go

Thank you for your interest in contributing to AMQP-Go! This document provides guidelines and instructions for contributing.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Code Style](#code-style)
- [Commit Messages](#commit-messages)

## Code of Conduct

Please be respectful and constructive in all interactions. We aim to foster an open and welcoming environment.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/strangeq.git
   cd strangeq/src/amqp-go
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/maxpert/strangeq.git
   ```

## Development Setup

### Prerequisites
- Go 1.25.1 or later
- Git
- Make (optional, for build automation)

### Install Dependencies
```bash
cd src/amqp-go
go mod download
```

### Build the Project
```bash
go build ./cmd/amqp-server
```

### Run Tests
```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run specific package tests
go test ./protocol/...
```

### Run Benchmarks
```bash
go test -bench=. -benchmem ./protocol/
go test -bench=. -benchmem ./benchmarks/
```

## Making Changes

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-description
   ```

2. **Make your changes**:
   - Write clear, concise code
   - Add tests for new functionality
   - Update documentation as needed
   - Follow the existing code style

3. **Test your changes**:
   ```bash
   # Run tests
   go test ./...
   
   # Run linter
   golangci-lint run
   
   # Format code
   go fmt ./...
   ```

4. **Commit your changes** (see [Commit Messages](#commit-messages))

## Testing

### Test Requirements
- All new features must include tests
- Bug fixes should include regression tests
- Maintain or improve code coverage
- Tests should be deterministic and not flaky

### Test Categories

**Unit Tests**
```bash
go test ./broker/
go test ./protocol/
go test ./storage/
```

**Integration Tests**
```bash
go test ./tests/
go test -tags=integration ./...
```

**Performance Benchmarks**
```bash
go test -bench=. -benchmem ./benchmarks/
```

**Fuzz Tests**
```bash
go test -fuzz=FuzzFrameMarshaling -fuzztime=30s ./protocol/
```

### Writing Tests
- Use table-driven tests when appropriate
- Use testify for assertions
- Clean up resources (defer cleanup functions)
- Use meaningful test names that describe what's being tested

Example:
```go
func TestQueueDeclare(t *testing.T) {
    tests := []struct {
        name      string
        queueName string
        durable   bool
        wantErr   bool
    }{
        {"valid queue", "test-queue", true, false},
        {"empty name", "", false, true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // test implementation
        })
    }
}
```

## Submitting Changes

1. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create a Pull Request**:
   - Go to the repository on GitHub
   - Click "New Pull Request"
   - Select your branch
   - Fill out the PR template completely
   - Link related issues

3. **Address Review Comments**:
   - Respond to all review comments
   - Make requested changes
   - Push updates to the same branch

4. **Keep PR Updated**:
   ```bash
   # Sync with upstream
   git fetch upstream
   git rebase upstream/main
   git push -f origin feature/your-feature-name
   ```

## Code Style

### General Guidelines
- Follow standard Go conventions
- Use `gofmt` for formatting
- Run `golangci-lint` before committing
- Write self-documenting code with clear variable names
- Add comments for complex logic
- Keep functions focused and concise

### Package Documentation
- Every package should have a package-level comment
- Export functions and types should have comments
- Comments should start with the name of the thing being described

Example:
```go
// Package broker provides message routing and delivery functionality
// for the AMQP server.
package broker

// DeclareQueue creates a new queue or returns an existing one.
// It returns an error if the queue parameters conflict with an existing queue.
func DeclareQueue(name string, durable bool) error {
    // implementation
}
```

### Error Handling
- Return errors, don't panic (except in truly exceptional cases)
- Use custom error types from the `errors` package
- Wrap errors with context: `fmt.Errorf("operation failed: %w", err)`
- Check all errors

### Concurrency
- Use mutexes appropriately (prefer RWMutex when reads dominate)
- Document goroutine lifecycles
- Use channels for communication between goroutines
- Avoid goroutine leaks (ensure all goroutines can exit)

## Commit Messages

### Format
```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `perf`: Performance improvements
- `refactor`: Code refactoring
- `style`: Code style changes (formatting, etc.)
- `chore`: Maintenance tasks

### Examples
```
feat(protocol): add support for exchange-to-exchange binding

Implements Exchange.Bind and Exchange.Unbind methods according to
AMQP 0.9.1 specification section 3.2.

Closes #123
```

```
fix(storage): prevent deadlock in badger transaction cleanup

The cleanup goroutine was holding a lock while waiting for transactions
to complete, causing a deadlock. Now releases lock before waiting.

Fixes #456
```

## AMQP Compliance

When working on AMQP protocol features:

1. **Reference the Specification**:
   - Link to relevant sections in code comments
   - Ensure compliance with AMQP 0.9.1 spec

2. **Test Against RabbitMQ**:
   - Use the RabbitMQ interoperability tests
   - Verify behavior matches RabbitMQ where appropriate

3. **Protocol Tests**:
   - Add compliance tests for new protocol features
   - Test frame encoding/decoding
   - Validate field types and ranges

## Performance Considerations

- Run benchmarks before and after changes
- Document performance characteristics
- Avoid allocations in hot paths
- Use buffer pooling where appropriate
- Profile before optimizing

## Questions?

If you have questions:
- Check existing issues and discussions
- Open a new issue with the `question` label
- Reach out to maintainers

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT License).

