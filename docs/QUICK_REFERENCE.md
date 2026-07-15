# Quick Reference

## Release

```bash
# Tag and push
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0

# Create release on GitHub: https://github.com/maxpert/strangeq/releases/new
# Binaries build automatically for macOS (arm64, amd64) and Linux (amd64, arm64, 386)
```

## Development

```bash
cd src/amqp-go

# Build
go build -o amqp-server ./cmd/amqp-server

# Run
go run ./cmd/amqp-server

# Test
go test -race ./...

# Benchmarks
go test -bench=. -benchmem ./storage/

# Format
gofmt -w .

# Vet
go vet ./...
```

## Cross-Compile

```bash
cd src/amqp-go
GOOS=linux GOARCH=amd64 go build -o amqp-server-linux ./cmd/amqp-server
```

## Files

```
.github/workflows/     CI/CD (build.yml, release.yml, codeql.yml)
deployment/systemd/    systemd service file
src/amqp-go/           Go source
CHANGELOG.md           Version history
CONTRIBUTING.md        Contribution guidelines
SECURITY.md            Security policy
```

## Troubleshooting

### Build failing
- Verify Go version: `go version` (needs 1.25.1+)
- Run locally: `go build ./cmd/amqp-server`
- Check logs in the Actions tab

### Workflow not running
- Check tag was pushed: `git ls-remote --tags`
- Verify release (not just tag) was created on GitHub
