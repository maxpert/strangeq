# GitHub Actions Workflows

## Workflows

### Release Build (`release.yml`)
Triggers on GitHub release creation or manual dispatch. Builds binaries for macOS (arm64, amd64) and Linux (amd64, arm64, 386), generates SHA256 checksums, and uploads to the release.

### Build and Test (`build.yml`)
Triggers on push/PR to main and develop. Runs tests with race detection, generates coverage, runs gofmt and go vet checks, and verifies the binary builds.

### CodeQL Analysis (`codeql.yml`)
Triggers on push/PR to main and weekly. Runs security and quality analysis.

### Dependabot (`dependabot.yml`)
Weekly checks for Go module and GitHub Actions updates.

## Local Testing

```bash
cd src/amqp-go
go test -race ./...
go build ./cmd/amqp-server
```

## Troubleshooting

### Release workflow not triggering
1. Check if release was created (not just a tag)
2. Verify workflow file is in main branch
3. Check Actions tab for errors

### Build failures
1. Check Go version (requires 1.25.1+)
2. Run locally: `go test ./... && go build ./cmd/amqp-server`
