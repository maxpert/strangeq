# Release Process

## Versioning

StrangeQ follows [Semantic Versioning](https://semver.org/).

## Creating a Release

1. Create and push a version tag:
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

2. Create a GitHub release:
   - Go to https://github.com/maxpert/strangeq/releases/new
   - Select the tag
   - Copy release notes from CHANGELOG.md
   - Publish release

3. GitHub Actions automatically builds binaries for:
   - macOS (arm64, amd64)
   - Linux (amd64, arm64, 386)
   
   SHA256 checksums are generated and uploaded with the binaries.

4. Verify the release:
   - Check that all binaries are attached
   - Download and verify checksums
   - Test at least one binary

## Manual Build

```bash
cd src/amqp-go
VERSION="v0.1.0"

GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-darwin-arm64 ./cmd/amqp-server
GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-darwin-amd64 ./cmd/amqp-server
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-linux-amd64 ./cmd/amqp-server
GOOS=linux GOARCH=arm64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-linux-arm64 ./cmd/amqp-server
GOOS=linux GOARCH=386  go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-linux-386  ./cmd/amqp-server

sha256sum amqp-server-* > checksums.txt
```

## Hotfix Releases

1. Create a hotfix branch from the release tag:
   ```bash
   git checkout -b hotfix/0.1.1 v0.1.0
   ```

2. Make the fix, update CHANGELOG.md, commit.

3. Tag and push:
   ```bash
   git tag -a v0.1.1 -m "Hotfix v0.1.1"
   git push origin v0.1.1
   ```

4. Create GitHub release.

5. Merge back to main:
   ```bash
   git checkout main
   git merge hotfix/0.1.1
   git push origin main
   ```
