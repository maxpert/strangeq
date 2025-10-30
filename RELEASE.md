# Release Process

This document describes the release process for AMQP-Go.

## Versioning

AMQP-Go follows [Semantic Versioning](https://semver.org/):
- **MAJOR** version for incompatible API changes
- **MINOR** version for backwards-compatible functionality additions
- **PATCH** version for backwards-compatible bug fixes

## Release Checklist

### Pre-Release

- [ ] All tests pass on main branch
- [ ] Update CHANGELOG.md with release notes
- [ ] Update version in relevant files if needed
- [ ] Review and merge all pending PRs for the release
- [ ] Run full benchmark suite and verify no regressions
- [ ] Test with real AMQP clients (RabbitMQ client, etc.)
- [ ] Update documentation if needed

### Creating a Release

1. **Create and push a version tag**:
   ```bash
   # Example for version 0.1.0
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

2. **Create GitHub Release**:
   - Go to https://github.com/YOUR-ORG/strangeq/releases/new
   - Select the tag you just created
   - Title: `v0.1.0` (or your version)
   - Description: Copy relevant section from CHANGELOG.md
   - Click "Publish release"

3. **Automated Build Process**:
   - GitHub Actions will automatically trigger on release creation
   - Builds binaries for all platforms:
     - macOS (amd64, arm64)
     - Linux (amd64, arm64, 386)
   - Generates SHA256 checksums for each binary
   - Uploads all artifacts to the GitHub release

4. **Verify Release**:
   - Check that all binaries are attached to the release
   - Download and verify checksums
   - Test at least one binary on the target platform

### Post-Release

- [ ] Announce release (if applicable)
- [ ] Update documentation site (if applicable)
- [ ] Create a new section in CHANGELOG.md for next release
- [ ] Close milestone (if used)
- [ ] Update any dependent projects

## Manual Build (if needed)

If you need to build binaries manually:

```bash
# Navigate to source directory
cd src/amqp-go

# Set version
VERSION="v0.1.0"

# Build for different platforms
# macOS ARM64
GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-darwin-arm64 ./cmd/amqp-server

# macOS AMD64
GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-darwin-amd64 ./cmd/amqp-server

# Linux AMD64
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-linux-amd64 ./cmd/amqp-server

# Linux ARM64
GOOS=linux GOARCH=arm64 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-linux-arm64 ./cmd/amqp-server

# Linux 386
GOOS=linux GOARCH=386 go build -ldflags="-s -w -X main.version=${VERSION}" -o amqp-server-linux-386 ./cmd/amqp-server

# Generate checksums
sha256sum amqp-server-* > checksums.txt
```

## Hotfix Releases

For critical bug fixes on a release branch:

1. Create a hotfix branch from the release tag:
   ```bash
   git checkout -b hotfix/0.1.1 v0.1.0
   ```

2. Make and commit the fix

3. Update CHANGELOG.md

4. Create and push the hotfix tag:
   ```bash
   git tag -a v0.1.1 -m "Hotfix v0.1.1"
   git push origin v0.1.1
   ```

5. Create GitHub release (same process as above)

6. Merge hotfix back to main:
   ```bash
   git checkout main
   git merge hotfix/0.1.1
   git push origin main
   ```

## Release Notes Template

Use this template for GitHub release descriptions:

```markdown
## What's New

### Features
- Feature 1 (#PR_NUMBER)
- Feature 2 (#PR_NUMBER)

### Bug Fixes
- Fix 1 (#PR_NUMBER)
- Fix 2 (#PR_NUMBER)

### Performance Improvements
- Improvement 1 (#PR_NUMBER)

### Documentation
- Doc update 1 (#PR_NUMBER)

## Installation

### Binaries
Download the appropriate binary for your platform from the assets below.

### Go Install
```bash
go install github.com/maxpert/amqp-go/cmd/amqp-server@v0.1.0
```

## Checksums
SHA256 checksums are provided for each binary. Verify your download:
```bash
sha256sum -c amqp-server-YOUR-PLATFORM.sha256
```

## Full Changelog
See [CHANGELOG.md](CHANGELOG.md) for complete details.
```

## Troubleshooting

### GitHub Actions Failing

1. Check the Actions tab for error logs
2. Verify Go version compatibility
3. Ensure all tests pass locally

### Missing Binaries

1. Check if the build job completed successfully
2. Verify artifact upload step succeeded
3. Check release asset upload step logs

## Support

For questions about the release process:
- Open an issue with the `question` label
- Contact maintainers

