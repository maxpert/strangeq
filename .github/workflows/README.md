# GitHub Actions Workflows

This directory contains automated workflows for the AMQP-Go project.

## Workflows

### 1. Release Build (`release.yml`)

**Triggers:**
- When a new release is created on GitHub
- Manual dispatch with version input

**What it does:**
- Builds binaries for all supported platforms:
  - macOS: arm64, amd64
  - Linux: amd64, arm64, 386
- Generates SHA256 checksums for each binary
- Uploads all binaries to the GitHub release

**Platforms:**
- macOS builds run on: `macos-latest`
- Linux cross-compilation runs on: `ubuntu-latest`

**Usage:**
```bash
# Create a release
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0

# Then create release on GitHub
# Workflow will automatically trigger
```

**Manual trigger:**
- Go to Actions → Release Build → Run workflow
- Enter version tag (e.g., v0.1.0-dev)

### 2. Build and Test (`build.yml`)

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop`

**What it does:**
- Runs full test suite with race detection
- Generates code coverage report
- Uploads coverage to Codecov
- Runs golangci-lint for code quality
- Builds the binary to verify compilation
- Caches Go modules for faster builds

**Jobs:**
1. **Test**: Run tests with coverage
2. **Lint**: Static code analysis
3. **Build**: Verify binary builds

### 3. CodeQL Analysis (`codeql.yml`)

**Triggers:**
- Push to `main` branch
- Pull requests to `main`
- Weekly schedule (Monday)

**What it does:**
- Performs security and quality analysis
- Identifies potential vulnerabilities
- Checks for common coding mistakes
- Reports findings in Security tab

### 4. Dependabot (`dependabot.yml`)

**Schedule:** Weekly on Monday

**What it does:**
- Checks for Go module updates
- Checks for GitHub Actions updates
- Creates PRs for outdated dependencies
- Keeps dependencies secure and up-to-date

## Secrets Required

### For Codecov (Optional)

- `CODECOV_TOKEN`: Token from codecov.io (optional but recommended)

## Workflow Badges

Add these to your README.md:

```markdown
[![Build Status](https://github.com/YOUR-ORG/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/YOUR-ORG/strangeq/actions)
[![CodeQL](https://github.com/YOUR-ORG/strangeq/workflows/CodeQL/badge.svg)](https://github.com/YOUR-ORG/strangeq/actions)
[![codecov](https://codecov.io/gh/YOUR-ORG/strangeq/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR-ORG/strangeq)
```

## Local Testing

### Test the build locally

```bash
# Install act (GitHub Actions local runner)
brew install act

# List workflows
act -l

# Run push event
act push

# Run pull request
act pull_request

# Run release (dry run)
act release -n
```

### Test builds manually

```bash
cd src/amqp-go

# Test all platforms
for GOOS in darwin linux windows; do
  for GOARCH in amd64 arm64; do
    echo "Building $GOOS/$GOARCH"
    GOOS=$GOOS GOARCH=$GOARCH go build -o /dev/null ./cmd/amqp-server 2>/dev/null && \
      echo "  ✓ $GOOS/$GOARCH" || \
      echo "  ✗ $GOOS/$GOARCH"
  done
done
```

## Troubleshooting

### Release workflow not triggering

1. Check if release was created (not just a tag)
2. Verify workflow file is in `main` branch
3. Check Actions tab for any errors

### Build failures

1. Check Go version compatibility (requires 1.25.1+)
2. Verify all tests pass locally: `go test ./...`
3. Check for platform-specific issues

### Codecov upload failing

1. This is non-critical - workflow will continue
2. Add `CODECOV_TOKEN` secret for better reliability
3. Check codecov.io for project setup

## Customization

### Change Go version

Edit all workflow files:

```yaml
- name: Set up Go
  uses: actions/setup-go@v5
  with:
    go-version: '1.25.1'  # Update this
```

### Add new platform

Edit `release.yml` matrix:

```yaml
matrix:
  include:
    # Add new platform
    - os: freebsd
      arch: amd64
      runner: ubuntu-latest
      goos: freebsd
      goarch: amd64
```

## Best Practices

1. **Always test locally** before pushing
2. **Create feature branches** for changes
3. **Wait for CI** before merging PRs
4. **Review coverage reports** to maintain quality
5. **Update workflows** when adding new dependencies
6. **Keep actions up to date** (Dependabot helps)
7. **Monitor workflow runs** for failures

## Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Go GitHub Actions](https://github.com/actions/setup-go)
- [CodeQL Documentation](https://codeql.github.com/docs/)

