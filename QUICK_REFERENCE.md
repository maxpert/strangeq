# Quick Reference Card

## Release Workflow

### Create a Release (3 steps)
```bash
# 1. Tag
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0

# 2. Create release on GitHub
# Go to: https://github.com/YOUR-ORG/strangeq/releases/new

# 3. Done! Binaries build automatically
```

### What Gets Built Automatically
- macOS: arm64, amd64
- Linux: amd64, arm64, 386

## Make Commands

```bash
make help              # Show all commands
make build             # Build binary
make build-all         # Build all platforms
make test              # Run tests
make test-coverage     # Tests + coverage report
make bench             # Run benchmarks
make lint              # Run linters
make fmt               # Format code
make clean             # Clean builds
make release-local     # Build release locally
```

## Development

```bash
# Start server
make run

# Start with debug logs
make dev

# Run tests
make test

# Check code quality
make check    # runs fmt, vet, lint, test
```

## Testing Builds

```bash
# Test single platform
cd src/amqp-go
GOOS=linux GOARCH=amd64 go build ./cmd/amqp-server

# Test all platforms
make build-all

# Results in build/ directory
ls -lh build/
```

## GitHub Actions

### Manual Trigger
1. Go to: Actions → Release Build
2. Click "Run workflow"
3. Enter version: v0.0.0-test
4. Click "Run workflow"

### Check Status
- Repository → Actions
- Click on workflow run
- View logs for each platform

## Files Overview

```
.github/
├── workflows/
│   ├── release.yml      # Multi-platform builds on release
│   ├── build.yml        # CI on push/PR
│   └── codeql.yml       # Security scanning
├── ISSUE_TEMPLATE/      # Bug/feature templates
└── dependabot.yml       # Dependency updates

deployment/
└── systemd/
    ├── amqp-server.service   # systemd unit file
    └── README.md             # Deployment guide

CHANGELOG.md             # Version history
CONTRIBUTING.md          # How to contribute
LICENSE                  # MIT License
RELEASE.md              # Release process details
RELEASE_CHECKLIST.md    # Release checklist
SECURITY.md             # Security policy
QUICKSTART.md           # Getting started
SETUP_SUMMARY.md        # This setup explained
Makefile                # Build automation
```

## Common Tasks

### Before Committing
```bash
make fmt      # Format code
make lint     # Check code quality
make test     # Run tests
```

### Before Release
```bash
make clean
make test-coverage
make bench
make build-all
# Update CHANGELOG.md
# Test binaries
```

### After Release
```bash
# Verify release
gh release view v0.1.0

# Download and test
wget https://github.com/.../amqp-server-linux-amd64
chmod +x amqp-server-linux-amd64
./amqp-server-linux-amd64 -version
```

## Version Numbers

- **Major** (1.0.0): Breaking changes
- **Minor** (0.1.0): New features
- **Patch** (0.0.1): Bug fixes

## Quick Fixes

### Workflow not running?
- Check tag was pushed: `git ls-remote --tags`
- Verify release (not just tag) created on GitHub
- Check Actions tab for errors

### Build failing?
- Run locally: `make build-all`
- Check logs in Actions tab
- Verify Go version matches (1.25.1)

## Support Files

- `SETUP_SUMMARY.md` - Complete setup explanation
- `RELEASE_CHECKLIST.md` - Step-by-step release guide
- `CONTRIBUTING.md` - Contribution guidelines
- `.github/workflows/README.md` - Workflow documentation

