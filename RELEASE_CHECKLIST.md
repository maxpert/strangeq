# Release Checklist

Use this checklist when preparing a new release.

## Pre-Release (1-2 weeks before)

### Code Quality
- [ ] All tests pass locally: `make test`
- [ ] All linters pass: `make lint`
- [ ] Code coverage is adequate: `make test-coverage`
- [ ] Benchmarks show no regressions: `make bench`
- [ ] No critical bugs in issue tracker
- [ ] All planned features for version are complete

### Documentation
- [ ] Update CHANGELOG.md with all changes
- [ ] Update README.md if features changed
- [ ] Update version number in code if needed
- [ ] Review and update examples if API changed
- [ ] Check all documentation links work
- [ ] Update configuration examples if needed

### Testing
- [ ] Run full integration test suite
- [ ] Test with real AMQP clients (Python, Go, Node.js)
- [ ] Test on different platforms (macOS, Linux, Windows)
- [ ] Test with different storage backends (memory, badger)
- [ ] Test authentication mechanisms
- [ ] Test TLS configuration
- [ ] Test with high load
- [ ] Test graceful shutdown
- [ ] Test recovery scenarios

### Dependencies
- [ ] Update Go dependencies: `make deps-update`
- [ ] Check for security vulnerabilities: `go list -m -json all | nancy sleuth`
- [ ] Review Dependabot PRs

## Release Day

### Version & Tag
- [ ] Decide on version number (semver)
- [ ] Update CHANGELOG.md with release date
- [ ] Create release notes from CHANGELOG
- [ ] Commit final changes

```bash
git add CHANGELOG.md
git commit -m "chore: prepare for v0.1.0 release"
git push origin main
```

### Create Tag
- [ ] Create annotated tag

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

### GitHub Release
- [ ] Go to https://github.com/YOUR-ORG/strangeq/releases/new
- [ ] Select the tag you created
- [ ] Title: "v0.1.0" (match tag)
- [ ] Description: Use template from RELEASE.md
- [ ] Check "Create a discussion for this release" (optional)
- [ ] Click "Publish release"

### Automated Builds
- [ ] Wait for GitHub Actions to complete
- [ ] Verify all platform binaries are uploaded
- [ ] Download and test at least one binary per OS
- [ ] Verify checksums match

```bash
# Download binary and checksum
wget https://github.com/YOUR-ORG/strangeq/releases/download/v0.1.0/amqp-server-linux-amd64
wget https://github.com/YOUR-ORG/strangeq/releases/download/v0.1.0/amqp-server-linux-amd64.sha256

# Verify
sha256sum -c amqp-server-linux-amd64.sha256
```

### Docker
- [ ] Verify Docker image was pushed
- [ ] Pull and test Docker image

```bash
docker pull YOUR-USERNAME/amqp-go:v0.1.0
docker pull YOUR-USERNAME/amqp-go:latest
docker run --rm YOUR-USERNAME/amqp-go:v0.1.0 -version
```

## Post-Release

### Communication
- [ ] Announce on project channels (if applicable)
- [ ] Update project website (if applicable)
- [ ] Post on social media (if applicable)
- [ ] Notify key users/contributors

### Documentation Sites
- [ ] Update documentation site (if applicable)
- [ ] Update any external documentation
- [ ] Update package manager entries (if applicable)

### Monitoring
- [ ] Monitor issue tracker for release-related bugs
- [ ] Monitor download statistics
- [ ] Check for user feedback
- [ ] Watch CI/CD for any issues

### Cleanup
- [ ] Close milestone (if used)
- [ ] Update project board (if used)
- [ ] Create new milestone for next release
- [ ] Create CHANGELOG section for next version

```markdown
## [Unreleased]

### Added

### Changed

### Fixed

### Removed
```

## Hotfix Release (If Needed)

### Urgent Bug Fix
- [ ] Create hotfix branch from release tag

```bash
git checkout -b hotfix/0.1.1 v0.1.0
```

- [ ] Make minimal fix
- [ ] Update CHANGELOG.md
- [ ] Test thoroughly
- [ ] Commit and tag

```bash
git commit -m "fix: critical bug description"
git tag -a v0.1.1 -m "Hotfix v0.1.1"
git push origin v0.1.1
```

- [ ] Create GitHub release
- [ ] Merge back to main

```bash
git checkout main
git merge hotfix/0.1.1
git push origin main
```

## Version Numbers Guide

### Major (X.0.0)
- Breaking changes to public API
- Removal of deprecated features
- Major architectural changes
- AMQP protocol version changes

### Minor (0.X.0)
- New features (backwards compatible)
- New configuration options
- New storage backends
- Performance improvements
- Deprecations (with warnings)

### Patch (0.0.X)
- Bug fixes
- Security patches
- Documentation updates
- Minor performance improvements
- Dependency updates

## Release Notes Template

```markdown
# Release v0.1.0

We're excited to announce the release of AMQP-Go v0.1.0!

## 🎉 Highlights

- Full AMQP 0.9.1 protocol implementation
- High-performance message broker (3M+ ops/sec)
- Multiple storage backends (memory, Badger)
- Complete authentication framework
- Prometheus metrics integration

## ✨ What's New

### Features
- Feature 1 (#PR)
- Feature 2 (#PR)

### Improvements
- Performance improvement 1 (#PR)
- Performance improvement 2 (#PR)

### Bug Fixes
- Bug fix 1 (#PR)
- Bug fix 2 (#PR)

## 📦 Installation

### Download Binary
Download the appropriate binary for your platform from the assets below.

### Docker
```bash
docker pull YOUR-USERNAME/amqp-go:v0.1.0
```

### Go Install
```bash
go install github.com/maxpert/amqp-go/cmd/amqp-server@v0.1.0
```

## 📚 Documentation

- [Quick Start Guide](QUICKSTART.md)
- [Configuration Guide](README.md#configuration)
- [Security Best Practices](SECURITY.md)

## 🔒 Security

See [SECURITY.md](SECURITY.md) for security advisories and best practices.

## 📝 Changelog

See [CHANGELOG.md](CHANGELOG.md) for complete details.

## 🙏 Contributors

Thanks to all contributors who made this release possible!

## ⬆️ Upgrading

This is the first stable release. See [CHANGELOG.md](CHANGELOG.md) for details.

## 🐛 Known Issues

- None at this time

## 💬 Feedback

Please report any issues on our [issue tracker](https://github.com/YOUR-ORG/strangeq/issues).
```

## Rollback Plan

If a critical issue is found:

1. **Immediate**: Delete the release (not recommended but possible)
2. **Quick**: Create hotfix release (v0.1.1)
3. **Document**: Update release notes with known issues
4. **Communicate**: Notify users through all channels

## Success Criteria

- [ ] All binaries available for download
- [ ] Docker images accessible
- [ ] No critical bugs reported in first 48 hours
- [ ] Documentation is clear and helpful
- [ ] Users can successfully install and run
- [ ] Tests pass on all platforms

## Notes

- Always test on a fresh machine if possible
- Keep release notes focused and user-friendly
- Include migration guides for breaking changes
- Celebrate the release! 🎉

