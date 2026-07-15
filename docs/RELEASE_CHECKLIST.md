# Release Checklist

## Pre-Release

### Code Quality
- [ ] All tests pass: `go test -race ./...`
- [ ] `gofmt -l .` produces no output
- [ ] `go vet ./...` passes
- [ ] Benchmarks show no regressions
- [ ] No critical bugs in issue tracker

### Documentation
- [ ] Update CHANGELOG.md
- [ ] Update README.md if features changed
- [ ] Check all documentation links work

### Testing
- [ ] Run full test suite with race detector
- [ ] Test with real AMQP clients (Python, Go, Node.js)
- [ ] Test authentication and TLS
- [ ] Test crash recovery scenarios

## Release

1. Update CHANGELOG.md with release date
2. Commit final changes:
   ```bash
   git add CHANGELOG.md
   git commit -m "chore: prepare for v0.1.0 release"
   git push origin main
   ```

3. Create and push tag:
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

4. Create GitHub release:
   - Go to https://github.com/maxpert/strangeq/releases/new
   - Select the tag
   - Copy release notes from CHANGELOG.md
   - Publish release

5. Wait for GitHub Actions to build binaries for all platforms

6. Verify:
   ```bash
   wget https://github.com/maxpert/strangeq/releases/download/v0.1.0/amqp-server-linux-amd64
   wget https://github.com/maxpert/strangeq/releases/download/v0.1.0/amqp-server-linux-amd64.sha256
   sha256sum -c amqp-server-linux-amd64.sha256
   ```

## Post-Release

- [ ] Monitor issue tracker for release-related bugs
- [ ] Create new CHANGELOG section for next version

## Hotfix

```bash
git checkout -b hotfix/0.1.1 v0.1.0
# make fix
git commit -m "fix: critical bug description"
git tag -a v0.1.1 -m "Hotfix v0.1.1"
git push origin v0.1.1
# create GitHub release, then merge back to main
git checkout main
git merge hotfix/0.1.1
git push origin main
```
