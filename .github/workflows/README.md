# GitHub Actions workflows

| Workflow | Triggers | Checks or outputs |
|---|---|---|
| [Build and Test](build.yml) | Pushes and pull requests to `main` or `develop` | Go 1.27.3 tests with race detection and coverage, formatting, vet, binary/version verification, systemd smoke test, and shell syntax checks |
| [Release Build](release.yml) | `v*` tag pushes, release creation/publication, or manual dispatch | macOS (`amd64`, `arm64`) and Linux (`amd64`, `arm64`, `386`) archives and SHA256 checksums |
| [CodeQL](codeql.yml) | Pushes and pull requests to `main`, plus Mondays at 00:00 UTC | Go security and quality analysis |

Release and tag events upload assets to the GitHub release, creating the release
if needed. Manual dispatch accepts a version label and builds downloadable
workflow artifacts; it does not create a release or upload release assets.

[Dependabot](../dependabot.yml) checks Go modules and GitHub Actions weekly on
Mondays. It is configured separately from the workflows.

## Local checks

From the repository root:

```sh
cd src/amqp-go
go test -race -timeout 25m ./...
go build ./cmd/amqp-server
```

See [Contributing](../../CONTRIBUTING.md) for formatting, package checks, and
development setup. The workflows pin Go 1.27.3.

## Troubleshooting

For build failures, inspect the failing job's log and reproduce its command from
`src/amqp-go`. The full race-enabled test suite can take several minutes.

For missing release assets, confirm the run used a `v*` tag push or a release
event, then inspect both the build jobs and the Create Release Assets job in
Actions. A manual run leaves its packages under the run's artifacts.
