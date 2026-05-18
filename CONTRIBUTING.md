# Contributing

Thanks for contributing to azkit.

## Setup

1. Fork and clone the repository.
2. Install dependencies: `go mod download`
3. Run tests: `go test ./...`

## Pull requests

- Run `go fmt ./...` and `go vet ./...` before opening a PR.
- Add tests for new behavior in `tables` and `credentials`.
- Update `docs/` and the root README when public APIs change.
- Keep changes focused; prefer small, reviewable PRs.

## Changelog

Document user-facing changes under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md).
