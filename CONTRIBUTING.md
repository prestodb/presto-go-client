# Contributing to presto-go

Thanks for your interest in the Presto Go client library. We welcome contributions from everyone.

## Getting Started

1. Fork and clone the repository
2. Ensure you have Go 1.26 or newer installed
3. Install required tools:
   ```bash
   go install honnef.co/go/tools/cmd/staticcheck@latest
   go install golang.org/x/vuln/cmd/govulncheck@latest
   ```
4. Run `go mod download` to fetch dependencies
5. Run `go test ./... -race -count=1` to verify everything works

## Development

### Module Structure

This repo contains three separate Go modules. The auth modules are separate to avoid forcing heavy dependencies (gokrb5, oauth2) on all consumers.

```
github.com/prestodb/presto-go-client/v2        # root module
├── prestoauth/kerberos/                     # separate module (gokrb5 dep)
└── prestoauth/oauth2/                       # separate module (x/oauth2 dep)
```

Submodules use `replace` directives for local development. When working on a submodule, `cd` into it before running commands.

### Running Tests

```bash
# Root module
go test ./... -race -count=1

# Submodules (must cd first)
cd prestoauth/kerberos && go test ./... -race -count=1
cd prestoauth/oauth2 && go test ./... -race -count=1
```

### Linting and Formatting

All code must pass these checks before submitting a PR:

```bash
gofmt -w .
go vet ./...
staticcheck ./...
```

### Vulnerability Scanning

```bash
govulncheck ./...
```

### Coverage

CI enforces an 80% coverage threshold on the root module:

```bash
go test ./... -coverprofile=coverage.out -covermode=atomic \
  -coverpkg=github.com/prestodb/presto-go-client/v2,github.com/prestodb/presto-go-client/v2/utils
go tool cover -func=coverage.out | grep total
```

### Bumping the Go Version

Use the provided script to update all `go.mod` files and the CI workflow at once:

```bash
./scripts/bump-go-version.sh 1.26.1
```

After running the script, follow the printed instructions to run `go mod tidy`, tests, and lint on all modules. If you upgraded to a new major Go release, rebuild your tools (`staticcheck`, `govulncheck`) with `go install` so they match.

### Before Submitting

Run the full check suite:

```bash
gofmt -w .
go mod tidy
go vet ./...
staticcheck ./...
go test ./... -race -count=1
govulncheck ./...
```

Repeat for each submodule you changed.

## Commit Standards

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification. CI enforces this on all PR commits.

**Format:**
```
<type>: <description>
```

**Types:**
- `feat` — new feature or functionality
- `fix` — bug fix
- `docs` — documentation only changes
- `refactor` — code refactoring without changing functionality
- `perf` — performance improvements
- `test` — adding or modifying tests
- `build` — build system or dependency changes
- `ci` — CI/CD configuration changes
- `chore` — maintenance tasks
- `revert` — reverting a previous commit
- `style` — formatting, whitespace (no logic changes)

**Examples:**
- `feat: add support for INTERVAL YEAR TO MONTH type`
- `fix: handle nil pointer in query stats parsing`
- `test: add coverage for TLS DSN parameters`
- `docs: update README authentication section`

## Pull Requests

- Keep PRs focused on a single topic
- Include tests for all bug fixes and new features
- Ensure all checks pass before requesting review
- Explain *what* and *why* in the PR description, not *how*

## Contributor License Agreement ("CLA")

To accept your pull request, you must submit a CLA. You only need to do this once for any repository in the [prestodb](https://github.com/prestodb) organization. When you submit a pull request for the first time, the communitybridge-easycla bot will notify you if you haven't signed and provide a link. If you are contributing on behalf of a company, let the person who manages your corporate CLA whitelist know they will be receiving a request from you.

## Conduct

Please refer to our [Code of Conduct](https://github.com/prestodb/tsc/blob/master/CODE_OF_CONDUCT.md).

## License

By contributing to this project, you agree that your contributions will be licensed under the [Apache License Version 2.0](LICENSE).
