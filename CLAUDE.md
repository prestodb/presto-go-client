# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Go client library for [Presto](https://prestodb.io/) and [Trino](https://trino.io/) SQL query engines. Provides:
- Complete Presto REST API client (query execution, cluster info, query state, query info)
- Go `database/sql` driver (registered as `"presto"`)
- Query info/stats parsing (`query_json` subpackage)

## Commands

```bash
# Run all tests (root module)
go test ./... -race -count=1

# Run a single test
go test ./... -run TestFunctionName -race -count=1

# Run submodule tests (separate Go modules, must cd first)
cd prestoauth/kerberos && go test ./... -race -count=1
cd prestoauth/oauth2 && go test ./... -race -count=1

# Coverage (CI enforces 80% threshold)
go test ./... -coverprofile=coverage.out -covermode=atomic \
  -coverpkg=github.com/prestodb/presto-go-client/v2,github.com/prestodb/presto-go-client/v2/utils
go tool cover -func=coverage.out | grep total

# Lint
go vet ./...
staticcheck ./...

# Vulnerability check
govulncheck ./...

# Bump Go version across all modules + CI
./scripts/bump-go-version.sh <version>  # e.g., 1.26.1
```

CI also includes a `release.yml` workflow for tagged releases.

## Architecture

### Module Structure

Three separate Go modules share this repo. Auth modules are opt-in to avoid forcing heavy dependencies (gokrb5, oauth2) on all consumers.

```
github.com/prestodb/presto-go-client/v2           # root module (presto package)
├── utils/                                  # BiMap utility (subpackage, same module)
├── query_json/                             # Query info/stats types (subpackage, same module)
├── prestotest/                             # MockPrestoServer (subpackage, same module)
├── prestoauth/kerberos/                    # separate module (gokrb5 dep)
└── prestoauth/oauth2/                      # separate module (x/oauth2 dep)
```

Submodules use `replace github.com/prestodb/presto-go-client/v2 => ../..` for local dev.

### Client / Session

`Client` embeds `Session` (circular ref: `Session.client` points back to `Client`). Client owns the `httpClient` and server URL. Session owns isolated state (catalog, schema, user, transaction ID, session params, request options).

`NewSession()` clones the embedded default session. Sessions are thread-safe via `Session.mu` (`sync.RWMutex`). Client-level fields (`httpClient`, `serverUrl`, `isTrino`, `forceHTTPS`) are separately protected by `Client.clientMu`. All session setters use the fluent pattern (return `*Session`).

Persistent `RequestOptions` on Session apply to **every** request including `FetchNextBatch` — this is how auth modules (Kerberos, OAuth2) ensure tokens are sent on all requests, not just the initial query.

### Driver (database/sql)

`driver.go` implements `driver.Driver`, `driver.Connector`, `driver.Conn`, `driver.Rows`, `driver.Stmt`, and `driver.Tx`. The `init()` function registers the `"presto"` driver.

DSN format: `presto://[user[:password]@]host[:port][/catalog[/schema]][?params]` (also `trino://` for Trino mode). Parameter interpolation replaces `?` placeholders client-side into SQL literals.

Core DSN params: `timezone`, `client_tags`, `client_info`, `source`. TLS params: `ssl_cert`, `ssl_key`, `ssl_ca`, `ssl_skip_verify` (any ssl_* param auto-upgrades scheme to HTTPS). Unrecognized params become session properties.

Auth module DSN params are parsed and stripped by each module before passing the cleaned DSN to `presto.NewConnector`:
- **Kerberos**: `kerberos_keytab`, `kerberos_principal`, `kerberos_realm`, `kerberos_config`, `kerberos_service_spn`
- **OAuth2**: `access_token` (static token) or `oauth2_client_id`, `oauth2_client_secret`, `oauth2_token_url`, `oauth2_scopes` (client credentials flow)

`ConnectorOption` functions (`WithSessionSetup`, `WithHTTPClient`) configure the connector. Auth modules use `WithSessionSetup` to inject request options into every session created by `Connect()`.

Complex Presto types (ARRAY, MAP, ROW) are returned as JSON strings through the driver. `NullSlice[T]`, `NullMap[K,V]`, and `NullRow[T]` in `types.go` implement `sql.Scanner` for deserializing them.

Interval types: `INTERVAL DAY TO SECOND` maps to `time.Duration`; `INTERVAL YEAR TO MONTH` maps to `string` (Presto's `"Y-M"` wire format). `time.Duration` query parameters are interpolated as `INTERVAL '...' DAY TO SECOND`.

`BuildTLSConfig(caFile, certFile, keyFile, skipVerify)` is an exported helper for constructing `*tls.Config` — shared by the driver's DSN parsing and available to low-level `Client` API users.

Varbinary values are base64-decoded from Presto's wire format. `time` and `time with time zone` types are parsed to `time.Time`. Closed connections return `driver.ErrBadConn`. The `prestoConn.closed` field uses `atomic.Bool` for thread-safe access.

Transactions store the context from `BeginTx` and use it for `Commit`/`Rollback`, so the caller's deadline/cancellation applies to the entire transaction lifecycle.

### Query Lifecycle

`Session.Query()` → POST to `/v1/statement` → returns `QueryResults`. Results are fetched batch-by-batch via `FetchNextBatch()` or streamed via `Drain()`. Context cancellation triggers a DELETE request to cancel the query server-side. `FetchNextBatch` uses a local variable to iterate through empty batches and only commits the final state to the caller's `QueryResults` on success, preventing partial mutation on error.

### Test Patterns

- **Internal tests** (`package presto`): `client_test.go`, `driver_internal_test.go`, `types_test.go` — access unexported symbols
- **External tests** (`package presto_test`): `driver_test.go`, `query_test.go` — test through exported API using `prestotest.MockPrestoServer`
- Mock server uses only stdlib `net/http`. Register queries with `AddQuery()`, control batching with `DataBatches`/`QueueBatches` fields
- Uses `testify/assert` and `testify/require`

### Trino Compatibility

`Client.IsTrino(true)` causes `CanonicalHeader()` to rewrite `X-Presto-*` headers to `X-Trino-*`. The DSN scheme `trino://` enables this automatically.

## Workflow

- Commit messages must follow semantic format: `<type>: <description>` or `<type>(<scope>): <description>`
  - Valid types: `feat`, `fix`, `refactor`, `test`, `docs`, `ci`, `chore`, `perf`, `style`, `build`, `revert`
  - CI enforces this on pull requests
- After completing any code change, always check whether README.md, CLAUDE.md, or code comments need to be updated to reflect the change.
- Before finishing, always run `gofmt -w .`, `go vet ./...`, `staticcheck ./...`, and `go mod tidy` to ensure no formatting or lint issues remain.
- A git pre-commit hook in `.githooks/pre-commit` runs fmt, vet, tests, and coverage across all modules. Activate with: `git config core.hooksPath .githooks`
