# presto-go

A Go client library for [Presto](https://prestodb.io/) and [Trino](https://trino.io/) SQL query engines.

## Features

- **`database/sql` driver** — use the standard Go database API (`sql.Open`, `db.Query`, `rows.Scan`)
- Complete Presto REST API client (query execution, cluster info, query state, query info)
- Query info/stats parsing (`query_json` subpackage)
- Trino compatibility mode (automatic header translation)
- Session management with isolated, cloneable sessions
- Transaction state tracking (automatic via response headers)
- Batch result streaming with memory-efficient `Drain` API
- Automatic retry with exponential backoff on 503 responses and transient connection errors
- Gzip request/response compression
- Thread-safe concurrent session access
- Fluent API for session configuration
- Pre-minted query ID support
- TLS/SSL with custom CAs and mutual TLS
- Kerberos/SPNEGO authentication (opt-in separate module)
- OAuth2/JWT authentication (opt-in separate module)
- Generic complex type scanners (`NullSlice[T]`, `NullMap[K,V]`, `NullRow[T]`)

## Installation

```bash
go get github.com/prestodb/presto-go-client/v2
```

Optional authentication modules (separate dependencies, opt-in):

```bash
go get github.com/prestodb/presto-go-client/v2/prestoauth/kerberos  # Kerberos/SPNEGO
go get github.com/prestodb/presto-go-client/v2/prestoauth/oauth2    # OAuth2/JWT
```

## Quick Start

### Using `database/sql` (recommended)

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/prestodb/presto-go-client/v2" // registers "presto" driver
)

func main() {
	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name FROM users WHERE active = ?", true)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
}
```

### Using the low-level API

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/prestodb/presto-go-client/v2"
)

func main() {
	client, err := presto.NewClient("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	session := client.NewSession()
	session.Catalog("hive").Schema("default").User("analyst")

	ctx := context.Background()
	results, _, err := session.Query(ctx, "SELECT id, name FROM users LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}

	err = results.Drain(ctx, func(qr *presto.QueryResults) error {
		for _, row := range qr.Data {
			var parsed []any
			json.Unmarshal(row, &parsed)
			fmt.Println(parsed)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Usage

### `database/sql` Driver

#### DSN Format

```
presto://[user[:password]@]host[:port][/catalog[/schema]][?key=value&...]
trino://...   (enables Trino header mode)
```

Default port is 8080 for both schemes. Query parameters:

| Parameter | Description |
|-----------|-------------|
| `timezone` | Session time zone |
| `client_tags` | Comma-separated tags |
| `client_info` | Client info string |
| `source` | Query source identifier |
| `ssl_cert` | Path to client certificate (PEM) for mutual TLS |
| `ssl_key` | Path to client private key (PEM) |
| `ssl_ca` | Path to CA certificate (PEM) for custom CAs |
| `ssl_skip_verify` | Skip TLS certificate verification (`true`/`1`) |
| *(other)* | Set as session properties |

When any `ssl_*` parameter is set, the connection automatically upgrades to HTTPS.

#### Using `sql.OpenDB` with a Connector

```go
connector, err := presto.NewConnector("presto://user@host:8080/hive/default")
if err != nil {
    log.Fatal(err)
}
db := sql.OpenDB(connector)
```

#### Parameter Interpolation

The driver interpolates `?` placeholders client-side into SQL literals:

```go
rows, err := db.Query("SELECT * FROM t WHERE name = ? AND id = ?", "alice", 42)
```

#### Transactions

```go
tx, err := db.BeginTx(ctx, nil)
// ... use tx.Query / tx.Exec ...
tx.Commit() // or tx.Rollback()
```

All Presto isolation levels are supported via `sql.TxOptions`:

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelSerializable,
    ReadOnly:  true,
})
```

### Client Initialization

```go
// Basic client
client, err := presto.NewClient("http://presto-coordinator:8080")

// With basic auth
client, err := presto.NewClient("http://presto-coordinator:8080", "base64-encoded-credentials")

// Trino mode with HTTPS
client, err := presto.NewClient("http://trino-coordinator:8443")
client.IsTrino(true).ForceHTTPS(true)
```

### TLS Configuration

#### Via DSN (database/sql)

```go
db, _ := sql.Open("presto", "presto://host:8443/catalog?ssl_ca=/path/ca.pem")
// or with mutual TLS:
db, _ := sql.Open("presto", "presto://host:8443/catalog?ssl_cert=/path/cert.pem&ssl_key=/path/key.pem&ssl_ca=/path/ca.pem")
// or skip verification (development only):
db, _ := sql.Open("presto", "presto://host:8443/catalog?ssl_skip_verify=true")
```

#### Via low-level API

```go
client, _ := presto.NewClient("https://presto:8443")

// Use BuildTLSConfig helper (same logic as DSN parsing):
tlsCfg, _ := presto.BuildTLSConfig("/path/ca.pem", "", "", false)
client.TLSConfig(tlsCfg)

// or with mutual TLS:
tlsCfg, _ = presto.BuildTLSConfig("/path/ca.pem", "/path/cert.pem", "/path/key.pem", false)
client.TLSConfig(tlsCfg)

// or provide a fully custom http.Client:
client.HTTPClient(&http.Client{
    Transport: &http.Transport{TLSClientConfig: tlsCfg},
})
```

#### Via Connector Options

```go
connector, _ := presto.NewConnector("presto://host:8443/catalog",
    presto.WithHTTPClient(customHTTPClient),
)
```

### Session Management

Sessions provide isolated execution contexts. Each session maintains its own catalog, schema, user identity, transaction state, and session parameters.

```go
// Create an isolated session from the client
session := client.NewSession()
session.Catalog("hive").Schema("production").User("etl_user")

// Set session parameters
session.SessionParam("query_max_memory", "2GB")
session.SessionParam("join_distribution_type", "PARTITIONED")

// Clone a session for parallel workloads
s2 := session.Clone()
s2.Schema("staging")
```

### Query Execution

```go
// Simple query
results, _, err := session.Query(ctx, "SELECT * FROM orders WHERE status = 'pending'")

// Query with pre-minted ID (for tracking)
results, _, err := session.QueryWithPreMintedID(ctx, "SELECT 1", "custom-query-id", "slug")

// Manual batch fetching
for results.HasMoreBatch() {
    err := results.FetchNextBatch(ctx)
    if err != nil {
        log.Fatal(err)
    }
    // Process results.Data
}

// Streaming drain (memory-efficient for large result sets)
err = results.Drain(ctx, func(qr *presto.QueryResults) error {
    // Process each batch; data is cleared after handler returns
    fmt.Printf("Batch: %d rows\n", len(qr.Data))
    return nil
})
```

### REST API Endpoints

#### Cluster Info

```go
stats, _, err := session.GetClusterInfo(ctx)
fmt.Printf("Active workers: %d, Running queries: %d\n", stats.ActiveWorkers, stats.RunningQueries)
```

#### Query State

```go
// List all queries for a specific user
user := "analyst"
states, _, err := session.GetQueryState(ctx, &presto.GetQueryStateOptions{
    User: &user,
})
for _, s := range states {
    fmt.Printf("Query %s: %s\n", s.QueryId, s.QueryState)
}
```

#### Query Info

```go
// Decode into a struct
var info query_json.QueryInfo
_, err := session.GetQueryInfo(ctx, "20231001_123456_00001_xxxxx", &info)

// Or write raw JSON to a file
file, _ := os.Create("query.json")
_, err = session.GetQueryInfo(ctx, queryId, file)
```

### Cancellation

Context cancellation automatically triggers server-side query cleanup:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

results, _, err := session.Query(ctx, "SELECT * FROM large_table")
// If context times out during FetchNextBatch, the query is canceled on the server
```

### Request Options

Override individual request settings without modifying the session:

```go
opt := func(r *http.Request) {
    r.Header.Set("X-Custom-Header", "value")
}
results, _, err := session.Query(ctx, "SELECT 1", opt)
```

### Persistent Request Options

Set request options that apply to every request from a session, including batch fetches. This is essential for authentication schemes like Kerberos/SPNEGO:

```go
session.RequestOptions(func(r *http.Request) {
    r.Header.Set("Authorization", "Bearer my-token")
})
results, _, err := session.Query(ctx, "SELECT 1") // header applied
// header also applied to all FetchNextBatch calls
```

Per-call options override session-level options when both set the same header.

### Kerberos/SPNEGO Authentication

The `prestoauth/kerberos` module provides Kerberos authentication as a separate module to keep the `gokrb5` dependency tree opt-in.

#### Using `database/sql`

```go
import (
    "database/sql"
    "github.com/prestodb/presto-go-client/v2/prestoauth/kerberos"
)

connector, closer, err := kerberos.NewConnector(
    "presto://host:8080/catalog/schema?kerberos_keytab=/etc/presto.keytab&kerberos_principal=user@REALM&kerberos_realm=REALM&kerberos_config=/etc/krb5.conf",
)
if err != nil {
    log.Fatal(err)
}
defer closer.Close()

db := sql.OpenDB(connector)
db.Query("SELECT 1") // SPNEGO header applied to all requests
```

DSN parameters for Kerberos:

| Parameter | Description |
|-----------|-------------|
| `kerberos_keytab` | Path to the `.keytab` file |
| `kerberos_principal` | Kerberos principal (e.g. `user@EXAMPLE.COM`) |
| `kerberos_realm` | Kerberos realm (e.g. `EXAMPLE.COM`) |
| `kerberos_config` | Path to `krb5.conf` |
| `kerberos_service_spn` | Service principal name (defaults to `HTTP/<hostname>`) |

#### Using the low-level API

```go
import (
    "github.com/prestodb/presto-go-client/v2"
    "github.com/prestodb/presto-go-client/v2/prestoauth/kerberos"
)

client, _ := presto.NewClient("http://presto:8080")
session := client.NewSession()

krbOpt, closer, err := kerberos.NewRequestOption(kerberos.Config{
    KeytabPath: "/etc/presto.keytab",
    Principal:  "presto/host@EXAMPLE.COM",
    Realm:      "EXAMPLE.COM",
    ConfigPath: "/etc/krb5.conf",
})
if err != nil {
    log.Fatal(err)
}
defer closer.Close()

session.RequestOptions(krbOpt)
results, _, _ := session.Query(ctx, "SELECT 1") // SPNEGO header applied
```

### OAuth2/JWT Authentication

The `prestoauth/oauth2` module provides token-based authentication as a separate module.

#### Static Bearer Token

```go
import "github.com/prestodb/presto-go-client/v2/prestoauth/oauth2"

// Via DSN
connector, _ := oauth2.NewConnector("presto://host:8080/catalog?access_token=my-jwt-token")
db := sql.OpenDB(connector)

// Via low-level API
session.RequestOptions(oauth2.NewStaticTokenOption("my-jwt-token"))
```

#### OAuth2 Client Credentials Flow

Tokens are automatically obtained and refreshed:

```go
// Via DSN
connector, _ := oauth2.NewConnector(
    "presto://host:8080/catalog?oauth2_client_id=id&oauth2_client_secret=secret&oauth2_token_url=https://auth.example.com/token&oauth2_scopes=read,write",
)
db := sql.OpenDB(connector)

// Via low-level API
opt, _ := oauth2.NewRequestOption(oauth2.Config{
    ClientID:     "my-client",
    ClientSecret: "my-secret",
    TokenURL:     "https://auth.example.com/token",
    Scopes:       []string{"read", "write"},
})
session.RequestOptions(opt)
```

DSN parameters for OAuth2:

| Parameter | Description |
|-----------|-------------|
| `access_token` | Static Bearer token |
| `oauth2_client_id` | OAuth2 client ID |
| `oauth2_client_secret` | OAuth2 client secret |
| `oauth2_token_url` | Token endpoint URL |
| `oauth2_scopes` | Comma-separated scopes |

### Complex Type Scanning

Presto `ARRAY`, `MAP`, and `ROW` columns are returned as JSON strings through `database/sql`. Use the provided generic scanner types to deserialize them:

```go
// ARRAY columns
var tags presto.NullSlice[string]
row.Scan(&tags)
fmt.Println(tags.Slice) // []string{"go", "presto"}

// MAP columns
var props presto.NullMap[string, float64]
row.Scan(&props)
fmt.Println(props.Map) // map[string]float64{"timeout": 30}

// ROW columns (scan into a struct)
type Address struct {
    Street string `json:"street"`
    City   string `json:"city"`
}
var addr presto.NullRow[Address]
row.Scan(&addr)
fmt.Println(addr.Row.Street) // "123 Main St"
```

All three types are nullable (`Valid` field), implement `sql.Scanner` and `driver.Valuer`, and support any JSON-compatible type parameter.

### Interval Types

`INTERVAL DAY TO SECOND` columns are scanned as `time.Duration`. `INTERVAL YEAR TO MONTH` columns are scanned as `string` (in Presto's `"Y-M"` format, e.g. `"3-6"` for 3 years 6 months) since months and years are not fixed-length durations.

When passing parameters, `time.Duration` values are interpolated as `INTERVAL '...' DAY TO SECOND`:

```go
rows, err := db.QueryContext(ctx, "SELECT date_add('day', ?, now())", 2*24*time.Hour+6*time.Hour)
```

### Connector Options

`NewConnector` accepts variadic options for configuring sessions created by the connector:

```go
connector, err := presto.NewConnector("presto://host:8080/hive/default",
    presto.WithSessionSetup(func(s *presto.Session) {
        s.RequestOptions(myAuthOption)
    }),
    presto.WithHTTPClient(customHTTPClient),
)
db := sql.OpenDB(connector)
```

## Testing

```bash
go test ./... -v
```

### Examples

The `example_test.go` file contains runnable getting-started snippets covering both the `database/sql` interface and the low-level API. All examples are skipped by default since they require a live Presto/Trino server:

```bash
go test -run TestExample -v
```

### Mock Server

The `prestotest` package provides a `MockPrestoServer` for integration testing. It uses only the standard library (`net/http`), so it introduces no additional dependencies.

```go
import (
    "github.com/prestodb/presto-go-client/v2"
    "github.com/prestodb/presto-go-client/v2/prestotest"
)

func TestMyApp(t *testing.T) {
    mock := prestotest.NewMockPrestoServer()
    defer mock.Close()

    mock.AddQuery(&prestotest.MockQueryTemplate{
        SQL:         "SELECT * FROM users",
        Columns:     []presto.Column{{Name: "id", Type: "bigint"}},
        Data:        [][]any{{1}, {2}, {3}},
        DataBatches: 2,
    })

    client, _ := presto.NewClient(mock.URL())
    session := client.NewSession()

    results, _, err := session.Query(context.Background(), "SELECT * FROM users")
    // Assert on results...
}
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
