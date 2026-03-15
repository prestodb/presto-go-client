package presto_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/prestodb/presto-go-client/v2"
	"github.com/prestodb/presto-go-client/v2/query_json"
)

// =============================================================================
// Getting Started Examples
//
// These tests serve as executable documentation showing how to use presto-go.
// They are skipped by default because they require a running Presto/Trino server.
//
// To run against a local cluster:
//   go test -run TestExample -v -args -presto-url=http://localhost:8080
// =============================================================================

const prestoURL = "http://localhost:8080"

// --- database/sql Interface ---

func TestExample_DatabaseSQL_BasicQuery(t *testing.T) {
	t.Skip("requires a running Presto server")

	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT 1 AS id, 'hello' AS greeting")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var greeting string
		if err := rows.Scan(&id, &greeting); err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, greeting)
	}
}

func TestExample_DatabaseSQL_ParameterInterpolation(t *testing.T) {
	t.Skip("requires a running Presto server")

	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// The driver interpolates ? placeholders client-side into SQL literals.
	rows, err := db.QueryContext(context.Background(),
		"SELECT * FROM users WHERE name = ? AND active = ? AND created > ?",
		"alice", true, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		// scan columns...
		_ = rows
	}
}

func TestExample_DatabaseSQL_Transactions(t *testing.T) {
	t.Skip("requires a running Presto server")

	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// All Presto isolation levels are supported.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := tx.QueryContext(ctx, "SELECT count(*) FROM orders")
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int64
		rows.Scan(&count)
		fmt.Println("order count:", count)
	}

	tx.Commit()
}

func TestExample_DatabaseSQL_ComplexTypes(t *testing.T) {
	t.Skip("requires a running Presto server")

	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	row := db.QueryRowContext(context.Background(),
		"SELECT ARRAY['go', 'presto'], MAP(ARRAY['timeout'], ARRAY[30]), CAST(ROW('123 Main St', 'Springfield') AS ROW(street VARCHAR, city VARCHAR))",
	)

	// ARRAY columns
	var tags presto.NullSlice[string]
	// MAP columns
	var props presto.NullMap[string, float64]
	// ROW columns (scan into a struct)
	type Address struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}
	var addr presto.NullRow[Address]

	if err := row.Scan(&tags, &props, &addr); err != nil {
		log.Fatal(err)
	}

	fmt.Println("tags:", tags.Slice)    // [go presto]
	fmt.Println("props:", props.Map)    // map[timeout:30]
	fmt.Println("city:", addr.Row.City) // Springfield
}

func TestExample_DatabaseSQL_IntervalTypes(t *testing.T) {
	t.Skip("requires a running Presto server")

	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// INTERVAL DAY TO SECOND is scanned as time.Duration.
	var interval time.Duration
	err = db.QueryRowContext(ctx, "SELECT INTERVAL '2' DAY + INTERVAL '6' HOUR").Scan(&interval)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("interval:", interval) // 54h0m0s

	// INTERVAL YEAR TO MONTH is scanned as string ("Y-M" format).
	var yearToMonth string
	err = db.QueryRowContext(ctx, "SELECT INTERVAL '3' YEAR + INTERVAL '6' MONTH").Scan(&yearToMonth)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("interval:", yearToMonth) // 3-6

	// Passing time.Duration as a parameter interpolates as INTERVAL DAY TO SECOND.
	rows, err := db.QueryContext(ctx,
		"SELECT date_add('second', to_unixtime(now()) - to_unixtime(now() - ?), 0)",
		2*24*time.Hour+6*time.Hour,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func TestExample_DatabaseSQL_TLS(t *testing.T) {
	t.Skip("requires a running Presto server with TLS")

	// Custom CA certificate
	_, _ = sql.Open("presto", "presto://host:8443/catalog?ssl_ca=/path/ca.pem")

	// Mutual TLS (client certificate)
	_, _ = sql.Open("presto", "presto://host:8443/catalog?ssl_cert=/path/cert.pem&ssl_key=/path/key.pem&ssl_ca=/path/ca.pem")

	// Skip verification (development only)
	_, _ = sql.Open("presto", "presto://host:8443/catalog?ssl_skip_verify=true")
}

func TestExample_DatabaseSQL_ConnectorOptions(t *testing.T) {
	t.Skip("requires a running Presto server")

	// Use sql.OpenDB with a Connector for programmatic configuration.
	connector, err := presto.NewConnector("presto://localhost:8080/hive/default",
		presto.WithSessionSetup(func(s *presto.Session) {
			s.SessionParam("query_max_memory", "2GB")
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var n int64
	db.QueryRowContext(context.Background(), "SELECT 42").Scan(&n)
	fmt.Println(n)
}

func TestExample_DatabaseSQL_Trino(t *testing.T) {
	t.Skip("requires a running Trino server")

	// Use the trino:// scheme to enable automatic Trino header translation.
	db, err := sql.Open("presto", "trino://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var greeting string
	db.QueryRowContext(context.Background(), "SELECT 'hello from trino'").Scan(&greeting)
	fmt.Println(greeting)
}

// --- Low-Level API ---

func TestExample_LowLevel_BasicQuery(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}

	session := client.NewSession()
	session.Catalog("hive").Schema("default")

	ctx := context.Background()
	results, _, err := session.Query(ctx, "select count(*) from orders")
	if err != nil {
		log.Fatal(err)
	}

	// Drain streams all batches, calling the handler for each batch.
	// Data is cleared after the handler returns, keeping memory usage low.
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

func TestExample_LowLevel_BatchFetching(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}

	session := client.NewSession()
	session.Catalog("hive").Schema("default")

	ctx := context.Background()
	results, _, err := session.Query(ctx, "SELECT * FROM large_table")
	if err != nil {
		log.Fatal(err)
	}

	// Manual batch-by-batch fetching for full control.
	totalRows := 0
	for results.HasMoreBatch() {
		if err := results.FetchNextBatch(ctx); err != nil {
			log.Fatal(err)
		}
		totalRows += len(results.Data)
		fmt.Printf("Fetched batch: %d rows (total: %d)\n", len(results.Data), totalRows)
	}
}

func TestExample_LowLevel_SessionIsolation(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}

	// Configure the default session on the client.
	client.User("default_user").Catalog("hive")

	// Create isolated sessions for different workloads.
	// Each session maintains independent state (catalog, schema, user, params).
	etlSession := client.NewSession()
	etlSession.Schema("staging").User("etl_service")
	etlSession.SessionParam("query_max_memory", "8GB")

	analyticsSession := client.NewSession()
	analyticsSession.Schema("production").User("analyst")
	analyticsSession.SessionParam("query_max_memory", "2GB")

	// Clone a session for a one-off workload.
	tempSession := etlSession.Clone()
	tempSession.Schema("temp")

	_ = analyticsSession
	_ = tempSession
}

func TestExample_LowLevel_Cancellation(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}
	session := client.NewSession().Catalog("hive").Schema("default")

	// Context cancellation automatically sends DELETE to cancel the query server-side.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, _, err := session.Query(ctx, "SELECT * FROM very_large_table")
	if err != nil {
		log.Fatal(err)
	}

	err = results.Drain(ctx, func(qr *presto.QueryResults) error {
		fmt.Printf("Processing %d rows\n", len(qr.Data))
		return nil
	})
	if err != nil {
		// If the context times out, the query is automatically canceled on the server.
		fmt.Println("Query stopped:", err)
	}
}

func TestExample_LowLevel_ClusterInfo(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}
	session := client.NewSession()

	ctx := context.Background()
	stats, _, err := session.GetClusterInfo(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Active workers: %d\n", stats.ActiveWorkers)
	fmt.Printf("Running queries: %d\n", stats.RunningQueries)
	fmt.Printf("Queued queries: %d\n", stats.QueuedQueries)
	fmt.Printf("Blocked queries: %d\n", stats.BlockedQueries)
}

func TestExample_LowLevel_QueryState(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}
	session := client.NewSession()

	ctx := context.Background()

	// List all queries for a specific user.
	user := "analyst"
	states, _, err := session.GetQueryState(ctx, &presto.GetQueryStateOptions{
		User: &user,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range states {
		fmt.Printf("Query %s: %s (created %s)\n", s.QueryId, s.QueryState, s.CreateTime)
	}
}

func TestExample_LowLevel_QueryInfo(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}
	session := client.NewSession().Catalog("hive").Schema("default")

	ctx := context.Background()

	// First, run a query to get a query ID.
	results, _, err := session.Query(ctx, "SELECT count(*) FROM orders")
	if err != nil {
		log.Fatal(err)
	}
	results.Drain(ctx, nil)
	queryId := results.Id

	// Fetch detailed query info (stats, stages, operators).
	var info query_json.QueryInfo
	_, err = session.GetQueryInfo(ctx, queryId, &info)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Query %s: state=%s\n", info.QueryId, info.State)
	if info.QueryStats != nil {
		fmt.Printf("  Elapsed time: %s\n", info.QueryStats.ElapsedTime)
		fmt.Printf("  CPU time: %s\n", info.QueryStats.TotalCpuTime)
	}
}

func TestExample_LowLevel_TLS(t *testing.T) {
	t.Skip("requires a running Presto server with TLS")

	// BuildTLSConfig is the same helper used internally by the database/sql driver.
	// Use it to create a *tls.Config for the low-level API.
	tlsCfg, err := presto.BuildTLSConfig("/path/ca.pem", "", "", false)
	if err != nil {
		log.Fatal(err)
	}

	client, err := presto.NewClient("https://presto:8443")
	if err != nil {
		log.Fatal(err)
	}
	client.TLSConfig(tlsCfg)

	// For mutual TLS, provide client cert and key:
	tlsCfg, err = presto.BuildTLSConfig("/path/ca.pem", "/path/cert.pem", "/path/key.pem", false)
	if err != nil {
		log.Fatal(err)
	}
	client.TLSConfig(tlsCfg)
}

func TestExample_LowLevel_RequestOptions(t *testing.T) {
	t.Skip("requires a running Presto server")

	client, err := presto.NewClient(prestoURL)
	if err != nil {
		log.Fatal(err)
	}
	session := client.NewSession().Catalog("hive").Schema("default")

	// Persistent request options apply to every request from this session,
	// including FetchNextBatch calls. This is how auth modules inject tokens.
	session.RequestOptions(func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer my-token")
	})

	ctx := context.Background()
	results, _, err := session.Query(ctx, "SELECT 1")
	if err != nil {
		log.Fatal(err)
	}
	// The Authorization header is also sent on all subsequent batch fetches.
	results.Drain(ctx, nil)
}
