package presto_test

import (
	"context"
	"database/sql"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/prestodb/presto-go-client/v2"
	"github.com/prestodb/presto-go-client/v2/prestotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Unit Tests (no server needed, but using presto_test package
// so we test the exported helpers and use the driver via sql.Open)
// ============================================================

// Since parseDSN, interpolateParams, etc. are unexported, we test them
// indirectly through the exported driver surface (sql.Open, NewConnector).
// For direct unit testing we use a separate internal test file or test
// observable behavior through the driver.

func TestParseDSN_ViaNewConnector(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{
			name: "basic presto",
			dsn:  "presto://localhost:8080/hive/default",
		},
		{
			name: "trino with user",
			dsn:  "trino://admin@trino-host:443/catalog/schema",
		},
		{
			name: "presto with user and password",
			dsn:  "presto://user:pass@host:9090/cat/sch",
		},
		{
			name: "presto minimal",
			dsn:  "presto://localhost",
		},
		{
			name: "presto with query params",
			dsn:  "presto://localhost/cat/sch?timezone=US/Eastern&client_tags=a,b&client_info=test&source=myapp&custom_prop=val",
		},
		{
			name:    "unsupported scheme",
			dsn:     "mysql://localhost/db",
			wantErr: true,
		},
		{
			name:    "missing host",
			dsn:     "presto:///catalog",
			wantErr: true,
		},
		{
			name:    "invalid URL",
			dsn:     "://bad",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := presto.NewConnector(tt.dsn)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, connector)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, connector)
			}
		})
	}
}

// ============================================================
// Internal unit tests via exported driver behavior
// ============================================================

// We test interpolation and type conversion indirectly through
// the QueryContext path, but also provide a direct internal test file.

// ============================================================
// Integration Tests (via MockPrestoServer)
// ============================================================

func newTestDB(t *testing.T, serverURL string) *sql.DB {
	t.Helper()
	// The mock server uses plain HTTP, so we use presto:// scheme
	// and strip the http:// prefix to build the DSN.
	host := serverURL[len("http://"):]
	dsn := "presto://" + host
	db, err := sql.Open("presto", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestDriver_QueryAndScan(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT id, name, score, active FROM users",
		Columns: []presto.Column{
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "varchar"},
			{Name: "score", Type: "double"},
			{Name: "active", Type: "boolean"},
		},
		Data: [][]any{
			{1, "alice", 95.5, true},
			{2, "bob", 82.3, false},
		},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	rows, err := db.QueryContext(context.Background(), "SELECT id, name, score, active FROM users")
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id     int64
		name   string
		score  float64
		active bool
	}

	for rows.Next() {
		var r struct {
			id     int64
			name   string
			score  float64
			active bool
		}
		err := rows.Scan(&r.id, &r.name, &r.score, &r.active)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "alice", results[0].name)
	assert.InDelta(t, 95.5, results[0].score, 0.001)
	assert.True(t, results[0].active)

	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "bob", results[1].name)
	assert.False(t, results[1].active)
}

func TestDriver_ExecContext(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// Register a DML query template with an update count
	updateCount := int64(42)
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "INSERT INTO t SELECT * FROM s",
		Columns:     []presto.Column{},
		Data:        [][]any{},
		DataBatches: 0,
	})

	db := newTestDB(t, mock.URL())

	// The mock server doesn't set UpdateCount, so we'll get 0.
	// But we can verify the exec path works without error.
	result, err := db.ExecContext(context.Background(), "INSERT INTO t SELECT * FROM s")
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	_ = affected
	_ = updateCount

	_, err = result.LastInsertId()
	assert.Error(t, err, "LastInsertId should not be supported")
}

func TestDriver_MultipleBatches(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SELECT n FROM numbers",
		Columns: []presto.Column{{Name: "n", Type: "integer"}},
		Data: [][]any{
			{1}, {2}, {3}, {4}, {5},
			{6}, {7}, {8}, {9}, {10},
		},
		DataBatches: 3,
	})

	db := newTestDB(t, mock.URL())

	rows, err := db.QueryContext(context.Background(), "SELECT n FROM numbers")
	require.NoError(t, err)
	defer rows.Close()

	var nums []int64
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		nums = append(nums, n)
	}
	require.NoError(t, rows.Err())

	assert.Len(t, nums, 10)
	for i, n := range nums {
		assert.Equal(t, int64(i+1), n)
	}
}

func TestDriver_ColumnTypes(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT a, b, c, d FROM typed",
		Columns: []presto.Column{
			{Name: "a", Type: "bigint"},
			{Name: "b", Type: "varchar(255)"},
			{Name: "c", Type: "double"},
			{Name: "d", Type: "boolean"},
		},
		Data:        [][]any{{1, "hello", 3.14, true}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	rows, err := db.QueryContext(context.Background(), "SELECT a, b, c, d FROM typed")
	require.NoError(t, err)
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, colTypes, 4)

	assert.Equal(t, "BIGINT", colTypes[0].DatabaseTypeName())
	assert.Equal(t, "VARCHAR", colTypes[1].DatabaseTypeName())
	assert.Equal(t, "DOUBLE", colTypes[2].DatabaseTypeName())
	assert.Equal(t, "BOOLEAN", colTypes[3].DatabaseTypeName())

	assert.Equal(t, reflect.TypeOf(int64(0)), colTypes[0].ScanType())
	assert.Equal(t, reflect.TypeOf(""), colTypes[1].ScanType())
	assert.Equal(t, reflect.TypeOf(float64(0)), colTypes[2].ScanType())
	assert.Equal(t, reflect.TypeOf(false), colTypes[3].ScanType())
}

func TestDriver_Transaction(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// Register the transaction SQL statements
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "START TRANSACTION",
		DataBatches: 0,
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "COMMIT",
		DataBatches: 0,
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "ROLLBACK",
		DataBatches: 0,
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 1",
		Columns:     []presto.Column{{Name: "result", Type: "integer"}},
		Data:        [][]any{{1}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	t.Run("Commit", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)

		rows, err := tx.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)
		rows.Close()

		err = tx.Commit()
		assert.NoError(t, err)
	})

	t.Run("Rollback", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)

		err = tx.Rollback()
		assert.NoError(t, err)
	})
}

func TestDriver_TransactionOptions(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		DataBatches: 0,
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "START TRANSACTION READ ONLY",
		DataBatches: 0,
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
		DataBatches: 0,
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "COMMIT",
		DataBatches: 0,
	})

	db := newTestDB(t, mock.URL())

	t.Run("isolation level", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: sql.LevelSerializable,
		})
		require.NoError(t, err)
		assert.NoError(t, tx.Commit())
	})

	t.Run("read only", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
			ReadOnly: true,
		})
		require.NoError(t, err)
		assert.NoError(t, tx.Commit())
	})

	t.Run("isolation level and read only", func(t *testing.T) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
			ReadOnly:  true,
		})
		require.NoError(t, err)
		assert.NoError(t, tx.Commit())
	})
}

func TestDriver_OpenDB(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 1",
		Columns:     []presto.Column{{Name: "result", Type: "integer"}},
		Data:        [][]any{{1}},
		DataBatches: 1,
	})

	host := mock.URL()[len("http://"):]
	connector, err := presto.NewConnector("presto://" + host)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	defer db.Close()

	var result int64
	err = db.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result)
}

func TestDriver_NullValues(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT name, age FROM nullable",
		Columns: []presto.Column{
			{Name: "name", Type: "varchar"},
			{Name: "age", Type: "integer"},
		},
		Data: [][]any{
			{"alice", 30},
			{nil, nil},
			{"charlie", nil},
		},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	rows, err := db.QueryContext(context.Background(), "SELECT name, age FROM nullable")
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		name sql.NullString
		age  sql.NullInt64
	}
	var results []row

	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.name, &r.age))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)

	assert.True(t, results[0].name.Valid)
	assert.Equal(t, "alice", results[0].name.String)
	assert.True(t, results[0].age.Valid)
	assert.Equal(t, int64(30), results[0].age.Int64)

	assert.False(t, results[1].name.Valid)
	assert.False(t, results[1].age.Valid)

	assert.True(t, results[2].name.Valid)
	assert.Equal(t, "charlie", results[2].name.String)
	assert.False(t, results[2].age.Valid)
}

func TestDriver_QueryError(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT * FROM nonexistent",
		Error: &presto.QueryError{
			Message:   "Table 'nonexistent' does not exist",
			ErrorCode: 1,
			ErrorName: "TABLE_NOT_FOUND",
			ErrorType: "USER_ERROR",
		},
		DataBatches: 0,
	})

	db := newTestDB(t, mock.URL())

	_, err := db.QueryContext(context.Background(), "SELECT * FROM nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TABLE_NOT_FOUND")
}

func TestDriver_TimestampTypes(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT d, ts FROM dates",
		Columns: []presto.Column{
			{Name: "d", Type: "date"},
			{Name: "ts", Type: "timestamp"},
		},
		Data: [][]any{
			{"2024-01-15", "2024-01-15 10:30:00.000"},
		},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	rows, err := db.QueryContext(context.Background(), "SELECT d, ts FROM dates")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var d, ts time.Time
	require.NoError(t, rows.Scan(&d, &ts))

	assert.Equal(t, 2024, d.Year())
	assert.Equal(t, time.January, d.Month())
	assert.Equal(t, 15, d.Day())

	assert.Equal(t, 10, ts.Hour())
	assert.Equal(t, 30, ts.Minute())
}

func TestDriver_DecimalType(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT price FROM items",
		Columns: []presto.Column{
			{Name: "price", Type: "decimal(10,2)"},
		},
		Data:        [][]any{{19.99}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	var price string
	err := db.QueryRowContext(context.Background(), "SELECT price FROM items").Scan(&price)
	require.NoError(t, err)
	assert.Equal(t, "19.99", price)
}

func TestDriver_PreparedStatement(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// The interpolated query will be "SELECT 42" after param substitution,
	// but the mock server matches on the literal SQL. Register a template
	// that matches common interpolated results.
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 42",
		Columns:     []presto.Column{{Name: "result", Type: "integer"}},
		Data:        [][]any{{42}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	stmt, err := db.Prepare("SELECT ?")
	require.NoError(t, err)
	defer stmt.Close()

	var result int64
	err = stmt.QueryRowContext(context.Background(), 42).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result)
}

func TestDriver_ParamInterpolation(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// Test string escaping: the interpolated query should be:
	// SELECT 'O''Brien'
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 'O''Brien'",
		Columns:     []presto.Column{{Name: "name", Type: "varchar"}},
		Data:        [][]any{{"O'Brien"}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	var name string
	err := db.QueryRowContext(context.Background(), "SELECT ?", "O'Brien").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "O'Brien", name)
}

func TestDriver_BoolParam(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT TRUE",
		Columns:     []presto.Column{{Name: "result", Type: "boolean"}},
		Data:        [][]any{{true}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	var result bool
	err := db.QueryRowContext(context.Background(), "SELECT ?", true).Scan(&result)
	require.NoError(t, err)
	assert.True(t, result)
}

func TestDriver_NullParam(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT NULL",
		Columns:     []presto.Column{{Name: "result", Type: "varchar"}},
		Data:        [][]any{{nil}},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())

	var result sql.NullString
	err := db.QueryRowContext(context.Background(), "SELECT ?", nil).Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid)
}

// TestDriver_QueuedBatches verifies the driver handles queries that start in QUEUED state.
func TestDriver_QueuedBatches(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:          "SELECT n FROM queued",
		Columns:      []presto.Column{{Name: "n", Type: "integer"}},
		Data:         [][]any{{1}, {2}, {3}},
		DataBatches:  1,
		QueueBatches: 3,
	})

	db := newTestDB(t, mock.URL())

	rows, err := db.QueryContext(context.Background(), "SELECT n FROM queued")
	require.NoError(t, err)
	defer rows.Close()

	var nums []int64
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		nums = append(nums, n)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{1, 2, 3}, nums)
}

func TestWithSessionSetup(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 1",
		Columns:     []presto.Column{{Name: "result", Type: "integer"}},
		Data:        [][]any{{1}},
		DataBatches: 1,
	})

	host := mock.URL()[len("http://"):]
	setupCalled := false
	connector, err := presto.NewConnector("presto://"+host, presto.WithSessionSetup(func(s *presto.Session) {
		setupCalled = true
		s.RequestOptions(func(r *http.Request) {
			r.Header.Set("X-Test-Auth", "kerberos-token")
		})
	}))
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	defer db.Close()

	var result int64
	err = db.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result)
	assert.True(t, setupCalled, "sessionSetup should have been called")
}

func TestDriver_ComplexTypes(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT tags, props, addr FROM complex",
		Columns: []presto.Column{
			{Name: "tags", Type: "array(varchar)"},
			{Name: "props", Type: "map(varchar,integer)"},
			{Name: "addr", Type: "row(street varchar, city varchar)"},
		},
		Data: [][]any{
			{
				[]any{"go", "presto"},
				map[string]any{"timeout": 30, "retries": 3},
				map[string]any{"street": "123 Main St", "city": "Springfield"},
			},
			{nil, nil, nil},
		},
		DataBatches: 1,
	})

	db := newTestDB(t, mock.URL())
	rows, err := db.QueryContext(context.Background(), "SELECT tags, props, addr FROM complex")
	require.NoError(t, err)
	defer rows.Close()

	// Row 1: non-null complex types
	require.True(t, rows.Next())
	var tags presto.NullSlice[string]
	var props presto.NullMap[string, float64]

	type Address struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}
	var addr presto.NullRow[Address]

	require.NoError(t, rows.Scan(&tags, &props, &addr))
	assert.True(t, tags.Valid)
	assert.Equal(t, []string{"go", "presto"}, tags.Slice)
	assert.True(t, props.Valid)
	assert.Equal(t, float64(30), props.Map["timeout"])
	assert.True(t, addr.Valid)
	assert.Equal(t, "123 Main St", addr.Row.Street)
	assert.Equal(t, "Springfield", addr.Row.City)

	// Row 2: null complex types
	require.True(t, rows.Next())
	var tags2 presto.NullSlice[string]
	var props2 presto.NullMap[string, float64]
	var addr2 presto.NullRow[Address]
	require.NoError(t, rows.Scan(&tags2, &props2, &addr2))
	assert.False(t, tags2.Valid)
	assert.False(t, props2.Valid)
	assert.False(t, addr2.Valid)

	require.NoError(t, rows.Err())
}

// ============================================================
// Internal tests (package presto) for unexported functions
// are in driver_internal_test.go
// ============================================================
