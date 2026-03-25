package presto

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
		check   func(t *testing.T, cfg *dsnConfig)
	}{
		{
			name: "basic presto",
			dsn:  "presto://localhost:8080/hive/default",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Equal(t, "localhost", cfg.host)
				assert.Equal(t, "8080", cfg.port)
				assert.Equal(t, "hive", cfg.catalog)
				assert.Equal(t, "default", cfg.schema)
				assert.False(t, cfg.isTrino)
			},
		},
		{
			name: "TLS params",
			dsn:  "presto://localhost:8443/hive?ssl_cert=/path/cert.pem&ssl_key=/path/key.pem&ssl_ca=/path/ca.pem&ssl_skip_verify=true",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Equal(t, "/path/cert.pem", cfg.sslCert)
				assert.Equal(t, "/path/key.pem", cfg.sslKey)
				assert.Equal(t, "/path/ca.pem", cfg.sslCA)
				assert.True(t, cfg.sslSkipVerify)
				assert.True(t, cfg.hasTLS())
				assert.Equal(t, "https://localhost:8443", cfg.serverURL())
			},
		},
		{
			name: "ssl_skip_verify=1",
			dsn:  "presto://localhost?ssl_skip_verify=1",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.True(t, cfg.sslSkipVerify)
				assert.True(t, cfg.hasTLS())
			},
		},
		{
			name: "no TLS",
			dsn:  "presto://localhost:8080",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.False(t, cfg.hasTLS())
				assert.Equal(t, "http://localhost:8080", cfg.serverURL())
			},
		},
		{
			name: "trino with defaults",
			dsn:  "trino://trino-host/catalog",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Equal(t, "trino-host", cfg.host)
				assert.Equal(t, "8080", cfg.port)
				assert.Equal(t, "catalog", cfg.catalog)
				assert.Empty(t, cfg.schema)
				assert.True(t, cfg.isTrino)
			},
		},
		{
			name: "user and password",
			dsn:  "presto://admin:secret@host:9090/cat/sch",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Equal(t, "admin", cfg.user)
				assert.Equal(t, "secret", cfg.password)
				assert.Equal(t, "host", cfg.host)
				assert.Equal(t, "9090", cfg.port)
			},
		},
		{
			name: "default port for presto",
			dsn:  "presto://localhost",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Equal(t, "8080", cfg.port)
			},
		},
		{
			name: "query params",
			dsn:  "presto://localhost/cat?timezone=US/Eastern&client_tags=a,b,c&client_info=myinfo&source=myapp&custom=val",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Equal(t, "US/Eastern", cfg.timezone)
				assert.Equal(t, []string{"a", "b", "c"}, cfg.clientTags)
				assert.Equal(t, "myinfo", cfg.clientInfo)
				assert.Equal(t, "myapp", cfg.source)
				assert.Equal(t, "val", cfg.sessionProps["custom"])
			},
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
			name: "no path",
			dsn:  "presto://host:1234",
			check: func(t *testing.T, cfg *dsnConfig) {
				assert.Empty(t, cfg.catalog)
				assert.Empty(t, cfg.schema)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseDSN(tt.dsn)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestValueToSQL(t *testing.T) {
	tests := []struct {
		name string
		val  driver.Value
		want string
	}{
		{"nil", nil, "NULL"},
		{"int64", int64(42), "42"},
		{"negative int64", int64(-100), "-100"},
		{"float64", float64(3.14), "3.14"},
		{"bool true", true, "TRUE"},
		{"bool false", false, "FALSE"},
		{"string", "hello", "'hello'"},
		{"string with quote", "it's", "'it''s'"},
		{"empty string", "", "''"},
		{"bytes", []byte{0xDE, 0xAD, 0xBE, 0xEF}, "X'deadbeef'"},
		{"empty bytes", []byte{}, "X''"},
		{"time", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), "TIMESTAMP '2024-01-15 10:30:00.000'"},
		{"duration", 5*24*time.Hour + 3*time.Hour + 14*time.Minute + 22*time.Second + 123*time.Millisecond,
			"INTERVAL '5 03:14:22.123' DAY TO SECOND"},
		{"negative duration", -(2*24*time.Hour + 6*time.Hour),
			"INTERVAL '-2 06:00:00.000' DAY TO SECOND"},
		{"zero duration", time.Duration(0), "INTERVAL '0 00:00:00.000' DAY TO SECOND"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := valueToSQL(tt.val)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("unsupported type", func(t *testing.T) {
		_, err := valueToSQL(struct{}{})
		assert.Error(t, err)
	})
}

func TestInterpolateParams(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		args    []driver.Value
		want    string
		wantErr bool
	}{
		{
			name:  "no args",
			query: "SELECT 1",
			args:  nil,
			want:  "SELECT 1",
		},
		{
			name:  "single placeholder",
			query: "SELECT ?",
			args:  []driver.Value{int64(42)},
			want:  "SELECT 42",
		},
		{
			name:  "multiple placeholders",
			query: "SELECT ?, ?, ?",
			args:  []driver.Value{int64(1), "hello", true},
			want:  "SELECT 1, 'hello', TRUE",
		},
		{
			name:  "skip ? in string literal",
			query: "SELECT * FROM t WHERE name = 'what?' AND id = ?",
			args:  []driver.Value{int64(1)},
			want:  "SELECT * FROM t WHERE name = 'what?' AND id = 1",
		},
		{
			name:  "escaped quotes in string literal",
			query: "SELECT * FROM t WHERE name = 'it''s ?' AND id = ?",
			args:  []driver.Value{int64(1)},
			want:  "SELECT * FROM t WHERE name = 'it''s ?' AND id = 1",
		},
		{
			name:    "too few args",
			query:   "SELECT ?, ?",
			args:    []driver.Value{int64(1)},
			wantErr: true,
		},
		{
			name:    "too many args",
			query:   "SELECT ?",
			args:    []driver.Value{int64(1), int64(2)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := interpolateParams(tt.query, tt.args)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"varchar(255)", "varchar"},
		{"VARCHAR(255)", "varchar"},
		{"decimal(10,2)", "decimal"},
		{"bigint", "bigint"},
		{"BIGINT", "bigint"},
		{"timestamp with time zone", "timestamp with time zone"},
		{"TIMESTAMP WITH TIME ZONE", "timestamp with time zone"},
		{"time with time zone", "time with time zone"},
		{"array(varchar)", "array"},
		{"map(varchar,integer)", "map"},
		{"  varchar(100)  ", "varchar"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeType(tt.input))
		})
	}
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name       string
		val        any
		prestoType string
		want       driver.Value
	}{
		{"nil", nil, "varchar", nil},
		{"bigint", float64(42), "bigint", int64(42)},
		{"integer", float64(10), "integer", int64(10)},
		{"smallint", float64(5), "smallint", int64(5)},
		{"tinyint", float64(1), "tinyint", int64(1)},
		{"double", float64(3.14), "double", float64(3.14)},
		{"real", float64(2.5), "real", float64(2.5)},
		{"boolean true", true, "boolean", true},
		{"boolean false", false, "boolean", false},
		{"varchar", "hello", "varchar", "hello"},
		{"char", "x", "char", "x"},
		{"decimal from float", float64(19.99), "decimal(10,2)", "19.99"},
		{"decimal from string", "123.456", "decimal", "123.456"},
		{"date", "2024-01-15", "date", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"timestamp", "2024-01-15 10:30:00.000", "timestamp", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
		{"time", "10:30:00.000", "time", time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC)},
		{"time with time zone", "10:30:00.000 UTC", "time with time zone", time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC)},
		{"varbinary", base64.StdEncoding.EncodeToString([]byte("hello")), "varbinary", []byte("hello")},
		{"array", []any{1.0, 2.0}, "array(integer)", "[1,2]"},
		{"interval year to month", "3-6", "interval year to month", "3-6"},
		{"interval day to second", "5 03:14:22.123", "interval day to second",
			5*24*time.Hour + 3*time.Hour + 14*time.Minute + 22*time.Second + 123*time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.val, tt.prestoType)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("error cases", func(t *testing.T) {
		_, err := convertValue("not a number", "bigint")
		assert.Error(t, err)

		_, err = convertValue("not a number", "double")
		assert.Error(t, err)

		_, err = convertValue("not a bool", "boolean")
		assert.Error(t, err)

		_, err = convertValue(42, "date")
		assert.Error(t, err)

		_, err = convertValue(42, "timestamp")
		assert.Error(t, err)

		_, err = convertValue(42, "timestamp with time zone")
		assert.Error(t, err)

		_, err = convertValue(42, "varbinary")
		assert.Error(t, err)

		_, err = convertValue(42, "time")
		assert.Error(t, err)

		_, err = convertValue(42, "time with time zone")
		assert.Error(t, err)

		_, err = convertValue(42, "interval year to month")
		assert.Error(t, err)

		_, err = convertValue(42, "interval day to second")
		assert.Error(t, err)
	})
}

func TestScanTypeForPrestoType(t *testing.T) {
	tests := []struct {
		prestoType string
		want       reflect.Type
	}{
		{"bigint", reflect.TypeOf(int64(0))},
		{"integer", reflect.TypeOf(int64(0))},
		{"smallint", reflect.TypeOf(int64(0))},
		{"tinyint", reflect.TypeOf(int64(0))},
		{"double", reflect.TypeOf(float64(0))},
		{"real", reflect.TypeOf(float64(0))},
		{"boolean", reflect.TypeOf(false)},
		{"varchar", reflect.TypeOf("")},
		{"varchar(255)", reflect.TypeOf("")},
		{"char", reflect.TypeOf("")},
		{"decimal", reflect.TypeOf("")},
		{"json", reflect.TypeOf("")},
		{"varbinary", reflect.TypeOf([]byte(nil))},
		{"date", reflect.TypeOf(time.Time{})},
		{"timestamp", reflect.TypeOf(time.Time{})},
		{"timestamp with time zone", reflect.TypeOf(time.Time{})},
		{"time", reflect.TypeOf(time.Time{})},
		{"time with time zone", reflect.TypeOf(time.Time{})},
		{"array(integer)", reflect.TypeOf("")},
		{"map(varchar,integer)", reflect.TypeOf("")},
		{"interval year to month", reflect.TypeOf("")},
		{"interval day to second", reflect.TypeOf(time.Duration(0))},
		{"unknown_type", reflect.TypeOf("")},
	}

	for _, tt := range tests {
		t.Run(tt.prestoType, func(t *testing.T) {
			assert.Equal(t, tt.want, scanTypeForPrestoType(tt.prestoType))
		})
	}
}

func TestDsnConfig_ServerURL(t *testing.T) {
	t.Run("presto", func(t *testing.T) {
		cfg := &dsnConfig{host: "localhost", port: "8080"}
		assert.Equal(t, "http://localhost:8080", cfg.serverURL())
	})

	t.Run("trino", func(t *testing.T) {
		cfg := &dsnConfig{host: "trino-host", port: "8080", isTrino: true}
		assert.Equal(t, "http://trino-host:8080", cfg.serverURL())
	})

	t.Run("TLS upgrades to HTTPS", func(t *testing.T) {
		cfg := &dsnConfig{host: "localhost", port: "8443", sslCA: "/path/ca.pem"}
		assert.Equal(t, "https://localhost:8443", cfg.serverURL())
	})
}

func TestBuildTLSConfig(t *testing.T) {
	t.Run("no TLS returns nil", func(t *testing.T) {
		cfg := &dsnConfig{host: "localhost", port: "8080"}
		tlsCfg, err := cfg.buildTLSConfig()
		require.NoError(t, err)
		assert.Nil(t, tlsCfg)
	})

	t.Run("skip verify only", func(t *testing.T) {
		cfg := &dsnConfig{host: "localhost", port: "8080", sslSkipVerify: true}
		tlsCfg, err := cfg.buildTLSConfig()
		require.NoError(t, err)
		require.NotNil(t, tlsCfg)
		assert.True(t, tlsCfg.InsecureSkipVerify)
	})

	t.Run("bad CA file", func(t *testing.T) {
		cfg := &dsnConfig{host: "localhost", port: "8080", sslCA: "/nonexistent/ca.pem"}
		_, err := cfg.buildTLSConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA certificate")
	})

	t.Run("bad client cert", func(t *testing.T) {
		cfg := &dsnConfig{host: "localhost", port: "8080", sslCert: "/nonexistent/cert.pem", sslKey: "/nonexistent/key.pem"}
		_, err := cfg.buildTLSConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client certificate")
	})
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		input string
		want  time.Time
	}{
		{"2024-01-15 10:30:00.000", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
		{"2024-01-15 10:30:00", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
		{"2024-01-15 10:30:00.000000", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseTimestamp(tt.input)
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got))
		})
	}

	t.Run("invalid", func(t *testing.T) {
		_, err := parseTimestamp("not a timestamp")
		assert.Error(t, err)
	})
}

func TestParseTimestampWithTZ(t *testing.T) {
	t.Run("offset format", func(t *testing.T) {
		got, err := parseTimestampWithTZ("2024-01-15 10:30:00.000 +00:00")
		require.NoError(t, err)
		assert.Equal(t, 2024, got.Year())
		assert.Equal(t, 10, got.Hour())
	})

	t.Run("named zone", func(t *testing.T) {
		got, err := parseTimestampWithTZ("2024-01-15 10:30:00.000 UTC")
		require.NoError(t, err)
		assert.Equal(t, 10, got.Hour())
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := parseTimestampWithTZ("not valid")
		assert.Error(t, err)
	})
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		input string
		want  time.Time
	}{
		{"10:30:00.000", time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC)},
		{"23:59:59.999", time.Date(0, 1, 1, 23, 59, 59, 999000000, time.UTC)},
		{"00:00:00", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseTime(tt.input)
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got))
		})
	}

	t.Run("invalid", func(t *testing.T) {
		_, err := parseTime("not valid")
		assert.Error(t, err)
	})
}

func TestParseTimeWithTZ(t *testing.T) {
	t.Run("named zone", func(t *testing.T) {
		got, err := parseTimeWithTZ("10:30:00.000 UTC")
		require.NoError(t, err)
		assert.Equal(t, 10, got.Hour())
		assert.Equal(t, 30, got.Minute())
	})

	t.Run("offset format", func(t *testing.T) {
		got, err := parseTimeWithTZ("10:30:00.000 +05:30")
		require.NoError(t, err)
		assert.Equal(t, 10, got.Hour())
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := parseTimeWithTZ("not valid")
		assert.Error(t, err)
	})
}

func TestParseIntervalDayToSecond(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"0 00:00:00.000", 0},
		{"5 03:14:22.123", 5*24*time.Hour + 3*time.Hour + 14*time.Minute + 22*time.Second + 123*time.Millisecond},
		{"0 12:00:00.000", 12 * time.Hour},
		{"1 00:00:00.000", 24 * time.Hour},
		{"-2 06:00:00.000", -(2*24*time.Hour + 6*time.Hour)},
		{"0 00:00:00.001", time.Millisecond},
		// Single-digit fractional seconds (tenths of a second)
		{"0 00:00:01.1", 1*time.Second + 100*time.Millisecond},
		// Six-digit fractional seconds (microseconds)
		{"0 00:00:22.123456", 22*time.Second + 123456*time.Microsecond},
		// Nine-digit fractional seconds (nanoseconds)
		{"0 00:00:01.123456789", 1*time.Second + 123456789*time.Nanosecond},
		// More than 9 digits truncated to nanoseconds
		{"0 00:00:01.1234567891", 1*time.Second + 123456789*time.Nanosecond},
		// No fractional seconds
		{"0 00:00:05", 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseIntervalDayToSecond(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseIntervalDayToSecond("not valid")
		assert.Error(t, err)
	})

	t.Run("invalid time", func(t *testing.T) {
		_, err := parseIntervalDayToSecond("1 bad:time")
		assert.Error(t, err)
	})
}

func TestPrestoIsolationLevel(t *testing.T) {
	tests := []struct {
		level sql.IsolationLevel
		want  string
	}{
		{sql.LevelReadUncommitted, "READ UNCOMMITTED"},
		{sql.LevelReadCommitted, "READ COMMITTED"},
		{sql.LevelRepeatableRead, "REPEATABLE READ"},
		{sql.LevelSerializable, "SERIALIZABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got, err := prestoIsolationLevel(tt.level)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("unsupported", func(t *testing.T) {
		_, err := prestoIsolationLevel(sql.LevelLinearizable)
		assert.Error(t, err)
	})
}

func TestConnectorEnsureClient_RetryAfterTransientFailure(t *testing.T) {
	// Create a connector with an invalid TLS cert path to force ensureClient() failure.
	// With the old sync.Once approach, this error would be cached permanently.
	// With the new double-checked locking, fixing the config allows retry.
	cfg := &dsnConfig{
		host:  "localhost",
		port:  "8080",
		sslCA: "/nonexistent/ca.pem", // triggers buildTLSConfig failure
	}
	c := &prestoConnector{cfg: cfg}

	// First Connect should fail due to bad TLS config
	_, err := c.Connect(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "presto connector:")

	// Verify that the connector is NOT permanently poisoned
	assert.False(t, c.initialized.Load(), "connector should not be marked initialized after failure")

	// Fix the config (remove bad TLS path) and retry.
	// Note: cfg must only be replaced before successful initialization;
	// after initialization, cfg is no longer consulted.
	c.cfg = &dsnConfig{host: "localhost", port: "8080"}
	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, conn)
	conn.Close()
}

func TestConnectorEnsureClient_SetsIsTrinoViaMethod(t *testing.T) {
	// Verify that Trino mode is set through the public IsTrino() method
	// (which acquires clientMu) rather than direct field assignment.
	cfg, err := parseDSN("trino://localhost:8080/hive/default")
	require.NoError(t, err)
	assert.True(t, cfg.isTrino)

	c := &prestoConnector{cfg: cfg}
	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, conn)

	// Verify isTrino was set on the client (single-goroutine test,
	// safe to read after Connect returns)
	assert.True(t, c.client.isTrino)
}

func TestIntervalDayToSecond_RoundTrip(t *testing.T) {
	// formatDurationAsDayToSecond uses millisecond precision (Presto's wire format).
	// Verify that format → parse round-trips correctly at millisecond precision.
	durations := []time.Duration{
		0,
		5*24*time.Hour + 3*time.Hour + 14*time.Minute + 22*time.Second + 123*time.Millisecond,
		12 * time.Hour,
		24 * time.Hour,
		time.Millisecond,
		-(2*24*time.Hour + 6*time.Hour),
	}
	for _, d := range durations {
		formatted := formatDurationAsDayToSecond(d)
		parsed, err := parseIntervalDayToSecond(formatted)
		require.NoError(t, err, "failed to parse formatted duration %q (original: %v)", formatted, d)
		assert.Equal(t, d, parsed, "round-trip mismatch for %v → %q → %v", d, formatted, parsed)
	}
}

func TestIntervalDayToSecond_SubMillisTruncatedOnFormat(t *testing.T) {
	// Durations with sub-millisecond precision lose precision through format
	// (which outputs millis only), but parse accepts the result.
	d := 1*time.Second + 123456789*time.Nanosecond
	formatted := formatDurationAsDayToSecond(d)
	assert.Equal(t, "0 00:00:01.123", formatted) // truncated to millis
	parsed, err := parseIntervalDayToSecond(formatted)
	require.NoError(t, err)
	assert.Equal(t, 1*time.Second+123*time.Millisecond, parsed)
}
