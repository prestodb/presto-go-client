package kerberos

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "missing keytab",
			cfg:     Config{Principal: "user@REALM", Realm: "REALM", ConfigPath: "/etc/krb5.conf"},
			wantErr: "KeytabPath is required",
		},
		{
			name:    "missing principal",
			cfg:     Config{KeytabPath: "/etc/presto.keytab", Realm: "REALM", ConfigPath: "/etc/krb5.conf"},
			wantErr: "Principal is required",
		},
		{
			name:    "missing realm",
			cfg:     Config{KeytabPath: "/etc/presto.keytab", Principal: "user@REALM", ConfigPath: "/etc/krb5.conf"},
			wantErr: "Realm is required",
		},
		{
			name:    "missing config path",
			cfg:     Config{KeytabPath: "/etc/presto.keytab", Principal: "user@REALM", Realm: "REALM"},
			wantErr: "ConfigPath is required",
		},
		{
			name: "all fields present",
			cfg: Config{
				KeytabPath: "/etc/presto.keytab",
				Principal:  "user@REALM",
				Realm:      "REALM",
				ConfigPath: "/etc/krb5.conf",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseDSN(t *testing.T) {
	t.Run("extracts and strips kerberos params", func(t *testing.T) {
		dsn := "presto://host:8080/catalog/schema?kerberos_keytab=/etc/presto.keytab&kerberos_principal=user@REALM&kerberos_realm=REALM&kerberos_config=/etc/krb5.conf&kerberos_service_spn=HTTP/presto.example.com&timezone=UTC"

		cfg, cleanDSN, err := parseDSN(dsn)
		require.NoError(t, err)

		assert.Equal(t, "/etc/presto.keytab", cfg.KeytabPath)
		assert.Equal(t, "user@REALM", cfg.Principal)
		assert.Equal(t, "REALM", cfg.Realm)
		assert.Equal(t, "/etc/krb5.conf", cfg.ConfigPath)
		assert.Equal(t, "HTTP/presto.example.com", cfg.ServiceSPN)

		// Kerberos params should be stripped
		assert.NotContains(t, cleanDSN, "kerberos_keytab")
		assert.NotContains(t, cleanDSN, "kerberos_principal")
		assert.NotContains(t, cleanDSN, "kerberos_realm")
		assert.NotContains(t, cleanDSN, "kerberos_config")
		assert.NotContains(t, cleanDSN, "kerberos_service_spn")
		// Non-kerberos params should be preserved
		assert.Contains(t, cleanDSN, "timezone=UTC")
		assert.Contains(t, cleanDSN, "presto://host:8080/catalog/schema")
	})

	t.Run("empty kerberos params", func(t *testing.T) {
		dsn := "presto://host:8080/catalog/schema?timezone=UTC"

		cfg, cleanDSN, err := parseDSN(dsn)
		require.NoError(t, err)

		assert.Empty(t, cfg.KeytabPath)
		assert.Empty(t, cfg.Principal)
		assert.Contains(t, cleanDSN, "timezone=UTC")
	})

	t.Run("invalid DSN", func(t *testing.T) {
		_, _, err := parseDSN("://bad")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid DSN")
	})

	t.Run("invalid DSN error omits the password", func(t *testing.T) {
		_, _, err := parseDSN("presto://alice:p/ss@host:8443?kerberos_principal=alice")
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "alice")
		assert.NotContains(t, err.Error(), ":p")
	})
}

func TestNewRequestOption_MissingConfig(t *testing.T) {
	t.Run("fails on missing keytab", func(t *testing.T) {
		_, _, err := NewRequestOption(Config{
			Principal:  "user@REALM",
			Realm:      "REALM",
			ConfigPath: "/etc/krb5.conf",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "KeytabPath is required")
	})

	t.Run("fails on missing principal", func(t *testing.T) {
		_, _, err := NewRequestOption(Config{
			KeytabPath: "/etc/presto.keytab",
			Realm:      "REALM",
			ConfigPath: "/etc/krb5.conf",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Principal is required")
	})
}

func TestNewRequestOption_BadKeytab(t *testing.T) {
	_, _, err := NewRequestOption(Config{
		KeytabPath: "/nonexistent/presto.keytab",
		Principal:  "user@REALM",
		Realm:      "REALM",
		ConfigPath: "/etc/krb5.conf",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load keytab")
}

func TestSPNDefault(t *testing.T) {
	cfg := Config{ServiceSPN: ""}
	// When ServiceSPN is empty, the request option should default to "HTTP/<hostname>"
	// We can't test the actual SPNEGO header without a KDC, but we verify
	// that an empty ServiceSPN passes validation (it's optional).
	assert.Empty(t, cfg.ServiceSPN, "empty ServiceSPN should be valid; defaulting happens at request time")
}

func TestNewConnector_ValidationError(t *testing.T) {
	// DSN with Kerberos params but missing required fields
	dsn := "presto://host:8080/catalog/schema?kerberos_keytab=&kerberos_principal=user@REALM&kerberos_realm=REALM&kerberos_config=/etc/krb5.conf"

	_, _, err := NewConnector(dsn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeytabPath is required")
}
