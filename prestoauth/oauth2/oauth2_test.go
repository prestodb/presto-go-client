package oauth2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStaticTokenOption(t *testing.T) {
	opt := NewStaticTokenOption("my-jwt-token")
	req := httptest.NewRequest("GET", "http://example.com", nil)
	opt(req)
	assert.Equal(t, "Bearer my-jwt-token", req.Header.Get("Authorization"))
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "missing client ID",
			cfg:     Config{ClientSecret: "secret", TokenURL: "http://auth/token"},
			wantErr: "ClientID is required",
		},
		{
			name:    "missing client secret",
			cfg:     Config{ClientID: "id", TokenURL: "http://auth/token"},
			wantErr: "ClientSecret is required",
		},
		{
			name:    "missing token URL",
			cfg:     Config{ClientID: "id", ClientSecret: "secret"},
			wantErr: "TokenURL is required",
		},
		{
			name: "valid config",
			cfg:  Config{ClientID: "id", ClientSecret: "secret", TokenURL: "http://auth/token"},
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

func TestNewRequestOption_ValidationError(t *testing.T) {
	_, err := NewRequestOption(Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ClientID is required")
}

func TestNewRequestOption_ClientCredentials(t *testing.T) {
	// Set up a mock token endpoint
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"test-token-123","token_type":"Bearer","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	opt, err := NewRequestOption(Config{
		ClientID:     "my-client",
		ClientSecret: "my-secret",
		TokenURL:     tokenServer.URL,
		Scopes:       []string{"read", "write"},
	})
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "http://presto:8080/v1/statement", nil)
	opt(req)
	assert.Equal(t, "Bearer test-token-123", req.Header.Get("Authorization"))
}

func TestParseDSN(t *testing.T) {
	t.Run("static access token", func(t *testing.T) {
		dsn := "presto://host:8080/catalog?access_token=my-token&timezone=UTC"
		opt, cleanDSN, err := parseDSN(dsn)
		require.NoError(t, err)
		require.NotNil(t, opt)

		// Token should be stripped
		assert.NotContains(t, cleanDSN, "access_token")
		assert.Contains(t, cleanDSN, "timezone=UTC")

		// Option should set Bearer header
		req := httptest.NewRequest("GET", "http://example.com", nil)
		opt(req)
		assert.Equal(t, "Bearer my-token", req.Header.Get("Authorization"))
	})

	t.Run("no OAuth2 params", func(t *testing.T) {
		dsn := "presto://host:8080/catalog?timezone=UTC"
		opt, cleanDSN, err := parseDSN(dsn)
		require.NoError(t, err)
		assert.Nil(t, opt)
		assert.Contains(t, cleanDSN, "timezone=UTC")
	})

	t.Run("client credentials missing secret", func(t *testing.T) {
		dsn := "presto://host:8080/catalog?oauth2_client_id=id&oauth2_token_url=http://auth/token"
		_, _, err := parseDSN(dsn)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ClientSecret is required")
	})

	t.Run("invalid DSN", func(t *testing.T) {
		_, _, err := parseDSN("://bad")
		require.Error(t, err)
	})

	t.Run("client credentials with scopes", func(t *testing.T) {
		// Use a mock token server for validation
		tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"access_token":"cc-token","token_type":"Bearer","expires_in":3600}`))
		}))
		defer tokenServer.Close()

		dsn := "presto://host:8080/catalog?oauth2_client_id=id&oauth2_client_secret=secret&oauth2_token_url=" + tokenServer.URL + "&oauth2_scopes=read,write&timezone=UTC"
		opt, cleanDSN, err := parseDSN(dsn)
		require.NoError(t, err)
		require.NotNil(t, opt)

		assert.NotContains(t, cleanDSN, "oauth2_client_id")
		assert.NotContains(t, cleanDSN, "oauth2_client_secret")
		assert.NotContains(t, cleanDSN, "oauth2_token_url")
		assert.NotContains(t, cleanDSN, "oauth2_scopes")
		assert.Contains(t, cleanDSN, "timezone=UTC")
	})

	t.Run("scopes with whitespace are trimmed", func(t *testing.T) {
		tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Verify scopes are trimmed in the token request
			r.ParseForm()
			assert.Equal(t, "read write", r.PostForm.Get("scope"))
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"access_token":"token","token_type":"Bearer","expires_in":3600}`))
		}))
		defer tokenServer.Close()

		dsn := "presto://host:8080/catalog?oauth2_client_id=id&oauth2_client_secret=secret&oauth2_token_url=" + tokenServer.URL + "&oauth2_scopes=read,+write"
		opt, _, err := parseDSN(dsn)
		require.NoError(t, err)
		require.NotNil(t, opt)
	})
}

func TestNewConnector_StaticToken(t *testing.T) {
	dsn := "presto://localhost:8080/catalog?access_token=my-token"
	connector, err := NewConnector(dsn)
	require.NoError(t, err)
	assert.NotNil(t, connector)
}

func TestNewConnector_NoAuth(t *testing.T) {
	dsn := "presto://localhost:8080/catalog"
	connector, err := NewConnector(dsn)
	require.NoError(t, err)
	assert.NotNil(t, connector)
}

func TestNewConnector_InvalidDSN(t *testing.T) {
	// Missing host after stripping params -> presto NewConnector will fail
	_, err := NewConnector("://bad")
	require.Error(t, err)
}
