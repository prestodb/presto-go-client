// Package oauth2 provides OAuth2 and static token authentication for the
// presto-go client library. It is a separate module to keep the oauth2
// dependency opt-in.
package oauth2

import (
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	presto "github.com/prestodb/presto-go-client/v2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// --- Static Token ---

// NewStaticTokenOption returns a RequestOption that sets a static Bearer token
// on every request. Use this for pre-obtained JWTs or long-lived access tokens.
func NewStaticTokenOption(token string) presto.RequestOption {
	return func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

// --- Client Credentials Flow ---

// Config holds OAuth2 client credentials configuration.
type Config struct {
	ClientID     string
	ClientSecret string
	TokenURL     string   // Token endpoint URL
	Scopes       []string // Optional scopes
}

// validate checks that required fields are set.
func (c *Config) validate() error {
	if c.ClientID == "" {
		return fmt.Errorf("oauth2: ClientID is required")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("oauth2: ClientSecret is required")
	}
	if c.TokenURL == "" {
		return fmt.Errorf("oauth2: TokenURL is required")
	}
	return nil
}

// NewRequestOption creates a RequestOption that automatically obtains and
// refreshes OAuth2 tokens using the client credentials flow. The returned
// option is safe for concurrent use — the underlying oauth2 token source
// handles caching and refresh.
func NewRequestOption(cfg Config) (presto.RequestOption, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ccCfg := &clientcredentials.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		TokenURL:     cfg.TokenURL,
		Scopes:       cfg.Scopes,
	}

	// Manage the cached token ourselves so that token refresh uses the
	// request's context (respecting cancellation/deadlines) rather than
	// a detached context.Background() that could hang indefinitely.
	var (
		mu          sync.Mutex
		cachedToken *oauth2.Token
	)

	opt := func(req *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if cachedToken == nil || !cachedToken.Valid() {
			ts := ccCfg.TokenSource(req.Context())
			newToken, err := ts.Token()
			if err != nil {
				// Cannot return an error from a RequestOption. The server will
				// return 401 if the header is missing, surfacing as a query error.
				return
			}
			cachedToken = newToken
		}
		cachedToken.SetAuthHeader(req)
	}

	return opt, nil
}

// --- DSN Integration ---

// DSN parameter names for OAuth2 configuration.
const (
	dsnAccessToken  = "access_token"
	dsnClientID     = "oauth2_client_id"
	dsnClientSecret = "oauth2_client_secret"
	dsnTokenURL     = "oauth2_token_url"
	dsnScopes       = "oauth2_scopes"
)

var oauth2DSNParams = []string{
	dsnAccessToken, dsnClientID, dsnClientSecret, dsnTokenURL, dsnScopes,
}

// parseDSN extracts OAuth2 parameters from a DSN and returns the
// appropriate RequestOption and cleaned DSN. It supports two modes:
//
//  1. Static token: access_token=<token>
//  2. Client credentials: oauth2_client_id, oauth2_client_secret, oauth2_token_url
func parseDSN(dsn string) (presto.RequestOption, string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, "", fmt.Errorf("oauth2: invalid DSN: %w", err)
	}

	q := u.Query()
	accessToken := q.Get(dsnAccessToken)
	clientID := q.Get(dsnClientID)
	clientSecret := q.Get(dsnClientSecret)
	tokenURL := q.Get(dsnTokenURL)
	scopes := q.Get(dsnScopes)

	// Remove OAuth2 params from query string
	for _, key := range oauth2DSNParams {
		q.Del(key)
	}
	u.RawQuery = q.Encode()
	cleanDSN := u.String()

	// Determine which mode to use
	if accessToken != "" {
		return NewStaticTokenOption(accessToken), cleanDSN, nil
	}

	if clientID != "" {
		cfg := Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     tokenURL,
		}
		if scopes != "" {
			parts := strings.Split(scopes, ",")
			cfg.Scopes = make([]string, 0, len(parts))
			for _, s := range parts {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					cfg.Scopes = append(cfg.Scopes, trimmed)
				}
			}
		}
		opt, err := NewRequestOption(cfg)
		if err != nil {
			return nil, "", err
		}
		return opt, cleanDSN, nil
	}

	// No OAuth2 params found
	return nil, cleanDSN, nil
}

// NewConnector creates a driver.Connector with OAuth2 authentication.
// It supports two modes via DSN parameters:
//
//  1. Static token: access_token=<token>
//  2. Client credentials: oauth2_client_id, oauth2_client_secret, oauth2_token_url
//
// OAuth2 parameters are stripped from the DSN before passing to presto.NewConnector.
func NewConnector(dsn string, opts ...presto.ConnectorOption) (driver.Connector, error) {
	authOpt, cleanDSN, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	if authOpt != nil {
		setupOpt := presto.WithSessionSetup(func(s *presto.Session) {
			s.RequestOptions(authOpt)
		})
		opts = append([]presto.ConnectorOption{setupOpt}, opts...)
	}

	return presto.NewConnector(cleanDSN, opts...)
}

// TokenSource wraps an oauth2.TokenSource as a presto.RequestOption.
// Use this when you have a custom token source (e.g., from a token file,
// metadata service, or custom refresh logic).
func TokenSource(ts oauth2.TokenSource) presto.RequestOption {
	return func(req *http.Request) {
		token, err := ts.Token()
		if err != nil {
			return
		}
		token.SetAuthHeader(req)
	}
}
