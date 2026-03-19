// Package kerberos provides Kerberos/SPNEGO authentication for the presto-go
// client library. It is a separate module to keep the gokrb5 dependency tree
// opt-in for consumers that don't need Kerberos.
package kerberos

import (
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
	presto "github.com/prestodb/presto-go-client/v2"
)

// Config holds Kerberos authentication parameters.
type Config struct {
	KeytabPath string // Path to .keytab file
	Principal  string // e.g. "user@EXAMPLE.COM"
	Realm      string // e.g. "EXAMPLE.COM"
	ConfigPath string // Path to krb5.conf
	ServiceSPN string // Service principal name, defaults to "HTTP/<hostname>"
}

// validate checks that all required fields are set.
func (c *Config) validate() error {
	if c.KeytabPath == "" {
		return fmt.Errorf("kerberos: KeytabPath is required")
	}
	if c.Principal == "" {
		return fmt.Errorf("kerberos: Principal is required")
	}
	if c.Realm == "" {
		return fmt.Errorf("kerberos: Realm is required")
	}
	if c.ConfigPath == "" {
		return fmt.Errorf("kerberos: ConfigPath is required")
	}
	return nil
}

// krbCloser wraps a gokrb5 client to implement io.Closer.
type krbCloser struct {
	cl *client.Client
}

func (k *krbCloser) Close() error {
	k.cl.Destroy()
	return nil
}

// NewRequestOption creates a presto.RequestOption that sets the SPNEGO
// Negotiate header on every request. It returns an io.Closer that must
// be called to destroy the underlying Kerberos client when done.
func NewRequestOption(cfg Config) (presto.RequestOption, io.Closer, error) {
	if err := cfg.validate(); err != nil {
		return nil, nil, err
	}

	kt, err := keytab.Load(cfg.KeytabPath)
	if err != nil {
		return nil, nil, fmt.Errorf("kerberos: failed to load keytab %q: %w", cfg.KeytabPath, err)
	}

	krb5Conf, err := config.Load(cfg.ConfigPath)
	if err != nil {
		return nil, nil, fmt.Errorf("kerberos: failed to load config %q: %w", cfg.ConfigPath, err)
	}

	// Parse principal into username and realm parts.
	// If the principal contains "@", split on it; otherwise use the configured realm.
	username := cfg.Principal
	realm := cfg.Realm
	if idx := strings.LastIndex(cfg.Principal, "@"); idx >= 0 {
		username = cfg.Principal[:idx]
		realm = cfg.Principal[idx+1:]
	}

	cl := client.NewWithKeytab(username, realm, kt, krb5Conf)
	if err := cl.Login(); err != nil {
		return nil, nil, fmt.Errorf("kerberos: login failed: %w", err)
	}

	closer := &krbCloser{cl: cl}

	opt := func(req *http.Request) {
		spn := cfg.ServiceSPN
		if spn == "" {
			spn = "HTTP/" + req.URL.Hostname()
		}
		// SetSPNEGOHeader adds the Authorization: Negotiate header.
		// Errors are silently ignored here; the server will return 401
		// if the token is missing, which surfaces as a query error.
		_ = spnego.SetSPNEGOHeader(cl, req, spn)
	}

	return opt, closer, nil
}

// DSN parameter names for Kerberos configuration.
const (
	dsnKeytab     = "kerberos_keytab"
	dsnPrincipal  = "kerberos_principal"
	dsnRealm      = "kerberos_realm"
	dsnConfig     = "kerberos_config"
	dsnServiceSPN = "kerberos_service_spn"
)

// kerberosDSNParams is the set of DSN query parameters consumed by this package.
var kerberosDSNParams = []string{
	dsnKeytab, dsnPrincipal, dsnRealm, dsnConfig, dsnServiceSPN,
}

// parseDSN extracts Kerberos parameters from a DSN URL and returns
// the Config and a cleaned DSN with Kerberos params removed.
func parseDSN(dsn string) (*Config, string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, "", fmt.Errorf("kerberos: invalid DSN: %w", err)
	}

	q := u.Query()
	cfg := &Config{
		KeytabPath: q.Get(dsnKeytab),
		Principal:  q.Get(dsnPrincipal),
		Realm:      q.Get(dsnRealm),
		ConfigPath: q.Get(dsnConfig),
		ServiceSPN: q.Get(dsnServiceSPN),
	}

	// Remove Kerberos params from the query string
	for _, key := range kerberosDSNParams {
		q.Del(key)
	}
	u.RawQuery = q.Encode()

	return cfg, u.String(), nil
}

// NewConnector creates a driver.Connector with Kerberos/SPNEGO authentication.
// It parses Kerberos parameters from the DSN, strips them, and passes the
// cleaned DSN to presto.NewConnector with a session setup hook that applies
// the SPNEGO request option to every session.
//
// The returned io.Closer must be called to destroy the Kerberos client
// (typically via defer). The connector remains usable until Close is called.
func NewConnector(dsn string) (driver.Connector, io.Closer, error) {
	cfg, cleanDSN, err := parseDSN(dsn)
	if err != nil {
		return nil, nil, err
	}

	opt, closer, err := NewRequestOption(*cfg)
	if err != nil {
		return nil, nil, err
	}

	connector, err := presto.NewConnector(cleanDSN, presto.WithSessionSetup(func(s *presto.Session) {
		s.RequestOptions(opt)
	}))
	if err != nil {
		if closeErr := closer.Close(); closeErr != nil {
			err = fmt.Errorf("%w (additionally, cleanup failed: %v)", err, closeErr)
		}
		return nil, nil, err
	}

	return connector, closer, nil
}
