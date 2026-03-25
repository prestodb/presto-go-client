package presto

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Presto/Trino protocol headers
const (
	UserHeader               = "X-Presto-User"
	CatalogHeader            = "X-Presto-Catalog"
	SchemaHeader             = "X-Presto-Schema"
	SessionHeader            = "X-Presto-Session"
	SetSessionHeader         = "X-Presto-Set-Session"
	ClearSessionHeader       = "X-Presto-Clear-Session"
	TransactionHeader        = "X-Presto-Transaction-Id"
	StartedTransactionHeader = "X-Presto-Started-Transaction-Id"
	ClearTransactionHeader   = "X-Presto-Clear-Transaction-Id"
	ClientInfoHeader         = "X-Presto-Client-Info"
	ClientTagHeader          = "X-Presto-Client-Tags"
	SourceHeader             = "X-Presto-Source"
	TimeZoneHeader           = "X-Presto-Time-Zone"

	ContentEncodingGzip = "gzip"
	MaxRetryAttempts    = 10
	MaxRetryDelay       = 30 * time.Second
)

// DefaultUser is the default username sent in the X-Presto-User header.
// Consumers can override this package-level variable to change the default.
var DefaultUser = "presto-go-client"

// RequestOption allows for functional overrides on individual requests
type RequestOption func(*http.Request)

// Session represents an isolated execution context linked to a Presto client
type Session struct {
	client         *Client // Link to the parent client for network transport
	userInfo       *url.Userinfo
	basicAuth      string
	catalog        string
	schema         string
	timezone       string
	clientInfo     string
	source         string
	transactionId  string
	sessionParams  map[string]any
	clientTags     []string
	requestOptions []RequestOption

	// mu protects session state during concurrent access
	mu sync.RWMutex
}

// Client serves as the factory and network configuration provider.
// The clientMu protects Client-level fields (httpClient, serverUrl, isTrino,
// forceHTTPS) during concurrent access. Session-level fields are separately
// protected by Session.mu.
type Client struct {
	Session    // Embedded default session
	clientMu   sync.RWMutex
	httpClient *http.Client
	serverUrl  *url.URL
	isTrino    bool
	forceHTTPS bool
}

// --- Initialization & Lifecycle ---

// NewClient initializes the client and links its embedded session to itself.
// basicAuth is an optional variadic parameter.
func NewClient(serverUrl string, basicAuth ...string) (*Client, error) {
	parsedUrl, err := url.Parse(serverUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %w", err)
	}

	c := &Client{
		httpClient: &http.Client{},
		serverUrl:  parsedUrl,
		Session: Session{
			userInfo:      url.User(DefaultUser),
			sessionParams: make(map[string]any),
		},
	}

	// Link the embedded session to the client
	c.Session.client = c

	if len(basicAuth) > 0 {
		c.basicAuth = basicAuth[0]
	}

	return c, nil
}

// Clone creates an isolated session copy that maintains the same client link
func (s *Session) Clone() *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	params := make(map[string]any, len(s.sessionParams))
	maps.Copy(params, s.sessionParams)

	tags := make([]string, len(s.clientTags))
	copy(tags, s.clientTags)

	opts := make([]RequestOption, len(s.requestOptions))
	copy(opts, s.requestOptions)

	return &Session{
		client:         s.client, // Maintain the same network client
		userInfo:       s.userInfo,
		basicAuth:      s.basicAuth,
		catalog:        s.catalog,
		schema:         s.schema,
		timezone:       s.timezone,
		clientInfo:     s.clientInfo,
		source:         s.source,
		transactionId:  s.transactionId,
		sessionParams:  params,
		clientTags:     tags,
		requestOptions: opts,
	}
}

// --- Session Setters (Fluent API) ---

func (s *Session) Catalog(catalog string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalog = catalog
	return s
}

func (s *Session) Schema(schema string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schema = schema
	return s
}

func (s *Session) User(user string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userInfo = url.User(user)
	return s
}

func (s *Session) UserPassword(user, password string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userInfo = url.UserPassword(user, password)
	return s
}

func (s *Session) TimeZone(tz string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timezone = tz
	return s
}

func (s *Session) ClientInfo(info string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientInfo = info
	return s
}

func (s *Session) Source(source string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.source = source
	return s
}

// SessionParam sets or removes a session parameter. Set value to nil to remove.
func (s *Session) SessionParam(key string, value any) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == nil {
		delete(s.sessionParams, key)
	} else {
		s.sessionParams[key] = value
	}
	return s
}

func (s *Session) ClearSessionParams() *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionParams = make(map[string]any)
	return s
}

func (s *Session) ClientTags(tags ...string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientTags = tags
	return s
}

func (s *Session) AppendClientTag(tags ...string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientTags = append(s.clientTags, tags...)
	return s
}

// --- Session Getters ---

// GetCatalog returns the current catalog for this session.
func (s *Session) GetCatalog() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.catalog
}

// GetSchema returns the current schema for this session.
func (s *Session) GetSchema() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schema
}

// GetTimeZone returns the current timezone for this session.
func (s *Session) GetTimeZone() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.timezone
}

// GetSessionParams returns the formatted session parameters header value.
func (s *Session) GetSessionParams() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.sessionParams) == 0 {
		return ""
	}
	return s.client.generateSessionHeader(s.sessionParams)
}

// RequestOptions sets persistent request options that are applied to every
// request made by this session. This is useful for authentication schemes
// (e.g., Kerberos/SPNEGO) that must apply to all requests including batch fetches.
func (s *Session) RequestOptions(opts ...RequestOption) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestOptions = opts
	return s
}

// --- Request Lifecycle (No Client argument needed) ---

// NewRequest builds an http.Request using internal session and client states, accepting optional overrides.
func (s *Session) NewRequest(method, urlStr string, body any, options ...RequestOption) (*http.Request, error) {
	u, err := s.client.prepareURL(urlStr)
	if err != nil {
		return nil, err
	}

	bodyReader, contentType, err := s.client.prepareRequestBody(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, u.String(), bodyReader)
	if err != nil {
		return nil, err
	}

	s.applyHeaders(req)

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept-Encoding", ContentEncodingGzip)

	// Apply session-level options first, then per-call overrides
	s.mu.RLock()
	sessionOpts := s.requestOptions
	s.mu.RUnlock()
	for _, opt := range sessionOpts {
		opt(req)
	}
	for _, opt := range options {
		opt(req)
	}

	return req, nil
}

func (s *Session) applyHeaders(req *http.Request) {
	// Snapshot isTrino before taking the session lock to avoid nested locking
	// (CanonicalHeader takes clientMu; holding s.mu first would create a lock-order risk).
	s.client.clientMu.RLock()
	isTrino := s.client.isTrino
	s.client.clientMu.RUnlock()

	canonical := func(name string) string {
		if isTrino {
			return strings.Replace(name, "X-Presto", "X-Trino", 1)
		}
		return name
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. Identity & Auth
	if s.userInfo != nil {
		req.Header.Set(canonical(UserHeader), s.userInfo.Username())
		if s.basicAuth != "" {
			req.Header.Set("Authorization", "Basic "+s.basicAuth)
		} else if pass, ok := s.userInfo.Password(); ok {
			req.SetBasicAuth(s.userInfo.Username(), pass)
		}
	}

	// 2. Contextual Headers
	if s.catalog != "" {
		req.Header.Set(canonical(CatalogHeader), s.catalog)
	}
	if s.schema != "" {
		req.Header.Set(canonical(SchemaHeader), s.schema)
	}
	if s.timezone != "" {
		req.Header.Set(canonical(TimeZoneHeader), s.timezone)
	}
	if s.clientInfo != "" {
		req.Header.Set(canonical(ClientInfoHeader), s.clientInfo)
	}
	if s.source != "" {
		req.Header.Set(canonical(SourceHeader), s.source)
	}

	// 3. State Headers
	if s.transactionId != "" {
		req.Header.Set(canonical(TransactionHeader), s.transactionId)
	}
	if len(s.sessionParams) > 0 {
		// generateSessionHeader is lock-free; safe to call inside s.mu
		req.Header.Set(canonical(SessionHeader), s.client.generateSessionHeader(s.sessionParams))
	}
	if len(s.clientTags) > 0 {
		req.Header.Set(canonical(ClientTagHeader), strings.Join(s.clientTags, ","))
	}
}

// --- Execution & Transaction Synchronization ---

// Do executes the request and automatically manages transaction state.
// When error is non-nil, the response may still be non-nil (e.g., on non-2xx status codes)
// but its Body has been fully consumed and closed.
func (s *Session) Do(ctx context.Context, req *http.Request, v any) (*http.Response, error) {
	req = req.WithContext(ctx)

	// Buffer the request body so it can be replayed on retries.
	// io.Reader is consumed after the first attempt, so we need GetBody.
	if req.Body != nil && req.GetBody == nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
	}

	s.client.clientMu.RLock()
	httpClient := s.client.httpClient
	s.client.clientMu.RUnlock()

	retryDelay := time.Second
	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		resp, err := httpClient.Do(req)
		if err != nil {
			// Bail out immediately if the context is done, even if the error
			// itself looks like a retryable network error (e.g. signal interrupt).
			if ctx.Err() != nil {
				return nil, err
			}
			// Retry on transient network errors, but not on context cancellation
			if !isRetryableNetError(err) {
				return nil, err
			}

			log.Debug().Err(err).Int("attempt", attempt+1).Msg("retrying on connection error")

			if req.GetBody != nil {
				body, bodyErr := req.GetBody()
				if bodyErr != nil {
					return nil, fmt.Errorf("failed to reset request body for retry: %w", bodyErr)
				}
				req.Body = body
			}

			if err := retrySleep(ctx, retryDelay); err != nil {
				return nil, err
			}
			retryDelay *= 2
			if retryDelay > MaxRetryDelay {
				retryDelay = MaxRetryDelay
			}
			continue
		}

		s.updateTransactionState(resp)
		s.updateSessionProperties(resp)

		if resp.StatusCode == http.StatusOK {
			err = s.client.decodeResponseBody(resp, v)
			return resp, err
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			if closeErr := resp.Body.Close(); closeErr != nil {
				log.Debug().Err(closeErr).Msg("failed to close response body")
			}

			// Reset the request body for the next attempt
			if req.GetBody != nil {
				body, bodyErr := req.GetBody()
				if bodyErr != nil {
					return nil, fmt.Errorf("failed to reset request body for retry: %w", bodyErr)
				}
				req.Body = body
			}

			if err := retrySleep(ctx, retryDelay); err != nil {
				return nil, err
			}
			retryDelay *= 2
			if retryDelay > MaxRetryDelay {
				retryDelay = MaxRetryDelay
			}
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return resp, fmt.Errorf("presto server error: %d (failed to read body: %w)", resp.StatusCode, readErr)
		}
		return resp, fmt.Errorf("presto server error: %d: %s", resp.StatusCode, string(body))
	}
	return nil, fmt.Errorf("max retries exceeded")
}

// retrySleep waits for the given duration or until the context is cancelled.
func retrySleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// isRetryableNetError returns true for transient network errors that warrant
// a retry (connection refused, DNS failures, connection reset, network timeouts).
// Context cancellation and deadline exceeded errors are NOT retried.
// Note: network-layer timeouts (e.g., dial timeouts) ARE retried, unlike
// context deadlines. This is intentional — a dial timeout is transient, while
// a context deadline reflects the caller's budget.
func isRetryableNetError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// net.Error covers *net.OpError and all other network-layer errors.
	_, ok := errors.AsType[net.Error](err)
	return ok
}

func (s *Session) updateTransactionState(resp *http.Response) {
	// Snapshot isTrino before taking the session lock (same reason as applyHeaders).
	s.client.clientMu.RLock()
	isTrino := s.client.isTrino
	s.client.clientMu.RUnlock()

	canonical := func(name string) string {
		if isTrino {
			return strings.Replace(name, "X-Presto", "X-Trino", 1)
		}
		return name
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if id := resp.Header.Get(canonical(StartedTransactionHeader)); id != "" {
		s.transactionId = id
	} else if resp.Header.Get(canonical(ClearTransactionHeader)) == "true" {
		s.transactionId = ""
	}
}

// updateSessionProperties processes X-Presto-Set-Session and X-Presto-Clear-Session
// response headers. The server sends these when SET SESSION or RESET SESSION statements
// are executed. Format: "key=urlEncodedValue" for Set, "key" for Clear.
func (s *Session) updateSessionProperties(resp *http.Response) {
	setKey := s.client.CanonicalHeader(SetSessionHeader)
	clearKey := s.client.CanonicalHeader(ClearSessionHeader)

	// Cheap existence check before allocating slices.
	if resp.Header.Get(setKey) == "" && resp.Header.Get(clearKey) == "" {
		return
	}

	setValues := resp.Header.Values(setKey)
	clearValues := resp.Header.Values(clearKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, h := range setValues {
		key, val, ok := strings.Cut(h, "=")
		if !ok {
			log.Warn().Str("header", h).Msg("malformed X-Presto-Set-Session header, skipping")
			continue
		}
		decoded, err := url.QueryUnescape(val)
		if err != nil {
			log.Warn().Err(err).Str("header", h).Msg("failed to unescape X-Presto-Set-Session value, skipping")
			continue
		}
		s.sessionParams[key] = decoded
	}
	for _, h := range clearValues {
		delete(s.sessionParams, h)
	}
}

// --- Client Configuration & Delegation ---

func (c *Client) IsTrino(isTrino bool) *Client {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	c.isTrino = isTrino
	return c
}

func (c *Client) ForceHTTPS(force bool) *Client {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	c.forceHTTPS = force
	return c
}

// GetHost returns the host (and port, if present) of the server URL.
func (c *Client) GetHost() string {
	c.clientMu.RLock()
	defer c.clientMu.RUnlock()
	return c.serverUrl.Host
}

// HTTPClient replaces the underlying http.Client. Use this to provide a
// client with custom TLS configuration, timeouts, or transport settings.
func (c *Client) HTTPClient(hc *http.Client) *Client {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	c.httpClient = hc
	return c
}

// TLSConfig configures TLS on the client's HTTP transport. If the client
// already has a custom transport that is not an *http.Transport, this is
// a no-op and the caller should use HTTPClient instead.
func (c *Client) TLSConfig(cfg *tls.Config) *Client {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	transport, ok := c.httpClient.Transport.(*http.Transport)
	if !ok {
		if c.httpClient.Transport == nil {
			transport = http.DefaultTransport.(*http.Transport).Clone()
		} else {
			return c
		}
	}
	transport.TLSClientConfig = cfg
	c.httpClient.Transport = transport
	return c
}

// --- Client Networking Utilities ---

func (c *Client) prepareURL(urlStr string) (*url.URL, error) {
	c.clientMu.RLock()
	serverUrl := c.serverUrl
	forceHTTPS := c.forceHTTPS
	c.clientMu.RUnlock()

	u, err := serverUrl.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if forceHTTPS && u.Scheme == "http" {
		u.Scheme = "https"
	}
	return u, nil
}

func (c *Client) prepareRequestBody(body any) (io.Reader, string, error) {
	if body == nil {
		return nil, "", nil
	}
	if s, ok := body.(string); ok {
		return strings.NewReader(s), "text/plain", nil
	}
	jsonBuf := &bytes.Buffer{}
	if err := json.NewEncoder(jsonBuf).Encode(body); err != nil {
		return nil, "", err
	}
	return jsonBuf, "application/json", nil
}

// CanonicalHeader transforms a Presto-style header key (starting with "X-Presto-")
// into its Trino equivalent ("X-Trino-") if the client is configured in Trino mode.
// This ensures compatibility across different versions of the Presto/Trino
// ecosystem while allowing the internal code to use a single, consistent
// naming convention.
func (c *Client) CanonicalHeader(name string) string {
	c.clientMu.RLock()
	isTrino := c.isTrino
	c.clientMu.RUnlock()

	if isTrino {
		return strings.Replace(name, "X-Presto", "X-Trino", 1)
	}
	return name
}

// GenerateSessionParamsHeaderValue formats a map of session parameters into
// the header value format expected by Presto/Trino (key=value pairs joined by commas).
func (c *Client) GenerateSessionParamsHeaderValue(params map[string]any) string {
	return c.generateSessionHeader(params)
}

func (c *Client) generateSessionHeader(params map[string]any) string {
	pairs := make([]string, 0, len(params))
	for k, v := range params {
		val := url.QueryEscape(fmt.Sprintf("%v", v))
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, val))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func (c *Client) decodeResponseBody(resp *http.Response, v any) (err error) {
	// Ensure the main response body is always closed
	defer func() {
		closeErr := resp.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// 1. Early return if no destination is provided
	if v == nil {
		return nil
	}

	var reader io.Reader = resp.Body

	// 2. Handle decompression
	if resp.Header.Get("Content-Encoding") == ContentEncodingGzip {
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return fmt.Errorf("failed to create gzip reader: %w", gzErr)
		}

		// Use a closure to handle gz.Close() error
		defer func() {
			cErr := gz.Close()
			if cErr != nil {
				// We log it instead of returning it to avoid
				// overwriting primary decoding errors.
				log.Debug().Err(cErr).Msg("failed to close gzip reader")
			}
		}()
		reader = gz
	}

	// 3. Decode payload
	if w, ok := v.(io.Writer); ok {
		_, err = io.Copy(w, reader)
		return err
	}

	if err = json.NewDecoder(reader).Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// NewSession creates a new, isolated session using the client's current
// connection settings. The new session is linked to this client but
// maintains its own headers, transaction state, and tags.
func (c *Client) NewSession() *Session {
	// We call Clone on the embedded default session to ensure we pick up
	// any default settings (like basicAuth or User) already configured on the client.
	return c.Session.Clone()
}
