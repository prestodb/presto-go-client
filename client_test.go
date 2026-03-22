package presto

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Segment 1: Initialization & Lifecycle ---

func TestNewClient_VariadicAuth(t *testing.T) {
	t.Run("Valid URL without auth", func(t *testing.T) {
		c, err := NewClient("http://localhost:8080")
		require.NoError(t, err)
		assert.Empty(t, c.basicAuth)
		assert.Equal(t, c, c.Session.client)
	})

	t.Run("Valid URL with auth", func(t *testing.T) {
		c, err := NewClient("http://localhost:8080", "secret-token")
		require.NoError(t, err)
		assert.Equal(t, "secret-token", c.basicAuth)
	})

	t.Run("Invalid URL error", func(t *testing.T) {
		_, err := NewClient("://invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid server URL")
	})
}

func TestSession_CloneAndIsolation(t *testing.T) {
	c, _ := NewClient("http://localhost")
	c.Catalog("base").SessionParam("k", "v").ClientTags("t1")

	// Create isolated session
	s := c.NewSession()
	s.Catalog("new").SessionParam("k", "v2").AppendClientTag("t2")

	// Parent should remain untouched
	assert.Equal(t, "base", c.catalog)
	assert.Equal(t, "v", c.sessionParams["k"])
	assert.Equal(t, []string{"t1"}, c.clientTags)

	// Child should have new state
	assert.Equal(t, "new", s.catalog)
	assert.Equal(t, "v2", s.sessionParams["k"])
	assert.Equal(t, []string{"t1", "t2"}, s.clientTags)
	assert.Equal(t, c, s.client)
}

// --- Segment 2: Fluent API & Header Generation ---

func TestSession_Setters(t *testing.T) {
	s := &Session{sessionParams: make(map[string]any)}

	s.Catalog("cat").Schema("sch").User("u").UserPassword("u", "p").
		TimeZone("UTC").ClientInfo("info").ClientTags("a", "b")

	assert.Equal(t, "cat", s.catalog)
	assert.Equal(t, "sch", s.schema)
	assert.Equal(t, "UTC", s.timezone)
	assert.Equal(t, "info", s.clientInfo)
	assert.Equal(t, "u", s.userInfo.Username())

	s.SessionParam("p1", 1).SessionParam("p2", nil) // Test removal
	assert.Equal(t, 1, s.sessionParams["p1"])
	assert.NotContains(t, s.sessionParams, "p2")

	s.ClearSessionParams()
	assert.Empty(t, s.sessionParams)
}

func TestSession_Getters(t *testing.T) {
	c, _ := NewClient("http://localhost:8080")
	s := c.NewSession()
	s.Catalog("hive").Schema("default").TimeZone("UTC").SessionParam("k1", "v1")

	assert.Equal(t, "hive", s.GetCatalog())
	assert.Equal(t, "default", s.GetSchema())
	assert.Equal(t, "UTC", s.GetTimeZone())
	assert.Contains(t, s.GetSessionParams(), "k1=v1")
}

func TestSession_GetSessionParams_Empty(t *testing.T) {
	c, _ := NewClient("http://localhost")
	s := c.NewSession()
	assert.Empty(t, s.GetSessionParams())
}

func TestClient_GetHost(t *testing.T) {
	c, _ := NewClient("http://myhost:8080")
	assert.Equal(t, "myhost:8080", c.GetHost())
}

func TestSession_AppendClientTag_Variadic(t *testing.T) {
	c, _ := NewClient("http://localhost")
	s := c.NewSession()
	s.ClientTags("t1")
	s.AppendClientTag("t2", "t3")
	assert.Equal(t, []string{"t1", "t2", "t3"}, s.clientTags)
}

func TestDefaultUser_IsVar(t *testing.T) {
	original := DefaultUser
	defer func() { DefaultUser = original }()

	DefaultUser = "custom-user"
	c, _ := NewClient("http://localhost")
	assert.Equal(t, "custom-user", c.userInfo.Username())
}

func TestClient_GenerateSessionParamsHeaderValue(t *testing.T) {
	c, _ := NewClient("http://localhost")
	params := map[string]any{"key": "value"}
	header := c.GenerateSessionParamsHeaderValue(params)
	assert.Equal(t, "key=value", header)
}

func TestClient_CanonicalHeader(t *testing.T) {
	c, _ := NewClient("http://localhost")

	// Presto Mode
	assert.Equal(t, "X-Presto-User", c.CanonicalHeader("X-Presto-User"))

	// Trino Mode
	c.IsTrino(true)
	assert.Equal(t, "X-Trino-User", c.CanonicalHeader("X-Presto-User"))
}

func TestGenerateSessionHeader(t *testing.T) {
	c := &Client{}
	params := map[string]any{
		"path": "/a/b",
		"val":  100,
	}
	header := c.generateSessionHeader(params)

	// Map iteration is random, check containment
	assert.Contains(t, header, "path=%2Fa%2Fb")
	assert.Contains(t, header, "val=100")
	assert.Contains(t, header, ",")
}

// --- Segment 3: Request Building & Body Handling ---

func TestNewRequest_OptionsAndEncoding(t *testing.T) {
	c, _ := NewClient("http://localhost")
	c.ForceHTTPS(true)
	s := c.NewSession().Catalog("c")

	t.Run("JSON Body encoding", func(t *testing.T) {
		body := map[string]string{"sql": "select 1"}
		req, err := s.NewRequest("POST", "/v1/statement", body)
		require.NoError(t, err)
		assert.Equal(t, "https://localhost/v1/statement", req.URL.String())
		assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
	})

	t.Run("Raw string body", func(t *testing.T) {
		req, _ := s.NewRequest("POST", "/", "SELECT 1")
		assert.Equal(t, "text/plain", req.Header.Get("Content-Type"))
	})

	t.Run("Request Options override", func(t *testing.T) {
		opt := func(r *http.Request) { r.Header.Set("X-Custom", "123") }
		req, _ := s.NewRequest("GET", "/", nil, opt)
		assert.Equal(t, "123", req.Header.Get("X-Custom"))
	})
}

// --- Segment 4: Do & Transaction State ---

func TestDo_RetryAndState(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Presto-Started-Transaction-Id", "tx123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL)
	s := c.NewSession()

	var res map[string]string
	req, _ := s.NewRequest("GET", "/", nil)
	_, err := s.Do(context.Background(), req, &res)

	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	assert.Equal(t, "tx123", s.transactionId)

	// Test Clearing Transaction
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Presto-Clear-Transaction-Id", "true")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv2.Close()

	req2, _ := s.NewRequest("GET", srv2.URL, nil)
	_, _ = s.Do(context.Background(), req2, nil)
	assert.Empty(t, s.transactionId)
}

func TestDo_SetSessionProperties(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Presto-Set-Session", "optimize_hash_generation=true")
		w.Header().Add("X-Presto-Set-Session", "execution_policy=a%2B1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL)
	s := c.NewSession()

	req, _ := s.NewRequest("GET", "/", nil)
	_, err := s.Do(context.Background(), req, nil)

	require.NoError(t, err)
	assert.Equal(t, "true", s.sessionParams["optimize_hash_generation"])
	assert.Equal(t, "a+1", s.sessionParams["execution_policy"])
}

func TestDo_ClearSessionProperties(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Presto-Clear-Session", "foo")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL)
	s := c.NewSession().SessionParam("foo", "bar").SessionParam("baz", "qux")

	req, _ := s.NewRequest("GET", "/", nil)
	_, err := s.Do(context.Background(), req, nil)

	require.NoError(t, err)
	_, hasFoo := s.sessionParams["foo"]
	assert.False(t, hasFoo, "foo should have been cleared")
	assert.Equal(t, "qux", s.sessionParams["baz"])
}

func TestDo_RetryBodyHandling(t *testing.T) {
	newRetryServer := func(failCount int) (*httptest.Server, *int, *[]string) {
		attempts := new(int)
		var bodies []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*attempts++
			body, _ := io.ReadAll(r.Body)
			bodies = append(bodies, string(body))
			if *attempts <= failCount {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		return srv, attempts, &bodies
	}

	t.Run("Opaque body preserved across retries", func(t *testing.T) {
		srv, attempts, bodies := newRetryServer(2)
		defer srv.Close()

		c, _ := NewClient(srv.URL)
		s := c.NewSession()

		// Build request with an opaque io.Reader body that Go's http.NewRequest
		// cannot snapshot (no GetBody auto-set). This exposes the consumed-body bug.
		bodyContent := "SELECT 1"
		opaqueReader := io.NopCloser(strings.NewReader(bodyContent))
		req, _ := http.NewRequest("POST", srv.URL+"/", opaqueReader)
		s.applyHeaders(req)

		var res map[string]string
		_, err := s.Do(context.Background(), req, &res)

		require.NoError(t, err)
		assert.Equal(t, 3, *attempts)
		for i, body := range *bodies {
			assert.Equal(t, bodyContent, body, "attempt %d should have full body", i+1)
		}
	})

	t.Run("Nil body retries without panic", func(t *testing.T) {
		srv, attempts, _ := newRetryServer(2)
		defer srv.Close()

		c, _ := NewClient(srv.URL)
		s := c.NewSession()

		req, _ := s.NewRequest("GET", "/", nil)
		var res map[string]string
		_, err := s.Do(context.Background(), req, &res)

		require.NoError(t, err)
		assert.Equal(t, 3, *attempts)
		assert.Equal(t, "ok", res["status"])
	})
}

func TestDo_ErrorResponseBodyClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid query syntax"))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL)
	s := c.NewSession()

	req, _ := s.NewRequest("GET", "/", nil)
	resp, err := s.Do(context.Background(), req, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
	assert.Contains(t, err.Error(), "invalid query syntax")
	assert.NotNil(t, resp)
}

func TestNewErrorResponse(t *testing.T) {
	t.Run("Reads body and formats error", func(t *testing.T) {
		resp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(strings.NewReader("bad SQL syntax")),
		}
		err := NewErrorResponse(resp)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, "bad SQL syntax", errResp.Message)
		assert.Equal(t, http.StatusBadRequest, errResp.Response.StatusCode)
		assert.Equal(t, "bad SQL syntax (status code: 400)", err.Error())
	})

	t.Run("Empty body", func(t *testing.T) {
		resp := &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(strings.NewReader("")),
		}
		err := NewErrorResponse(resp)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Empty(t, errResp.Message)
	})
}

// failingRoundTripper simulates transient connection failures before delegating
// to a real transport.
type failingRoundTripper struct {
	failCount int
	calls     int
	wrapped   http.RoundTripper
}

func (f *failingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	f.calls++
	if f.calls <= f.failCount {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}
	}
	return f.wrapped.RoundTrip(req)
}

func TestDo_ConnectionErrorRetry(t *testing.T) {
	t.Run("Retries on connection error then succeeds", func(t *testing.T) {
		attempts := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer srv.Close()

		c, _ := NewClient(srv.URL)
		s := c.NewSession()

		// Inject a transport that fails 2 times then delegates to the real server
		c.httpClient.Transport = &failingRoundTripper{
			failCount: 2,
			wrapped:   srv.Client().Transport,
		}

		var res map[string]string
		req, _ := s.NewRequest("GET", "/", nil)
		_, err := s.Do(context.Background(), req, &res)

		require.NoError(t, err)
		assert.Equal(t, 1, attempts, "server should be hit once after 2 transport failures")
		assert.Equal(t, "ok", res["status"])
	})

	t.Run("Does not retry on context cancellation", func(t *testing.T) {
		c, _ := NewClient("http://127.0.0.1:1") // port 1 is never open
		s := c.NewSession()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		req, _ := s.NewRequest("GET", "/", nil)
		_, err := s.Do(ctx, req, nil)

		require.Error(t, err)
		// Should return immediately, NOT "max retries exceeded"
		assert.NotContains(t, err.Error(), "max retries exceeded")
	})
}

func TestIsRetryableNetError(t *testing.T) {
	t.Run("Context canceled is not retryable", func(t *testing.T) {
		assert.False(t, isRetryableNetError(context.Canceled))
	})

	t.Run("Context deadline exceeded is not retryable", func(t *testing.T) {
		assert.False(t, isRetryableNetError(context.DeadlineExceeded))
	})

	t.Run("Generic error is not retryable", func(t *testing.T) {
		assert.False(t, isRetryableNetError(fmt.Errorf("some other error")))
	})

	t.Run("Net OpError is retryable", func(t *testing.T) {
		err := &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}
		assert.True(t, isRetryableNetError(err))
	})

	t.Run("Wrapped net error is retryable", func(t *testing.T) {
		inner := &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}
		wrapped := fmt.Errorf("request failed: %w", inner)
		assert.True(t, isRetryableNetError(wrapped))
	})
}

// --- Segment 5: Decode & Decompression ---

func TestDecodeResponseBody_Corners(t *testing.T) {
	c := &Client{}

	t.Run("Nil destination", func(t *testing.T) {
		resp := &http.Response{Body: io.NopCloser(strings.NewReader("data"))}
		err := c.decodeResponseBody(resp, nil)
		assert.NoError(t, err)
	})

	t.Run("io.Writer destination", func(t *testing.T) {
		resp := &http.Response{Body: io.NopCloser(strings.NewReader("raw-data"))}
		buf := &bytes.Buffer{}
		err := c.decodeResponseBody(resp, buf)
		require.NoError(t, err)
		assert.Equal(t, "raw-data", buf.String())
	})

	t.Run("Gzip handling", func(t *testing.T) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, _ = gw.Write([]byte(`{"a":1}`))
		_ = gw.Close()

		resp := &http.Response{
			Header: make(http.Header),
			Body:   io.NopCloser(&buf),
		}
		resp.Header.Set("Content-Encoding", "gzip")

		var out map[string]int
		err := c.decodeResponseBody(resp, &out)
		require.NoError(t, err)
		assert.Equal(t, 1, out["a"])
	})

	t.Run("Gzip error", func(t *testing.T) {
		resp := &http.Response{
			Header: make(http.Header),
			Body:   io.NopCloser(strings.NewReader("not-gzipped")),
		}
		resp.Header.Set("Content-Encoding", "gzip")
		err := c.decodeResponseBody(resp, &map[string]any{})
		assert.Error(t, err)
	})
}

// --- Segment 6: Persistent RequestOptions ---

func TestSession_RequestOptions(t *testing.T) {
	c, _ := NewClient("http://localhost")
	s := c.NewSession()

	opt := func(r *http.Request) { r.Header.Set("Authorization", "Negotiate abc") }
	s.RequestOptions(opt)

	req, err := s.NewRequest("GET", "/", nil)
	require.NoError(t, err)
	assert.Equal(t, "Negotiate abc", req.Header.Get("Authorization"))
}

func TestSession_RequestOptions_PerCallOverride(t *testing.T) {
	c, _ := NewClient("http://localhost")
	s := c.NewSession()

	sessionOpt := func(r *http.Request) { r.Header.Set("X-Custom", "session") }
	s.RequestOptions(sessionOpt)

	callOpt := func(r *http.Request) { r.Header.Set("X-Custom", "call") }
	req, err := s.NewRequest("GET", "/", nil, callOpt)
	require.NoError(t, err)
	assert.Equal(t, "call", req.Header.Get("X-Custom"), "per-call options should override session-level")
}

func TestSession_RequestOptions_ClonePreserved(t *testing.T) {
	c, _ := NewClient("http://localhost")
	s := c.NewSession()

	opt := func(r *http.Request) { r.Header.Set("Authorization", "Negotiate xyz") }
	s.RequestOptions(opt)

	cloned := s.Clone()

	// Cloned session should retain the request option
	req, err := cloned.NewRequest("GET", "/", nil)
	require.NoError(t, err)
	assert.Equal(t, "Negotiate xyz", req.Header.Get("Authorization"))

	// Modifying cloned session's options should not affect original
	cloned.RequestOptions()
	origReq, _ := s.NewRequest("GET", "/", nil)
	assert.Equal(t, "Negotiate xyz", origReq.Header.Get("Authorization"))
}

// --- Segment 7: TLS Configuration ---

func TestClient_TLSConfig(t *testing.T) {
	c, _ := NewClient("http://localhost")
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	c.TLSConfig(tlsCfg)

	transport, ok := c.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, tlsCfg, transport.TLSClientConfig)
}

func TestClient_HTTPClient(t *testing.T) {
	c, _ := NewClient("http://localhost")
	custom := &http.Client{Timeout: 42}
	c.HTTPClient(custom)
	assert.Equal(t, custom, c.httpClient)
}

// --- Segment 8: Concurrency Safety ---

func TestSession_Concurrency(t *testing.T) {
	c, _ := NewClient("http://localhost")
	var wg sync.WaitGroup
	const count = 50

	for i := range count {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s := c.NewSession().Catalog(fmt.Sprintf("cat-%d", id))
			req, _ := s.NewRequest("GET", "/", nil)
			// Header check ensures mu lock is effective
			assert.Contains(t, req.Header.Get("X-Presto-Catalog"), fmt.Sprintf("cat-%d", id))
		}(i)
	}
	wg.Wait()
}
