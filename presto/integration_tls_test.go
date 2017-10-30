// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build go1.9

package presto

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

func TestIntegrationTLS(t *testing.T) {
	proxyServer := newTLSReverseProxy(t)
	defer proxyServer.Close()
	RegisterCustomClient("test_tls", proxyServer.Client())
	defer DeregisterCustomClient("test_tls")
	dsn := proxyServer.URL + "?custom_client=test_tls"
	testSimpleQuery(t, dsn)
}

func TestIntegrationInsecureTLS(t *testing.T) {
	proxyServer := newTLSReverseProxy(t)
	defer proxyServer.Close()
	RegisterCustomClient("test_insecure_tls", &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	})
	defer DeregisterCustomClient("test_insecure_tls")
	dsn := proxyServer.URL + "?custom_client=test_insecure_tls"
	testSimpleQuery(t, dsn)
}

func testSimpleQuery(t *testing.T, dsn string) {
	db, err := sql.Open("presto", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := db.QueryRow("SELECT 1")
	var count int
	if err = row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("unexpected count=", count)
	}
}

// hax0r reverse tls proxy for integration tests

// newTLSReverseProxy creates a TLS integration test server.
func newTLSReverseProxy(t *testing.T) *httptest.Server {
	dsn := integrationServerDSN(t)
	prestoURL, _ := url.Parse(dsn)
	cproxyURL := make(chan string, 1)
	handler := newReverseProxyHandler(prestoURL, cproxyURL)
	srv := httptest.NewTLSServer(http.HandlerFunc(handler))
	cproxyURL <- srv.URL
	close(cproxyURL)
	proxyURL, _ := url.Parse(srv.URL)
	proxyURL.User = prestoURL.User
	proxyURL.Path = prestoURL.Path
	proxyURL.RawPath = prestoURL.RawPath
	proxyURL.RawQuery = prestoURL.RawQuery
	srv.URL = proxyURL.String()
	return srv
}

// newReverseProxyHandler creates an http handler that proxies requests to the given prestoURL, and replaces URLs in responses with the first value sent to the cproxyURL channel.
func newReverseProxyHandler(prestoURL *url.URL, cproxyURL chan string) http.HandlerFunc {
	baseURL := []byte(prestoURL.Scheme + "://" + prestoURL.Host)
	var proxyURL []byte
	var onceProxyURL sync.Once
	return func(w http.ResponseWriter, r *http.Request) {
		onceProxyURL.Do(func() {
			proxyURL = []byte(<-cproxyURL)
		})
		target := *prestoURL
		target.User = nil
		target.Path = r.URL.Path
		target.RawPath = r.URL.RawPath
		target.RawQuery = r.URL.RawQuery
		req, err := http.NewRequest(r.Method, target.String(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		for k, v := range r.Header {
			if strings.HasPrefix(k, "X-") {
				req.Header[k] = v
			}
		}
		client := *http.DefaultClient
		client.Timeout = *integrationServerQueryTimeout
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		pr, pw := io.Pipe()
		go func() {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			b = bytes.Replace(b, baseURL, proxyURL, -1)
			pw.Write(b)
			pw.Close()
		}()
		io.Copy(w, pr)
	}
}
