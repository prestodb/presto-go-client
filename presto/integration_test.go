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

package presto

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	integrationServerFlag = flag.String(
		"presto_server_dsn",
		os.Getenv("PRESTO_SERVER_DSN"),
		"dsn of the presto server used for integration tests; default disabled",
	)
	integrationServerQueryTimeout = flag.Duration(
		"presto_query_timeout",
		5*time.Second,
		"max duration for presto queries to run before giving up",
	)
)

func init() {
	flag.Parse()
	DefaultQueryTimeout = *integrationServerQueryTimeout
	DefaultCancelQueryTimeout = *integrationServerQueryTimeout
}

// integrationServerDSN returns the URL of the integration test server.
func integrationServerDSN(t *testing.T) string {
	dsn := *integrationServerFlag
	if dsn == "" {
		t.Skip()
	}
	return dsn
}

// integrationOpen opens a connection to the integration test server.
func integrationOpen(t *testing.T, dsn ...string) *sql.DB {
	target := integrationServerDSN(t)
	if len(dsn) > 0 {
		target = dsn[0]
	}
	db, err := sql.Open("presto", target)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// integration tests based on python tests:
// https://github.com/prestodb/presto-python-client/tree/master/integration_tests

func TestIntegrationEnabled(t *testing.T) {
	dsn := *integrationServerFlag
	if dsn == "" {
		example := "http://test@localhost:8080"
		t.Skip("integration tests not enabled; use e.g. -presto_server_dsn=" + example)
	}
}

type nodesRow struct {
	NodeID      string
	HTTPURI     string
	NodeVersion string
	Coordinator bool
	State       string
}

func TestIntegrationSelectQueryIterator(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM system.runtime.nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col nodesRow
		err = rows.Scan(
			&col.NodeID,
			&col.HTTPURI,
			&col.NodeVersion,
			&col.Coordinator,
			&col.State,
		)
		if err != nil {
			t.Fatal(err)
		}
		if col.NodeID != "test" {
			t.Fatal("node_id != test")
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count < 1 {
		t.Fatal("no rows returned")
	}
}

func TestIntegrationSelectQueryNoResult(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	row := db.QueryRow("SELECT * FROM system.runtime.nodes where false")
	var col nodesRow
	err := row.Scan(
		&col.NodeID,
		&col.HTTPURI,
		&col.NodeVersion,
		&col.Coordinator,
		&col.State,
	)
	if err == nil {
		t.Fatalf("unexpected query returning data: %+v", col)
	}
}

func TestIntegrationSelectFailedQuery(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM catalog.schema.do_not_exist")
	if err == nil {
		rows.Close()
		t.Fatal("query to invalid catalog succeeded")
	}
	_, ok := err.(*ErrQueryFailed)
	if !ok {
		t.Fatal("unexpected error:", err)
	}
}

type tpchRow struct {
	CustKey    int
	Name       string
	Address    string
	NationKey  int
	Phone      string
	AcctBal    float64
	MktSegment string
	Comment    string
}

func TestIntegrationSelectTpch1000(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM tpch.sf1.customer LIMIT 1000")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col tpchRow
		err = rows.Scan(
			&col.CustKey,
			&col.Name,
			&col.Address,
			&col.NationKey,
			&col.Phone,
			&col.AcctBal,
			&col.MktSegment,
			&col.Comment,
		)
		if err != nil {
			t.Fatal(err)
		}
		/*
			if col.CustKey == 1 && col.AcctBal != 711.56 {
				t.Fatal("unexpected acctbal for custkey=1:", col.AcctBal)
			}
		*/
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
	if count != 1000 {
		t.Fatal("not enough rows returned:", count)
	}
}

func TestIntegrationSelectCancelQuery(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	deadline := time.Now().Add(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT * FROM tpch.sf1.customer")
	if err != nil {
		goto handleErr
	}
	defer rows.Close()
	for rows.Next() {
		var col tpchRow
		err = rows.Scan(
			&col.CustKey,
			&col.Name,
			&col.Address,
			&col.NationKey,
			&col.Phone,
			&col.AcctBal,
			&col.MktSegment,
			&col.Comment,
		)
		if err != nil {
			break
		}
	}
	if err = rows.Err(); err == nil {
		t.Fatal("unexpected query with deadline succeeded")
	}
handleErr:
	errmsg := err.Error()
	for _, msg := range []string{"cancel", "deadline"} {
		if strings.Contains(errmsg, msg) {
			return
		}
	}
	t.Fatal("unexpected error:", err)
}

func TestIntegrationSessionProperties(t *testing.T) {
	dsn := integrationServerDSN(t)
	dsn += "?session_properties=query_max_run_time=10m,query_priority=2"
	db := integrationOpen(t, dsn)
	defer db.Close()
	rows, err := db.Query("SHOW SESSION")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		col := struct {
			Name        string
			Value       string
			Default     string
			Type        string
			Description string
		}{}
		err = rows.Scan(
			&col.Name,
			&col.Value,
			&col.Default,
			&col.Type,
			&col.Description,
		)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case col.Name == "query_max_run_time" && col.Value != "10m":
			t.Fatal("unexpected value for query_max_run_time:", col.Value)
		case col.Name == "query_priority" && col.Value != "2":
			t.Fatal("unexpected value for query_priority:", col.Value)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationTypeConversion(t *testing.T) {
	db := integrationOpen(t)
	var (
		goTime     time.Time
		nullTime   NullTime
		goString   string
		nullString sql.NullString
		i64slice   NullSliceInt64
		f64slice2  NullSlice2Float64
		goMap      map[string]interface{}
		nullMap    NullMap
	)
	err := db.QueryRow(`
		SELECT
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST(NULL AS TIMESTAMP),
			CAST('string' AS VARCHAR),
			CAST(NULL AS VARCHAR),
			ARRAY[1, 2, 3],
			ARRAY[ARRAY[1, 1, 1], ARRAY[2, 2, 2]],
			MAP(ARRAY['a', 'b'], ARRAY['c', 'd']),
			CAST(NULL AS MAP(ARRAY(INTEGER), ARRAY(INTEGER)))
	`).Scan(
		&goTime,
		&nullTime,
		&goString,
		&nullString,
		&i64slice,
		&f64slice2,
		&goMap,
		&nullMap,
	)
	if err != nil {
		t.Fatal(err)
	}
}

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
