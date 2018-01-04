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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	c := &Config{
		PrestoURI:         "http://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
	}
	dsn, err := c.FormatDSN()
	if err != nil {
		t.Fatal(err)
	}
	want := "http://foobar@localhost:8080?session_properties=query_priority%3D1&source=presto-go-client"
	if dsn != want {
		t.Fatal("unexpected dsn:", dsn)
	}
}

func TestConfigWithMalformedURL(t *testing.T) {
	_, err := (&Config{PrestoURI: ":("}).FormatDSN()
	if err == nil {
		t.Fatal("dsn generated from malformed url")
	}
}

func TestConnErrorDSN(t *testing.T) {
	testcases := []struct {
		Name string
		DSN  string
	}{
		{Name: "malformed", DSN: "://"},
		{Name: "unknown_client", DSN: "http://localhost?custom_client=unknown"},
	}
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			db, err := sql.Open("presto", tc.DSN)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Query("SELECT 1"); err == nil {
				db.Close()
				t.Fatal("test dsn is supposed to fail:", tc.DSN)
			}
		})
	}
}

func TestRegisterCustomClientReserved(t *testing.T) {
	for _, tc := range []string{"true", "false"} {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			err := RegisterCustomClient(tc, &http.Client{})
			if err == nil {
				t.Fatal("client key name supposed to fail:", tc)
			}
		})
	}
}

func TestRoundTripRetryQueryError(t *testing.T) {
	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
			count++
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: "TEST",
			},
		})
	}))
	defer ts.Close()
	db, err := sql.Open("presto", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if _, ok := err.(*ErrQueryFailed); !ok {
		t.Fatal("unexpected error:", err)
	}
}

func TestRoundTripCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()
	db, err := sql.Open("presto", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = db.QueryContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("unexpected query with cancelled context succeeded")
	}
}

func TestQueryCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: "USER_CANCELLED",
			},
		})
	}))
	defer ts.Close()
	db, err := sql.Open("presto", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if err != ErrQueryCancelled {
		t.Fatal("unexpected error:", err)
	}
}

func TestQueryFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	db, err := sql.Open("presto", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if _, ok := err.(*ErrQueryFailed); !ok {
		t.Fatal("unexpected error:", err)
	}
}

func TestUnsupportedExec(t *testing.T) {
	db, err := sql.Open("presto", "http://localhost:9")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE foobar (V VARCHAR)"); err == nil {
		t.Fatal("unsupported exec succeeded with no error")
	}
}

func TestUnsupportedTransaction(t *testing.T) {
	db, err := sql.Open("presto", "http://localhost:9")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Begin(); err == nil {
		t.Fatal("unsupported transaction succeeded with no error")
	}
}

func TestTypeConversion(t *testing.T) {
	utc, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}
	testcases := []struct {
		PrestoType                       string
		PrestoResponseUnmarshalledSample interface{}
		ExpectedGoValue                  interface{}
	}{
		{
			PrestoType:                       "boolean",
			PrestoResponseUnmarshalledSample: true,
			ExpectedGoValue:                  true,
		},
		{
			PrestoType:                       "varchar(1)",
			PrestoResponseUnmarshalledSample: "hello",
			ExpectedGoValue:                  "hello",
		},
		{
			PrestoType:                       "bigint",
			PrestoResponseUnmarshalledSample: float64(1),
			ExpectedGoValue:                  int64(1),
		},
		{
			PrestoType:                       "double",
			PrestoResponseUnmarshalledSample: float64(1),
			ExpectedGoValue:                  float64(1),
		},
		{
			PrestoType:                       "date",
			PrestoResponseUnmarshalledSample: "2017-07-10",
			ExpectedGoValue:                  time.Date(2017, 7, 10, 0, 0, 0, 0, utc),
		},
		{
			PrestoType:                       "time",
			PrestoResponseUnmarshalledSample: "01:02:03.000",
			ExpectedGoValue:                  time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			PrestoType:                       "time with time zone",
			PrestoResponseUnmarshalledSample: "01:02:03.000 UTC",
			ExpectedGoValue:                  time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			PrestoType:                       "timestamp",
			PrestoResponseUnmarshalledSample: "2017-07-10 01:02:03.000",
			ExpectedGoValue:                  time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			PrestoType:                       "timestamp with time zone",
			PrestoResponseUnmarshalledSample: "2017-07-10 01:02:03.000 UTC",
			ExpectedGoValue:                  time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			PrestoType:                       "map",
			PrestoResponseUnmarshalledSample: nil,
			ExpectedGoValue:                  nil,
		},
		{
			// arrays return data as-is for slice scanners
			PrestoType:                       "array",
			PrestoResponseUnmarshalledSample: nil,
			ExpectedGoValue:                  nil,
		},
	}
	for _, tc := range testcases {
		converter := newTypeConverter(tc.PrestoType)

		t.Run(tc.PrestoType+":nil", func(t *testing.T) {
			if _, err := converter.ConvertValue(nil); err != nil {
				t.Fatal(err)
			}
		})

		t.Run(tc.PrestoType+":bogus", func(t *testing.T) {
			if _, err := converter.ConvertValue(struct{}{}); err == nil {
				t.Fatal("bogus data scanned with no error")
			}
		})
		t.Run(tc.PrestoType+":sample", func(t *testing.T) {
			v, err := converter.ConvertValue(tc.PrestoResponseUnmarshalledSample)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(v, tc.ExpectedGoValue) {
				t.Fatalf("unexpected data from sample:\nhave %+v\nwant %+v", v, tc.ExpectedGoValue)
			}
		})
	}
}

func TestSliceTypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                           string
		Scanner                          sql.Scanner
		PrestoResponseUnmarshalledSample interface{}
		TestScanner                      func(t *testing.T, s sql.Scanner)
	}{
		{
			GoType:  "[]bool",
			Scanner: &NullSliceBool{},
			PrestoResponseUnmarshalledSample: []interface{}{true},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSliceBool)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[]string",
			Scanner: &NullSliceString{},
			PrestoResponseUnmarshalledSample: []interface{}{"hello"},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSliceString)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[]int64",
			Scanner: &NullSliceInt64{},
			PrestoResponseUnmarshalledSample: []interface{}{float64(1)},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSliceInt64)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},

		{
			GoType:  "[]float64",
			Scanner: &NullSliceFloat64{},
			PrestoResponseUnmarshalledSample: []interface{}{float64(1)},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSliceFloat64)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[]time.Time",
			Scanner: &NullSliceTime{},
			PrestoResponseUnmarshalledSample: []interface{}{"2017-07-01"},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSliceTime)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[]map[string]interface{}",
			Scanner: &NullSliceMap{},
			PrestoResponseUnmarshalledSample: []interface{}{map[string]interface{}{"hello": "world"}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSliceMap)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			if err := tc.Scanner.Scan(nil); err != nil {
				t.Error(err)
			}
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			if err := tc.Scanner.Scan(struct{}{}); err == nil {
				t.Error("bogus data scanned with no error")
			}
			if err := tc.Scanner.Scan([]interface{}{struct{}{}}); err == nil {
				t.Error("bogus data scanned with no error")
			}
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			if err := tc.Scanner.Scan(tc.PrestoResponseUnmarshalledSample); err != nil {
				t.Error(err)
			}
			tc.TestScanner(t, tc.Scanner)
		})
	}
}

func TestSlice2TypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                           string
		Scanner                          sql.Scanner
		PrestoResponseUnmarshalledSample interface{}
		TestScanner                      func(t *testing.T, s sql.Scanner)
	}{
		{
			GoType:  "[][]bool",
			Scanner: &NullSlice2Bool{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{true}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice2Bool)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][]string",
			Scanner: &NullSlice2String{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{"hello"}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice2String)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][]int64",
			Scanner: &NullSlice2Int64{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{float64(1)}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice2Int64)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][]float64",
			Scanner: &NullSlice2Float64{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{float64(1)}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice2Float64)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][]time.Time",
			Scanner: &NullSlice2Time{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{"2017-07-01"}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice2Time)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][]map[string]interface{}",
			Scanner: &NullSlice2Map{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice2Map)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			if err := tc.Scanner.Scan(nil); err != nil {
				t.Error(err)
			}
			if err := tc.Scanner.Scan([]interface{}{nil}); err != nil {
				t.Error(err)
			}
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			if err := tc.Scanner.Scan(struct{}{}); err == nil {
				t.Error("bogus data scanned with no error")
			}
			if err := tc.Scanner.Scan([]interface{}{struct{}{}}); err == nil {
				t.Error("bogus data scanned with no error")
			}
			if err := tc.Scanner.Scan([]interface{}{[]interface{}{struct{}{}}}); err == nil {
				t.Error("bogus data scanned with no error")
			}
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			if err := tc.Scanner.Scan(tc.PrestoResponseUnmarshalledSample); err != nil {
				t.Error(err)
			}
			tc.TestScanner(t, tc.Scanner)
		})
	}
}

func TestSlice3TypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                           string
		Scanner                          sql.Scanner
		PrestoResponseUnmarshalledSample interface{}
		TestScanner                      func(t *testing.T, s sql.Scanner)
	}{
		{
			GoType:  "[][][]bool",
			Scanner: &NullSlice3Bool{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{true}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice3Bool)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][][]string",
			Scanner: &NullSlice3String{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{"hello"}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice3String)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][][]int64",
			Scanner: &NullSlice3Int64{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{float64(1)}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice3Int64)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][][]float64",
			Scanner: &NullSlice3Float64{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{float64(1)}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice3Float64)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][][]time.Time",
			Scanner: &NullSlice3Time{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{"2017-07-01"}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice3Time)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
		{
			GoType:  "[][][]map[string]interface{}",
			Scanner: &NullSlice3Map{},
			PrestoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}}},
			TestScanner: func(t *testing.T, s sql.Scanner) {
				v, _ := s.(*NullSlice3Map)
				if !v.Valid {
					t.Fatal("scanner failed")
				}
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			if err := tc.Scanner.Scan(nil); err != nil {
				t.Fatal(err)
			}
			if err := tc.Scanner.Scan([]interface{}{[]interface{}{nil}}); err != nil {
				t.Fatal(err)
			}
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			if err := tc.Scanner.Scan(struct{}{}); err == nil {
				t.Error("bogus data scanned with no error")
			}
			if err := tc.Scanner.Scan([]interface{}{[]interface{}{struct{}{}}}); err == nil {
				t.Error("bogus data scanned with no error")
			}
			if err := tc.Scanner.Scan([]interface{}{[]interface{}{[]interface{}{struct{}{}}}}); err == nil {
				t.Error("bogus data scanned with no error")
			}
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			if err := tc.Scanner.Scan(tc.PrestoResponseUnmarshalledSample); err != nil {
				t.Error(err)
			}
			tc.TestScanner(t, tc.Scanner)
		})
	}
}
