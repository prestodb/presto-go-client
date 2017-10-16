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
	"database/sql"
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
	if dsn != "http://foobar@localhost:8080?session_properties=query_priority%3D1" {
		t.Fatal("unexpected dsn:", dsn)
	}
}

func TestTypeConversion(t *testing.T) {
	utc, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}
	testdata := []struct {
		TypeName string
		Data     interface{}
		Want     interface{}
	}{
		{
			TypeName: "boolean",
			Data:     true,
		},
		{
			TypeName: "varchar(1)",
			Data:     "hello",
		},
		{
			TypeName: "bigint",
			Data:     float64(1),
			Want:     int64(1),
		},
		{
			TypeName: "double",
			Data:     float64(1),
		},
		{
			TypeName: "date",
			Data:     "2017-07-10",
			Want:     time.Date(2017, 7, 10, 0, 0, 0, 0, utc),
		},
		{
			TypeName: "time",
			Data:     "01:02:03.000",
			Want:     time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			TypeName: "time with time zone",
			Data:     "01:02:03.000 UTC",
			Want:     time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			TypeName: "timestamp",
			Data:     "2017-07-10 01:02:03.000",
			Want:     time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			TypeName: "timestamp with time zone",
			Data:     "2017-07-10 01:02:03.000 UTC",
			Want:     time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			TypeName: "map",
			Data:     map[string]interface{}{"hello": "world"},
		},
		{
			// arrays return data as-is for slice scanners
			TypeName: "array",
			Data:     "passthrough",
		},
	}
	for _, td := range testdata {
		t.Run(td.TypeName, func(t *testing.T) {
			tc := typeConverter{TypeName: td.TypeName}
			if _, err := tc.ConvertValue(nil); err != nil {
				t.Fatal(err)
			}
			v, err := tc.ConvertValue(td.Data)
			if err != nil {
				t.Fatal(err)
			}
			if td.Want == nil {
				td.Want = td.Data
			}
			if !reflect.DeepEqual(v, td.Want) {
				t.Fatalf("unexpected data: have %v, want %v", v, td.Want)
			}
		})
	}
}

func TestSliceTypeConversion(t *testing.T) {
	testdata := []struct {
		TypeName string
		Scanner  sql.Scanner
		Data     interface{} // data as returned from presto
		NullData interface{}
	}{
		{
			TypeName: "[]bool",
			Scanner:  &NullSliceBool{},
			Data:     []interface{}{true},
			NullData: []interface{}{nil},
		},
		{
			TypeName: "[][]bool",
			Scanner:  &NullSlice2Bool{},
			Data:     []interface{}{[]interface{}{true}},
			NullData: []interface{}{[]interface{}{nil}},
		},
		{
			TypeName: "[][][]bool",
			Scanner:  &NullSlice3Bool{},
			Data:     []interface{}{[]interface{}{[]interface{}{true}}},
			NullData: []interface{}{[]interface{}{[]interface{}{nil}}},
		},
		{
			TypeName: "[]string",
			Scanner:  &NullSliceString{},
			Data:     []interface{}{"hello"},
			NullData: []interface{}{nil},
		},
		{
			TypeName: "[][]string",
			Scanner:  &NullSlice2String{},
			Data:     []interface{}{[]interface{}{"hello"}},
			NullData: []interface{}{[]interface{}{nil}},
		},
		{
			TypeName: "[][][]string",
			Scanner:  &NullSlice3String{},
			Data:     []interface{}{[]interface{}{[]interface{}{"hello"}}},
			NullData: []interface{}{[]interface{}{[]interface{}{nil}}},
		},
		{
			TypeName: "[]int64",
			Scanner:  &NullSliceInt64{},
			Data:     []interface{}{float64(1)},
			NullData: []interface{}{nil},
		},
		{
			TypeName: "[][]int64",
			Scanner:  &NullSlice2Int64{},
			Data:     []interface{}{[]interface{}{float64(1)}},
			NullData: []interface{}{[]interface{}{nil}},
		},
		{
			TypeName: "[][][]int64",
			Scanner:  &NullSlice3Int64{},
			Data:     []interface{}{[]interface{}{[]interface{}{float64(1)}}},
			NullData: []interface{}{[]interface{}{[]interface{}{nil}}},
		},
		{
			TypeName: "[]float64",
			Scanner:  &NullSliceFloat64{},
			Data:     []interface{}{float64(1)},
			NullData: []interface{}{nil},
		},
		{
			TypeName: "[][]float64",
			Scanner:  &NullSlice2Float64{},
			Data:     []interface{}{[]interface{}{float64(1)}},
			NullData: []interface{}{[]interface{}{nil}},
		},
		{
			TypeName: "[][][]float64",
			Scanner:  &NullSlice3Float64{},
			Data:     []interface{}{[]interface{}{[]interface{}{float64(1)}}},
			NullData: []interface{}{[]interface{}{[]interface{}{nil}}},
		},
		{
			TypeName: "[]time.Time",
			Scanner:  &NullSliceTime{},
			Data:     []interface{}{"2017-07-01"},
			NullData: []interface{}{nil},
		},
		{
			TypeName: "[][]time.Time",
			Scanner:  &NullSlice2Time{},
			Data:     []interface{}{[]interface{}{"2017-07-01"}},
			NullData: []interface{}{[]interface{}{nil}},
		},
		{
			TypeName: "[][][]time.Time",
			Scanner:  &NullSlice3Time{},
			Data:     []interface{}{[]interface{}{[]interface{}{"2017-07-01"}}},
			NullData: []interface{}{[]interface{}{[]interface{}{nil}}},
		},
		{
			TypeName: "[]NullMap",
			Scanner:  &NullSliceMap{},
			Data:     []interface{}{map[string]interface{}{"hello": "world"}},
			NullData: []interface{}{nil},
		},
		{
			TypeName: "[][]NullMap",
			Scanner:  &NullSlice2Map{},
			Data:     []interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}},
			NullData: []interface{}{[]interface{}{nil}},
		},
		{
			TypeName: "[][][]NullMap",
			Scanner:  &NullSlice3Map{},
			Data:     []interface{}{[]interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}}},
			NullData: []interface{}{[]interface{}{[]interface{}{nil}}},
		},
	}
	for _, td := range testdata {
		t.Run(td.TypeName, func(t *testing.T) {
			if err := td.Scanner.Scan(td.Data); err != nil {
				t.Error(err)
			}
			if err := td.Scanner.Scan(td.NullData); err != nil {
				t.Error(err)
			}
			if err := td.Scanner.Scan(nil); err != nil {
				t.Error(err)
			}
			if err := td.Scanner.Scan("!"); err == nil {
				t.Error("unexpected scan succeeded with bad data")
			}
		})
	}
}
