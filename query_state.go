package presto

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// QueryStateInfo represents the state of a running or recently completed query
// as returned by the /v1/queryState endpoint.
type QueryStateInfo struct {
	QueryId    string          `json:"queryId"`
	QueryState string          `json:"queryState"`
	CreateTime time.Time       `json:"createTime"`
	ErrorCode  QueryStateError `json:"errorCode"`
}

// QueryStateError represents an error associated with a query state entry.
type QueryStateError struct {
	Code      int    `json:"code"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Retriable bool   `json:"retriable"`
}

// GetQueryStateOptions includes parameters for the /v1/queryState endpoint.
// See https://github.com/prestodb/presto/blob/master/presto-main/src/main/java/com/facebook/presto/server/QueryStateInfoResource.java
type GetQueryStateOptions struct {
	User                         *string `query:"user"`
	IncludeLocalQueryOnly        *bool   `query:"includeLocalQueryOnly"`
	IncludeAllQueries            *bool   `query:"includeAllQueries"`
	IncludeAllQueryProgressStats *bool   `query:"includeAllQueryProgressStats"`
	ExcludeResourceGroupPathInfo *bool   `query:"excludeResourceGroupPathInfo"`
	QueryTextSizeLimit           *int    `query:"queryTextSizeLimit"`
}

// GenerateHTTPQueryParameter converts a struct with `query` tags into a URL query string.
// Nil pointer fields are skipped. Returns an empty string for non-struct input.
func GenerateHTTPQueryParameter(v any) string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return ""
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return ""
	}
	queryBuilder := strings.Builder{}
	vt := rv.Type()
	for i := range vt.NumField() {
		fv, ft := rv.Field(i), vt.Field(i)
		// Dereference pointers; skip nil
		for fv.Kind() == reflect.Pointer || fv.Kind() == reflect.Interface {
			if fv.IsNil() {
				break
			}
			fv = fv.Elem()
		}
		if !fv.IsValid() || !fv.CanInterface() {
			continue
		}
		// Skip nil pointer fields
		if rv.Field(i).Kind() == reflect.Pointer && rv.Field(i).IsNil() {
			continue
		}
		if tag := ft.Tag.Get("query"); tag != "" {
			if queryBuilder.Len() > 0 {
				queryBuilder.WriteString("&")
			}
			queryBuilder.WriteString(fmt.Sprintf("%s=%s", url.QueryEscape(tag), url.QueryEscape(fmt.Sprint(fv.Interface()))))
		}
	}
	return queryBuilder.String()
}

// GetQueryState retrieves the state of queries from the /v1/queryState endpoint.
func (s *Session) GetQueryState(ctx context.Context, reqOpt *GetQueryStateOptions, opts ...RequestOption) ([]QueryStateInfo, *http.Response, error) {
	urlStr := "v1/queryState"
	if reqOpt != nil {
		if params := GenerateHTTPQueryParameter(reqOpt); params != "" {
			urlStr = fmt.Sprintf("v1/queryState?%s", params)
		}
	}
	req, err := s.NewRequest("GET", urlStr, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	infoArray := make([]QueryStateInfo, 0, 16)
	resp, err := s.Do(ctx, req, &infoArray)
	if err != nil {
		return nil, resp, err
	}
	return infoArray, resp, nil
}
