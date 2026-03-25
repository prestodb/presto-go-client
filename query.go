package presto

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// requestQueryResults executes an HTTP request and processes the response as a QueryResults object.
// This is an internal helper method used by the various query execution methods.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - req: The prepared HTTP request to execute
//
// Returns:
//   - The QueryResults containing the query response
//   - The HTTP response
//   - An error if the request fails or the query returns an error
func (s *Session) requestQueryResults(ctx context.Context, req *http.Request) (*QueryResults, *http.Response, error) {
	qr := new(QueryResults)
	resp, err := s.Do(ctx, req, qr)
	if err != nil {
		return nil, resp, err
	}
	// Maintain the link to the session for the QueryResults object
	qr.session = s
	if qr.Error != nil {
		return qr, resp, qr.Error
	}
	return qr, resp, nil
}

// Query executes a SQL query on the Presto/Trino server.
// This is the primary method for executing queries and returns the initial query results.
// For large result sets, you'll need to fetch additional batches using the NextUri.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - query: The SQL query to execute
//   - opts: Optional request modifiers
//
// Returns:
//   - The initial QueryResults containing schema information and possibly the first batch of data
//   - The HTTP response
//   - An error if the request fails or the query returns an error
//
// Example:
//
//	results, _, err := session.Query(ctx, "SELECT * FROM my_table LIMIT 100")
//	if err != nil {
//	    return err
//	}
//	// Process results...
func (s *Session) Query(ctx context.Context, query string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := s.NewRequest("POST", "v1/statement", query, opts...)
	if err != nil {
		return nil, nil, err
	}

	return s.requestQueryResults(ctx, req)
}

// QueryWithPreMintedID executes a SQL query using a pre-assigned query ID.
// This is useful when you want to control the query ID for tracking or management purposes.
// If queryId is empty, this method falls back to the standard Query method.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - query: The SQL query to execute
//   - queryId: The pre-assigned query ID
//   - slug: A unique identifier for this query execution
//   - opts: Optional request modifiers
//
// Returns:
//   - The initial QueryResults containing schema information and possibly the first batch of data
//   - The HTTP response
//   - An error if the request fails or the query returns an error
//
// Example:
//
//	results, _, err := session.QueryWithPreMintedID(ctx, "SELECT * FROM my_table", "my-custom-id-123", "my-slug")
//	if err != nil {
//	    return err
//	}
//	// Process results...
func (s *Session) QueryWithPreMintedID(ctx context.Context, query, queryId, slug string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	if queryId == "" {
		return s.Query(ctx, query, opts...)
	}
	req, err := s.NewRequest("PUT",
		fmt.Sprintf("v1/statement/%s?slug=%s", url.PathEscape(queryId), url.QueryEscape(slug)), query, opts...)
	if err != nil {
		return nil, nil, err
	}

	return s.requestQueryResults(ctx, req)
}

// FetchNextBatch retrieves the next batch of results for a query.
// This method should be called when a QueryResults has a non-nil NextUri,
// indicating that more data is available.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - nextUri: The URI for the next batch of results (from QueryResults.NextUri)
//   - opts: Optional request modifiers
//
// Returns:
//   - The next batch of QueryResults
//   - The HTTP response
//   - An error if the request fails or the query returns an error
//
// Example:
//
//	// Assuming 'results' is a QueryResults with NextUri != nil
//	nextResults, _, err := session.FetchNextBatch(ctx, *results.NextUri)
//	if err != nil {
//	    return err
//	}
//	// Process next batch...
func (s *Session) FetchNextBatch(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := s.NewRequest("GET", nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return s.requestQueryResults(ctx, req)
}

// CancelQuery cancels a running query.
// This method should be called when you want to stop a query before it completes.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - nextUri: The URI for the query to cancel (from QueryResults.NextUri)
//   - opts: Optional request modifiers
//
// Returns:
//   - The final QueryResults for the canceled query
//   - The HTTP response
//   - An error if the request fails
//
// Example:
//
//	// Assuming 'results' is a QueryResults for a running query
//	_, _, err := session.CancelQuery(ctx, *results.NextUri)
//	if err != nil {
//	    return err
//	}
//	// Query has been canceled
func (s *Session) CancelQuery(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := s.NewRequest("DELETE", nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return s.requestQueryResults(ctx, req)
}

// GetQueryInfo retrieves query execution details from /v1/query/{queryId}.
// The v parameter controls how the response is handled:
//   - Pass a struct pointer (e.g., &QueryInfo{}) to decode the JSON response into it
//   - Pass an io.Writer (e.g., *os.File) to write the raw JSON response
//   - Pass nil to discard the response body
//
// Example:
//
//	// Decode into a struct
//	var info queryjson.QueryInfo
//	resp, err := session.GetQueryInfo(ctx, "20231001_123456_00001_xxxxx", &info)
//
//	// Write raw JSON to a file
//	file, _ := os.Create("query.json")
//	resp, err := session.GetQueryInfo(ctx, "20231001_123456_00001_xxxxx", file)
func (s *Session) GetQueryInfo(ctx context.Context, queryId string, v any, opts ...RequestOption) (*http.Response, error) {
	req, err := s.NewRequest("GET", "v1/query/"+url.PathEscape(queryId), nil, opts...)
	if err != nil {
		return nil, err
	}
	return s.Do(ctx, req, v)
}
