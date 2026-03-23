package presto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

// QueryRow represents a single row of data returned from a Presto query.
type QueryRow []any

// QueryResults represents the results of a Presto query execution.
// It contains both the query data and metadata about the query execution.
// For large result sets, data is returned in batches, with NextUri pointing to the next batch.
type QueryResults struct {
	// Id is the unique identifier for this query
	Id string `json:"id"`

	// InfoUri is a URI that can be used to get information about the query
	InfoUri string `json:"infoUri"`

	// PartialCancelUri is a URI that can be used to cancel parts of the query
	PartialCancelUri *string `json:"partialCancelUri,omitempty"`

	// NextUri is a URI that can be used to fetch the next batch of results
	// If nil, there are no more results to fetch
	NextUri *string `json:"nextUri,omitempty"`

	// Columns contains metadata about the columns in the result set
	Columns []Column `json:"columns,omitempty"`

	// Data contains the actual rows of data as JSON raw messages
	// These need to be unmarshaled into appropriate types
	Data []json.RawMessage `json:"data,omitempty"`
	// binaryData;

	// Stats contains statistics about the query execution
	Stats StatementStats `json:"stats"`

	// Error contains information about any error that occurred during execution
	Error *QueryError `json:"error,omitempty"`

	// Warnings contains any warnings generated during query execution
	Warnings []Warning `json:"warnings"`

	// UpdateType indicates the type of update performed (for INSERT, UPDATE, DELETE)
	UpdateType *string `json:"updateType,omitempty"`

	// UpdateCount indicates the number of rows affected (for INSERT, UPDATE, DELETE)
	UpdateCount *int64 `json:"updateCount,omitempty"`

	// session is a reference to the Session that created this QueryResults
	// This is used for fetching additional batches and managing state
	session *Session
}

// HasMoreBatch returns true if there are more batches of results to fetch.
// Example:
//
//	for results.HasMoreBatch() {
//	    err := results.FetchNextBatch(ctx)
//	    // Process batch...
//	}
func (qr *QueryResults) HasMoreBatch() bool {
	return qr != nil && qr.NextUri != nil
}

// FetchNextBatch retrieves the next batch of results for this query.
// It updates the current QueryResults object with the new data.
// If the context is canceled during the fetch, the query will be automatically canceled.
//
// This method handles several edge cases:
//   - If the QueryResults is nil, it returns an error
//   - If the context is canceled during the fetch, it attempts to cancel the query on the server
//     to prevent resource leaks, then returns a context cancellation error
//   - If the server returns empty batches (no Data but NextUri is present), it continues fetching
//     until it gets data or reaches the end of results
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//
// Returns:
//   - An error if the fetch fails or the query returns an error
//   - The error will include the query ID and context information when available
//
// Example:
//
//	for results.HasMoreBatch() {
//	    err := results.FetchNextBatch(ctx)
//	    if err != nil {
//	        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
//	            // Handle timeout/cancellation specially
//	            return fmt.Errorf("query processing interrupted: %w", err)
//	        }
//	        return fmt.Errorf("failed to fetch results: %w", err)
//	    }
//	    // Process the data in results.Data
//	}
func (qr *QueryResults) FetchNextBatch(ctx context.Context) error {
	if qr == nil {
		return errors.New("cannot fetch next batch: nil QueryResults")
	}
	if qr.session == nil {
		return errors.New("cannot fetch next batch: no session associated with results")
	}

	// Use a local variable to iterate so that *qr is only updated on success.
	// This prevents the caller from seeing a partially-mutated struct if
	// a context cancellation or error occurs mid-loop.
	current := qr
	for current.NextUri != nil {
		nextUri := *current.NextUri
		// Fetching through session automatically handles transaction state sync
		newQr, _, err := qr.session.FetchNextBatch(ctx, nextUri)
		if err != nil {
			if ctx.Err() != nil {
				// Use background context for cleanup to ensure it executes despite cancellation
				_, _, cancelErr := qr.session.CancelQuery(context.Background(), nextUri)
				if cancelErr != nil {
					log.Debug().Err(cancelErr).Str("query_id", qr.Id).Msg("failed to cancel query after context cancellation")
				} else {
					log.Debug().Str("query_id", qr.Id).Msg("successfully canceled query because the context was cancelled")
				}
				return fmt.Errorf("fetch next batch failed due to context cancellation for query %s: %w", qr.Id, err)
			}
			return fmt.Errorf("fetch next batch failed for query %s: %w", qr.Id, err)
		}

		current = newQr

		if len(current.Data) > 0 {
			break
		}
	}

	// Commit the final state to the caller's struct
	if current != qr {
		session := qr.session
		*qr = *current
		qr.session = session
	}
	return nil
}

// ResultBatchHandler is a function type for processing batches of query results.
type ResultBatchHandler func(qr *QueryResults) error

// Drain fetches and processes all remaining batches of results for this query.
// It clears data after each batch to optimize memory usage.
func (qr *QueryResults) Drain(ctx context.Context, handler ResultBatchHandler) error {
	if qr == nil {
		return errors.New("cannot drain results: nil QueryResults")
	}

	processBatch := func() error {
		if len(qr.Data) == 0 {
			return nil
		}
		if handler != nil {
			if err := handler(qr); err != nil {
				qr.Data = nil
				return fmt.Errorf("batch handler returned error for query %s: %w", qr.Id, err)
			}
		}
		// Aggressively clear Data to prevent memory bloat during large drains
		qr.Data = nil
		return nil
	}

	if err := processBatch(); err != nil {
		return err
	}

	for qr.HasMoreBatch() {
		if err := qr.FetchNextBatch(ctx); err != nil {
			return fmt.Errorf("drain operation failed: %w", err)
		}
		if err := processBatch(); err != nil {
			return err
		}
	}
	return nil
}
