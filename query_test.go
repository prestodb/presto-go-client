package presto_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prestodb/presto-go-client/v2"
	"github.com/prestodb/presto-go-client/v2/prestotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Server Logic Tests ---

// TestMockServer_BatchCapping verifies that AddQuery correctly caps DataBatches based on row count.
func TestMockServer_BatchCapping(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	// Case 1: Sparse data (3 rows, requested 10 batches)
	tmpl := &prestotest.MockQueryTemplate{
		SQL:         "SELECT * FROM sparse",
		Data:        [][]any{{1}, {2}, {3}},
		DataBatches: 10,
	}
	mockServer.AddQuery(tmpl)
	assert.Equal(t, 3, tmpl.DataBatches, "DataBatches should be capped at row count")

	// Case 2: DataBatches defaults to 1 when data exists but DataBatches is 0
	tmplDefault := &prestotest.MockQueryTemplate{
		SQL:  "SELECT * FROM defaulted",
		Data: [][]any{{1}, {2}},
	}
	mockServer.AddQuery(tmplDefault)
	assert.Equal(t, 1, tmplDefault.DataBatches, "DataBatches should default to 1 when data exists")

	// Case 3: Zero data
	tmplEmpty := &prestotest.MockQueryTemplate{
		SQL:         "SELECT * FROM empty",
		Data:        [][]any{},
		DataBatches: 5,
	}
	mockServer.AddQuery(tmplEmpty)
	assert.Equal(t, 0, tmplEmpty.DataBatches, "DataBatches should be 0 for empty data")
}

// TestMockServer_DistributedLatency verifies the (latency / batches + 1) logic.
func TestMockServer_DistributedLatency(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	// Setup: 200ms total latency, 1 data batch (Total 2 requests: initial + batch 1)
	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 1",
		Data:        [][]any{{1}},
		Latency:     200 * time.Millisecond,
		DataBatches: 1,
	})

	start := time.Now()
	results, _, err := session.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)

	// First request (Initial POST) should have slept for ~100ms
	duration := time.Since(start)
	assert.True(t, duration >= 90*time.Millisecond, "Initial request should incur proportional latency")

	startBatch := time.Now()
	err = results.FetchNextBatch(context.Background())
	require.NoError(t, err)

	// Second request (Batch 1 GET) should have slept for remaining ~100ms
	batchDuration := time.Since(startBatch)
	assert.True(t, batchDuration >= 90*time.Millisecond, "Batch request should incur proportional latency")
}

// --- QueryResults Logic Tests ---

// TestQueryResults_DrainHandlerErrorOnSubsequentBatch verifies that a handler error on
// the second fetched batch is propagated correctly and Data is cleared.
func TestQueryResults_DrainHandlerErrorOnSubsequentBatch(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT * FROM two_batches",
		Data:        [][]any{{1}, {2}},
		DataBatches: 2,
	})

	results, _, err := session.Query(context.Background(), "SELECT * FROM two_batches")
	require.NoError(t, err)

	handlerErr := errors.New("processing failed on second batch")
	callCount := 0
	err = results.Drain(context.Background(), func(qr *presto.QueryResults) error {
		callCount++
		if callCount == 1 {
			return nil
		}
		return handlerErr
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, handlerErr)
	assert.Contains(t, err.Error(), "batch handler returned error")
	assert.Equal(t, 2, callCount, "handler should be called for both batches before error stops iteration")
	assert.Nil(t, results.Data, "Data should be cleared on handler error from subsequent batch")
}

// TestQueryResults_DrainSuccess verifies that Drain correctly processes all data and clears memory.
func TestQueryResults_DrainSuccess(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	data := [][]any{{1}, {2}, {3}, {4}, {5}}
	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT * FROM drain",
		Data:        data,
		DataBatches: 3,
	})

	results, _, err := session.Query(context.Background(), "SELECT * FROM drain")
	require.NoError(t, err)

	rowCount := 0
	err = results.Drain(context.Background(), func(qr *presto.QueryResults) error {
		rowCount += len(qr.Data)
		// Verify memory optimization: Data should exist during handler
		assert.NotEmpty(t, qr.Data)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 5, rowCount)
	assert.Empty(t, results.Data, "Data should be cleared after Drain completes")
}

func TestQueryResults_DrainProcessesCurrentBatch(t *testing.T) {
	results := &presto.QueryResults{
		Id:   "query-with-initial-batch",
		Data: []json.RawMessage{json.RawMessage(`[1]`), json.RawMessage(`[2]`)},
	}

	rowCount := 0
	err := results.Drain(context.Background(), func(qr *presto.QueryResults) error {
		rowCount += len(qr.Data)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, rowCount, "Drain should process rows already present in the initial response")
	assert.Nil(t, results.Data, "Data should be cleared after Drain completes")
}

// TestQueryResults_DrainHandlerError verifies Drain stops and returns error when handler fails.
func TestQueryResults_DrainHandlerError(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT * FROM fail_drain",
		Data:        [][]any{{1}, {2}, {3}},
		DataBatches: 2,
	})

	results, _, queryErr := session.Query(context.Background(), "SELECT * FROM fail_drain")
	require.NoError(t, queryErr)

	handlerErr := errors.New("handler failed")
	err = results.Drain(context.Background(), func(qr *presto.QueryResults) error {
		return handlerErr
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, handlerErr)
	assert.Contains(t, err.Error(), "batch handler returned error")
	assert.Nil(t, results.Data, "Data should be cleared on handler error")
}

// TestQueryResults_ContextCancellation verifies server-side cleanup on client timeout.
func TestQueryResults_ContextCancellation(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	// Set a high latency to trigger timeout
	mockServer.SetDefaultLatency(1 * time.Second)
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT * FROM slow",
		Data:        [][]any{{1}, {2}, {3}, {4}, {5}},
		DataBatches: 2,
	})

	// Initial query succeeds (batch 0)
	results, _, _ := session.Query(context.Background(), "SELECT * FROM slow")

	// Create a context that will time out during the next fetch
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = results.FetchNextBatch(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

// TestQueryResults_EmptyBatches verifies the skipping logic in FetchNextBatch.
func TestQueryResults_EmptyBatches(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	// We simulate a query that stays in QUEUED for 2 polls before delivering 1 batch of data.
	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:          "SELECT * FROM skip",
		Data:         [][]any{{1}},
		QueueBatches: 2, // The client will poll batch 0 twice before getting batch 1.
		DataBatches:  1,
	})

	results, _, _ := session.Query(context.Background(), "SELECT * FROM skip")
	assert.True(t, results.HasMoreBatch())
	assert.Equal(t, string(prestotest.QueryStateQueued), results.Stats.State)

	// FetchNextBatch should loop through the 2 empty queued polls and then
	// return as soon as it hits the first data batch (Batch 1).
	err = results.FetchNextBatch(context.Background())
	require.NoError(t, err)

	assert.Len(t, results.Data, 1, "Should have eventually fetched the data")
	assert.Equal(t, string(prestotest.QueryStateFinished), results.Stats.State)
}

func TestQueryWithPreMintedID(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         "SELECT 1",
		Columns:     []presto.Column{{Name: "result", Type: "integer"}},
		Data:        [][]any{{1}},
		DataBatches: 1,
	})

	t.Run("With pre-minted ID", func(t *testing.T) {
		results, _, err := session.QueryWithPreMintedID(
			context.Background(), "SELECT 1", "my-query-id", "my-slug")
		require.NoError(t, err)
		assert.Equal(t, "my-query-id", results.Id)
	})

	t.Run("Empty ID falls back to Query", func(t *testing.T) {
		results, _, err := session.QueryWithPreMintedID(
			context.Background(), "SELECT 1", "", "ignored-slug")
		require.NoError(t, err)
		assert.NotEmpty(t, results.Id)
	})

	t.Run("Special characters are escaped", func(t *testing.T) {
		// If escaping is broken, the URL would be malformed and the request would fail
		results, _, err := session.QueryWithPreMintedID(
			context.Background(), "SELECT 1", "id with spaces", "slug&param=val")
		require.NoError(t, err)
		assert.NotEmpty(t, results.Id)
	})
}

// TestQueryResults_ConcurrentAccess verifies session mutex protection.
func TestQueryResults_ConcurrentAccess(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{SQL: "SELECT 1", DataBatches: 1})

	var wg sync.WaitGroup
	// Run 10 concurrent fetches using the same session reference
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = session.Query(context.Background(), "SELECT 1")
		}()
	}
	wg.Wait()
}

// TestQuery_SetSessionFromResponse verifies that X-Presto-Set-Session response headers
// update session properties for subsequent requests.
func TestQuery_SetSessionFromResponse(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SET SESSION optimize_hash_generation = true",
		Columns: []presto.Column{{Name: "result", Type: "boolean"}},
		Data:    [][]any{{true}},
		SetSessionProperties: map[string]string{
			"optimize_hash_generation": "true",
		},
	})

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SELECT 1",
		Columns: []presto.Column{{Name: "_col0", Type: "integer"}},
		Data:    [][]any{{1}},
	})

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession()

	// Execute SET SESSION — should update session params
	_, _, err = session.Query(context.Background(), "SET SESSION optimize_hash_generation = true")
	require.NoError(t, err)

	assert.Equal(t, "optimize_hash_generation=true", session.GetSessionParams())

	// Verify the session property is sent on subsequent queries
	_, _, err = session.Query(context.Background(), "SELECT 1")
	require.NoError(t, err)
}

// TestQuery_ClearSessionFromResponse verifies that X-Presto-Clear-Session response headers
// remove session properties.
func TestQuery_ClearSessionFromResponse(t *testing.T) {
	mockServer := prestotest.NewMockPrestoServer()
	defer mockServer.Close()

	mockServer.AddQuery(&prestotest.MockQueryTemplate{
		SQL:                    "RESET SESSION optimize_hash_generation",
		Columns:                []presto.Column{{Name: "result", Type: "boolean"}},
		Data:                   [][]any{{true}},
		ClearSessionProperties: []string{"optimize_hash_generation"},
	})

	client, err := presto.NewClient(mockServer.URL(), "")
	require.NoError(t, err)
	session := client.NewSession().SessionParam("optimize_hash_generation", "true")

	assert.Equal(t, "optimize_hash_generation=true", session.GetSessionParams())

	// Execute RESET SESSION — should clear the param
	_, _, err = session.Query(context.Background(), "RESET SESSION optimize_hash_generation")
	require.NoError(t, err)

	assert.Equal(t, "", session.GetSessionParams())
}
