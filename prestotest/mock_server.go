package prestotest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prestodb/presto-go-client/v2"
)

// --- Data Models ---

// QueryState represents the standard life-cycle stages of a Presto/Trino query.
type QueryState string

const (
	// QueryStateQueued indicates the query is waiting for coordinator resources.
	QueryStateQueued QueryState = "QUEUED"
	// QueryStateRunning indicates the query is actively being processed by workers.
	QueryStateRunning QueryState = "RUNNING"
	// QueryStateCancelled indicates execution was terminated by the user.
	QueryStateCancelled QueryState = "CANCELLED"
	// QueryStateFinished indicates successful completion.
	QueryStateFinished QueryState = "FINISHED"
	// QueryStateFailed indicates an execution or planning error occurred.
	QueryStateFailed QueryState = "FAILED"
)

// String returns the string representation of the QueryState.
func (qs QueryState) String() string {
	return string(qs)
}

// generateMockSlug creates a random string to simulate the Presto security slug.
func generateMockSlug() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// MockQueryTemplate defines the static result set and structure for a specific SQL string.
// It acts as an immutable blueprint from which MockActiveQuery instances are created.
//
// Batching and Data Distribution:
// The mock server simulates a Presto/Trino coordinator by dividing the static 'Data'
// slice into sequential windows (batches) based on the 'DataBatches' field.
//
//  1. Pre-calculated Batch Count:
//     DataBatches is adjusted during registration in AddQuery. If DataBatches is 10
//     but there are only 3 rows, it is capped at 3 to prevent empty polls.
//
//  2. Rows Per Batch Calculation:
//     The server uses a ceiling division formula to determine the batch size:
//     rowsPerBatch = (totalRows + DataBatches - 1) / DataBatches.
//
//  3. Sequential Paging:
//     Each request (batchID > 0) returns a specific slice of data:
//     - start = (batchID - 1) * rowsPerBatch
//     - end = start + rowsPerBatch.
type MockQueryTemplate struct {
	SQL          string             // The SQL query string used for template matching.
	DataBatches  int                // The number of data splits, capped by row count.
	QueueBatches int                // The number of batches it is in queue for. It should be at least 1.
	Columns      []presto.Column    // Metadata describing the result set columns.
	Data         [][]any            // The full result set partitioned across batches.
	Error        *presto.QueryError // Optional error to simulate a query failure.
	Latency      time.Duration      // Latency for the query execution.
}

// MockActiveQuery represents a live execution instance of a template.
type MockActiveQuery struct {
	ID        string
	Template  *MockQueryTemplate
	State     QueryState
	QueuedFor int // How many batches it has stayed in the "QUEUED" state.
}

// --- Mock Server Implementation ---

// MockPrestoServer simulates a Presto/Trino coordinator for integration testing.
type MockPrestoServer struct {
	server *httptest.Server

	// templates maps SQL strings to their pre-validated MockQueryTemplate blueprints.
	templates map[string]*MockQueryTemplate

	// activeQueries maps unique execution IDs to their current MockActiveQuery state.
	activeQueries map[string]*MockActiveQuery

	queriesMutex sync.RWMutex // Protects maps during concurrent test execution.

	// defaultLatency is the default fallback query latency if no template latency is defined.
	defaultLatency time.Duration

	queryIDCounter atomic.Int64
	today          string // Cached date string for optimized ID generation.
}

// NewMockPrestoServer initializes a new mock server using the standard library.
func NewMockPrestoServer() *MockPrestoServer {
	mock := &MockPrestoServer{
		templates:     make(map[string]*MockQueryTemplate),
		activeQueries: make(map[string]*MockActiveQuery),
		today:         time.Now().Format("20060102"),
	}

	mux := http.NewServeMux()

	// POST /v1/statement: Initiates a new query with a server-generated ID.
	mux.HandleFunc("POST /v1/statement", mock.handleNewQuery)

	// PUT /v1/statement/{queryId}: Initiates a query with a client-provided ID.
	mux.HandleFunc("PUT /v1/statement/{queryId}", mock.handleQueryWithPreMintedID)

	// GET /v1/statement/{status}/{queryId}/{batchId}: Polls for the next data batch.
	mux.HandleFunc("GET /v1/statement/{status}/{queryId}/{batchId}", mock.handleFetchNextBatch)

	// DELETE /v1/statement/{status}/{queryId}/{batchId}: Cancels a running query.
	mux.HandleFunc("DELETE /v1/statement/{status}/{queryId}/{batchId}", mock.handleCancelQuery)

	// GET /v1/query/{queryId}: Returns query info.
	mux.HandleFunc("GET /v1/query/{queryId}", mock.handleQueryInfo)

	mock.server = httptest.NewServer(mux)

	return mock
}

// AddQuery registers a SQL template and pre-calculates the valid DataBatches.
func (m *MockPrestoServer) AddQuery(tmpl *MockQueryTemplate) {
	m.queriesMutex.Lock()
	defer m.queriesMutex.Unlock()

	totalRows := len(tmpl.Data)
	if tmpl.DataBatches == 0 && totalRows > 0 {
		tmpl.DataBatches = 1
	} else if totalRows < tmpl.DataBatches {
		tmpl.DataBatches = totalRows
	}
	if tmpl.QueueBatches < 1 {
		tmpl.QueueBatches = 1
	}

	m.templates[tmpl.SQL] = tmpl
}

// SetDefaultLatency configures the fallback query latency.
func (m *MockPrestoServer) SetDefaultLatency(latency time.Duration) {
	m.defaultLatency = latency
}

// --- Request Handlers ---

// handleNewQuery is a specialized caller for the internal logic using a server-generated ID.
func (m *MockPrestoServer) handleNewQuery(w http.ResponseWriter, r *http.Request) {
	m.handleQueryInternal(w, r, m.newQueryID())
}

// handleQueryWithPreMintedID initiates a query using the ID provided in the URL.
func (m *MockPrestoServer) handleQueryWithPreMintedID(w http.ResponseWriter, r *http.Request) {
	m.handleQueryInternal(w, r, r.PathValue("queryId"))
}

// handleQueryInternal manages SQL matching and MockActiveQuery instantiation.
func (m *MockPrestoServer) handleQueryInternal(w http.ResponseWriter, r *http.Request, queryID string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusInternalServerError)
		return
	}
	sql := string(body)

	m.queriesMutex.RLock()
	template, exists := m.templates[sql]
	m.queriesMutex.RUnlock()

	if !exists {
		template = &MockQueryTemplate{
			SQL:          sql,
			DataBatches:  1,
			QueueBatches: 1,
			Columns:      []presto.Column{{Name: "result", Type: "varchar"}},
			Data:         [][]any{{"Query template not found; default success"}},
		}
	}

	m.queriesMutex.Lock()
	m.activeQueries[queryID] = &MockActiveQuery{
		ID:       queryID,
		Template: template,
		State:    QueryStateQueued,
	}
	m.queriesMutex.Unlock()

	m.sendQueryResponse(r.Context(), w, queryID, 0)
}

func (m *MockPrestoServer) handleFetchNextBatch(w http.ResponseWriter, r *http.Request) {
	batchID, _ := strconv.Atoi(r.PathValue("batchId"))
	m.sendQueryResponse(r.Context(), w, r.PathValue("queryId"), batchID)
}

func (m *MockPrestoServer) handleQueryInfo(w http.ResponseWriter, r *http.Request) {
	queryID := r.PathValue("queryId")

	m.queriesMutex.RLock()
	query, exists := m.activeQueries[queryID]
	m.queriesMutex.RUnlock()

	state := "FINISHED"
	var sql string
	if exists {
		state = string(query.State)
		sql = query.Template.SQL
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queryId": queryID,
		"state":   state,
		"query":   sql,
	})
}

func (m *MockPrestoServer) handleCancelQuery(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("queryId")
	m.queriesMutex.Lock()
	if q, ok := m.activeQueries[id]; ok {
		q.State = QueryStateCancelled
	}
	m.queriesMutex.Unlock()
	m.sendQueryResponse(r.Context(), w, id, 0)
}

// --- Protocol Response Logic ---

// writeJSON encodes v as JSON and writes it to the response with the given status code.
func writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(v)
}

// sendQueryResponse prepares a JSON payload and applies hierarchical latency.
func (m *MockPrestoServer) sendQueryResponse(ctx context.Context, w http.ResponseWriter, queryID string, batchID int) {
	m.queriesMutex.RLock()
	query, exists := m.activeQueries[queryID]
	if !exists {
		m.queriesMutex.RUnlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Query not found"})
		return
	}

	totalLatency := m.defaultLatency
	if query.Template.Latency > 0 {
		totalLatency = query.Template.Latency
	}

	// Calculate total lifecycle requests to distribute latency evenly.
	dataBatchCount := query.Template.DataBatches
	queueBatchCount := query.Template.QueueBatches
	totalRequests := dataBatchCount + queueBatchCount
	if totalRequests == 0 {
		totalRequests = 1
	}

	sleepDuration := totalLatency / time.Duration(totalRequests)
	m.queriesMutex.RUnlock()

	if sleepDuration > 0 {
		t := time.NewTimer(sleepDuration)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}

	m.queriesMutex.Lock()
	query, exists = m.activeQueries[queryID]
	if !exists {
		m.queriesMutex.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Query removed during processing"})
		return
	}
	defer m.queriesMutex.Unlock()

	// Logic for managing the "Queued" phase loop.
	if batchID == 0 {
		query.QueuedFor++
	}

	// Transition to RUNNING only after exiting the queue loop.
	if query.QueuedFor >= queueBatchCount && query.State == QueryStateQueued {
		query.State = QueryStateRunning
	}

	// Determine if more batches (either queue status or data) are expected.
	hasMore := query.QueuedFor < queueBatchCount || batchID < dataBatchCount
	if !hasMore && query.State == QueryStateRunning {
		if query.Template.Error != nil {
			query.State = QueryStateFailed
		} else {
			query.State = QueryStateFinished
		}
	}

	resp := presto.QueryResults{
		Id:      queryID,
		Columns: query.Template.Columns,
		Stats: presto.StatementStats{
			State:           string(query.State),
			Scheduled:       true,
			TotalSplits:     dataBatchCount,
			CompletedSplits: batchID,
		},
	}

	if hasMore {
		nextBatch := batchID + 1
		// If still in the queue loop, keep the client polling batch 0.
		if query.QueuedFor < queueBatchCount {
			nextBatch = 0
		}
		nextUri := fmt.Sprintf("%s/v1/statement/%s/%s/%d?slug=%s",
			m.server.URL, query.State, queryID, nextBatch, generateMockSlug())
		resp.NextUri = &nextUri
	}

	// Data is delivered sequentially across DataBatches.
	if batchID > 0 && dataBatchCount > 0 && len(query.Template.Data) > 0 {
		rowsPerBatch := (len(query.Template.Data) + dataBatchCount - 1) / dataBatchCount
		start := (batchID - 1) * rowsPerBatch
		if start < len(query.Template.Data) {
			end := start + rowsPerBatch
			if end > len(query.Template.Data) {
				end = len(query.Template.Data)
			}
			batchRows := query.Template.Data[start:end]
			resp.Data = make([]json.RawMessage, len(batchRows))
			for i, row := range batchRows {
				data, marshalErr := json.Marshal(row)
				if marshalErr != nil {
					http.Error(w, fmt.Sprintf("failed to marshal row data: %v", marshalErr), http.StatusInternalServerError)
					return
				}
				resp.Data[i] = data
			}
		}
	}

	// Only include error in the final response, matching real Presto behavior
	// where errors are reported after query planning/execution completes.
	if !hasMore && query.Template.Error != nil {
		resp.Error = query.Template.Error
	}

	if query.State == QueryStateFinished || query.State == QueryStateCancelled || query.State == QueryStateFailed {
		delete(m.activeQueries, queryID)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (m *MockPrestoServer) newQueryID() string {
	return fmt.Sprintf("%s_%d", m.today, m.queryIDCounter.Add(1))
}

// URL returns the base URL of the mock server.
func (m *MockPrestoServer) URL() string { return m.server.URL }

// Close shuts down the mock server.
func (m *MockPrestoServer) Close() { m.server.Close() }
