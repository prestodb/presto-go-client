package presto

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchNextBatch_NilQueryResults(t *testing.T) {
	var qr *QueryResults
	err := qr.FetchNextBatch(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil QueryResults")
}

func TestFetchNextBatch_NoSession(t *testing.T) {
	qr := &QueryResults{}
	qr.NextUri = new("http://localhost/next")
	err := qr.FetchNextBatch(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no session associated")
}

func TestDrain_NilQueryResults(t *testing.T) {
	var qr *QueryResults
	err := qr.Drain(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil QueryResults")
}

func TestDrain_NilHandlerWithInitialData(t *testing.T) {
	qr := &QueryResults{
		Id:   "test-nil-handler",
		Data: []json.RawMessage{json.RawMessage(`[1]`), json.RawMessage(`[2]`)},
	}
	err := qr.Drain(context.Background(), nil)
	require.NoError(t, err)
	assert.Nil(t, qr.Data, "Data should be cleared even with nil handler")
}

func TestDrain_HandlerErrorOnInitialBatch(t *testing.T) {
	handlerErr := errors.New("processing failed")
	qr := &QueryResults{
		Id:   "test-123",
		Data: []json.RawMessage{json.RawMessage(`[1]`)},
	}
	err := qr.Drain(context.Background(), func(_ *QueryResults) error {
		return handlerErr
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, handlerErr)
	assert.Contains(t, err.Error(), "batch handler returned error")
	assert.Nil(t, qr.Data, "Data should be cleared on handler error")
}
