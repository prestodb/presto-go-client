package presto_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prestodb/presto-go-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetClusterInfo(t *testing.T) {
	expected := &presto.ClusterStats{
		RunningQueries: 5,
		BlockedQueries: 2,
		QueuedQueries:  3,
		ActiveWorkers:  10,
		RunningDrivers: 50,
		RunningTasks:   20,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/cluster", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	c, err := presto.NewClient(srv.URL)
	require.NoError(t, err)
	s := c.NewSession()

	stats, resp, err := s.GetClusterInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 5, stats.RunningQueries)
	assert.Equal(t, 10, stats.ActiveWorkers)
}
