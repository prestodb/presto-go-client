package presto_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prestodb/presto-go-client/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetQueryState(t *testing.T) {
	createTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	expected := []presto.QueryStateInfo{
		{
			QueryId:    "q1",
			QueryState: "RUNNING",
			CreateTime: createTime,
		},
		{
			QueryId:    "q2",
			QueryState: "QUEUED",
			CreateTime: createTime,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/queryState", r.URL.Path)
		assert.Equal(t, "true", r.URL.Query().Get("includeAllQueries"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	c, err := presto.NewClient(srv.URL)
	require.NoError(t, err)
	s := c.NewSession()

	trueVal := true
	states, resp, err := s.GetQueryState(context.Background(), &presto.GetQueryStateOptions{
		IncludeAllQueries: &trueVal,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, states, 2)
	assert.Equal(t, "q1", states[0].QueryId)
	assert.Equal(t, "RUNNING", states[0].QueryState)
}

func TestGetQueryState_NilOptions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/queryState", r.URL.Path)
		assert.Empty(t, r.URL.RawQuery)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	c, _ := presto.NewClient(srv.URL)
	s := c.NewSession()
	states, _, err := s.GetQueryState(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, states)
}

func TestGenerateHTTPQueryParameter(t *testing.T) {
	t.Run("All fields set", func(t *testing.T) {
		trueVal := true
		limit := 100
		user := "admin"
		opts := presto.GetQueryStateOptions{
			User:               &user,
			IncludeAllQueries:  &trueVal,
			QueryTextSizeLimit: &limit,
		}
		result := presto.GenerateHTTPQueryParameter(&opts)
		assert.Contains(t, result, "user=admin")
		assert.Contains(t, result, "includeAllQueries=true")
		assert.Contains(t, result, "queryTextSizeLimit=100")
	})

	t.Run("Nil pointer fields skipped", func(t *testing.T) {
		trueVal := true
		opts := presto.GetQueryStateOptions{
			IncludeAllQueries: &trueVal,
		}
		result := presto.GenerateHTTPQueryParameter(&opts)
		assert.Equal(t, "includeAllQueries=true", result)
	})

	t.Run("Nil input", func(t *testing.T) {
		result := presto.GenerateHTTPQueryParameter((*presto.GetQueryStateOptions)(nil))
		assert.Empty(t, result)
	})

	t.Run("Non-struct input", func(t *testing.T) {
		result := presto.GenerateHTTPQueryParameter("not a struct")
		assert.Empty(t, result)
	})
}

func TestGetQueryInfo(t *testing.T) {
	queryId := "20240101_120000_00001_xxxxx"
	expectedBody := map[string]string{
		"queryId": queryId,
		"state":   "FINISHED",
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/query/"+queryId, r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expectedBody)
	}))
	defer srv.Close()

	c, _ := presto.NewClient(srv.URL)
	s := c.NewSession()

	var result map[string]string
	resp, err := s.GetQueryInfo(context.Background(), queryId, &result)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, queryId, result["queryId"])
	assert.Equal(t, "FINISHED", result["state"])
}
