package query_json

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareForInsert_NestedOutputStage(t *testing.T) {
	raw := `{
		"queryId": "test_nested",
		"state": "FINISHED",
		"query": "SELECT 1",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z",
			"stageGcStatistics": [{"stageId":0},{"stageId":1}]
		},
		"outputStage": {
			"stageId": "test_nested.0",
			"plan": {"jsonRepresentation": "{\"id\":\"0\"}"},
			"latestAttemptExecutionInfo": {
				"state": "FINISHED",
				"stats": {
					"totalTasks": 1,
					"gcInfo": {"stageExecutionId": 0}
				}
			},
			"subStages": [{
				"stageId": "test_nested.1",
				"plan": {"jsonRepresentation": "{\"id\":\"1\"}"},
				"latestAttemptExecutionInfo": {
					"state": "FINISHED",
					"stats": {
						"totalTasks": 2,
						"gcInfo": {"stageExecutionId": 0}
					}
				},
				"subStages": []
			}]
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Nil(t, qi.OutputStage)
	assert.Len(t, qi.FlattenedStageList, 2)
	assert.Equal(t, "0", qi.FlattenedStageList[0].StageId)
	assert.Equal(t, "1", qi.FlattenedStageList[1].StageId)
	assert.NotEmpty(t, qi.AssembledQueryPlanJson)
}

func TestPrepareForInsert_TrinoFlatStages(t *testing.T) {
	raw := `{
		"queryId": "test_flat",
		"state": "FINISHED",
		"query": "SELECT count(*) FROM orders",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z",
			"stageGcStatistics": [{"stageId":0},{"stageId":1},{"stageId":2}]
		},
		"stages": {
			"outputStageId": "test_flat.0",
			"stages": [
				{
					"stageId": "test_flat.0",
					"plan": {"jsonRepresentation": "{\"id\":\"0\",\"root\":{\"type\":\"output\"}}"},
					"stageStats": {
						"totalTasks": 1,
						"gcInfo": {"stageId": 0}
					},
					"subStages": ["test_flat.1"]
				},
				{
					"stageId": "test_flat.1",
					"plan": {"jsonRepresentation": "{\"id\":\"1\",\"root\":{\"type\":\"aggregate\"}}"},
					"stageStats": {
						"totalTasks": 4,
						"gcInfo": {"stageId": 1}
					},
					"subStages": ["test_flat.2"]
				},
				{
					"stageId": "test_flat.2",
					"plan": {"jsonRepresentation": "{\"id\":\"2\",\"root\":{\"type\":\"scan\"}}"},
					"stageStats": {
						"totalTasks": 8,
						"gcInfo": {"stageId": 2}
					},
					"subStages": []
				}
			]
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Nil(t, qi.RawStages)
	assert.Len(t, qi.FlattenedStageList, 3)
	assert.Equal(t, "0", qi.FlattenedStageList[0].StageId)
	assert.Equal(t, "1", qi.FlattenedStageList[1].StageId)
	assert.Equal(t, "2", qi.FlattenedStageList[2].StageId)

	// Verify stats were picked up from stageStats (Trino format)
	assert.Equal(t, 1, qi.FlattenedStageList[0].TrinoStats.TotalTasks)
	assert.Equal(t, 4, qi.FlattenedStageList[1].TrinoStats.TotalTasks)
	assert.Equal(t, 8, qi.FlattenedStageList[2].TrinoStats.TotalTasks)

	// Verify query plan was assembled
	assert.NotEmpty(t, qi.AssembledQueryPlanJson)
	var plan map[string]RawPlanWrapper
	require.NoError(t, json.Unmarshal([]byte(qi.AssembledQueryPlanJson), &plan))
	assert.Len(t, plan, 3)
}

func TestPrepareForInsert_NoStages(t *testing.T) {
	raw := `{
		"queryId": "test_no_stages",
		"state": "FAILED",
		"query": "SELECT bad",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z"
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Empty(t, qi.FlattenedStageList)
	assert.Equal(t, "{}", qi.AssembledQueryPlanJson)
}

func TestPrepareForInsert_DerivedStats(t *testing.T) {
	raw := `{
		"queryId": "test_stats",
		"state": "FINISHED",
		"query": "SELECT 1",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z",
			"executionTime": "2.00s",
			"totalCpuTime": "4.00s",
			"rawInputDataSize": "1000B",
			"rawInputPositions": 200
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	// 1000 bytes / 2 seconds = 500 bytes/sec
	assert.Equal(t, int64(500), qi.QueryStats.BytesPerSec)
	// 1000 bytes / 4 seconds = 250 bytes/cpu-sec
	assert.Equal(t, int64(250), qi.QueryStats.BytesPerCPUSec)
	// 200 rows / 4 seconds = 50 rows/cpu-sec
	assert.Equal(t, int64(50), qi.QueryStats.RowsPerCPUSec)
}

func TestPrepareForInsert_DerivedStats_SubSecond(t *testing.T) {
	// Sub-second durations exercise the actual bug fix: the old code divided
	// by milliseconds, producing bytes/ms instead of bytes/sec (off by 1000x).
	raw := `{
		"queryId": "test_subsecond",
		"state": "FINISHED",
		"query": "SELECT 1",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z",
			"executionTime": "0.50s",
			"totalCpuTime": "0.25s",
			"rawInputDataSize": "1000B",
			"rawInputPositions": 100
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	// 1000 bytes / 0.5 seconds = 2000 bytes/sec (old code would give 1000/500 = 2)
	assert.Equal(t, int64(2000), qi.QueryStats.BytesPerSec)
	// 1000 bytes / 0.25 seconds = 4000 bytes/cpu-sec
	assert.Equal(t, int64(4000), qi.QueryStats.BytesPerCPUSec)
	// 100 rows / 0.25 seconds = 400 rows/cpu-sec
	assert.Equal(t, int64(400), qi.QueryStats.RowsPerCPUSec)
}

func TestPrepareForInsert_Idempotent(t *testing.T) {
	raw := `{
		"queryId": "test_idempotent",
		"state": "FINISHED",
		"query": "SELECT 1",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z"
		},
		"outputStage": {
			"stageId": "test_idempotent.0",
			"plan": {"jsonRepresentation": "{\"id\":\"0\"}"},
			"latestAttemptExecutionInfo": {
				"state": "FINISHED",
				"stats": {
					"totalTasks": 1,
					"gcInfo": {"stageExecutionId": 0}
				}
			},
			"subStages": []
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Len(t, qi.FlattenedStageList, 1)
	firstResult := qi.AssembledQueryPlanJson

	// Second call should be a no-op — same results, no error
	require.NoError(t, qi.PrepareForInsert())
	assert.Len(t, qi.FlattenedStageList, 1)
	assert.Equal(t, firstResult, qi.AssembledQueryPlanJson)
}
