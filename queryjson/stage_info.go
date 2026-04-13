package queryjson

import (
	"encoding/json"
	"strings"
)

// StageInfo represents the execution information for a query stage.
type StageInfo struct {
	StageId                    string               `json:"stageId" presto_query_stage_stats:"stage_id"`
	LatestAttemptExecutionInfo *StageExecutionInfo  `json:"latestAttemptExecutionInfo"`
	Plan                       *StagePlan           `json:"plan"`
	TrinoStats                 *StageExecutionStats `json:"stageStats"`

	SubStages []*StageInfo `json:"subStages"`

	// StageExecutionId is populated by PrepareForInsert.
	StageExecutionId int `json:"-" presto_query_stage_stats:"stage_execution_id"`
}

// StageExecutionInfo contains execution info for a stage attempt.
type StageExecutionInfo struct {
	State string               `json:"state"`
	Stats *StageExecutionStats `json:"stats"`
}

// StageExecutionStats contains per-stage execution statistics.
type StageExecutionStats struct {
	TotalTasks              int              `json:"totalTasks" presto_query_stage_stats:"tasks"`
	TotalScheduledTime      Duration         `json:"totalScheduledTime" presto_query_stage_stats:"total_scheduled_time_ms"`
	TotalCpuTime            Duration         `json:"totalCpuTime" presto_query_stage_stats:"total_cpu_time_ms"`
	RetriedCpuTime          Duration         `json:"retriedCpuTime" presto_query_stage_stats:"retried_cpu_time_ms"`
	TotalBlockedTime        Duration         `json:"totalBlockedTime" presto_query_stage_stats:"total_blocked_time_ms"`
	RawInputDataSize        SISize           `json:"rawInputDataSize" presto_query_stage_stats:"raw_input_data_size_bytes"`
	ProcessedInputDataSize  SISize           `json:"processedInputDataSize" presto_query_stage_stats:"processed_input_data_size_bytes"`
	PhysicalWrittenDataSize SISize           `json:"physicalWrittenDataSize" presto_query_stage_stats:"physical_written_data_size_bytes"`
	GcInfoJson              *json.RawMessage `json:"gcInfo" presto_query_stage_stats:"gc_statistics"`
	GcInfo                  *StageGcInfo     `json:"-"`
}

// StagePlan contains the JSON representation of a stage's execution plan.
type StagePlan struct {
	JsonRepresentation string `json:"jsonRepresentation"`
}

// StageGcInfo contains GC information for a stage execution.
type StageGcInfo struct {
	StageExecutionId int `json:"stageExecutionId"`
}

// RawPlanWrapper wraps a raw JSON plan for assembly into the final query plan.
type RawPlanWrapper struct {
	Plan json.RawMessage `json:"plan"`
}

// processForInsert handles per-stage processing without recursion. It is called from both
// the recursive tree walk (Presto/older Trino) and the flat list iteration (newer Trino).
//
// Processing steps:
//  1. Strip the query ID prefix from the stage ID (e.g. "queryId.3" → "3")
//  2. Select the appropriate stats source (Trino uses stageStats, Presto uses latestAttemptExecutionInfo.stats)
//  3. Parse the GC info JSON to extract stageExecutionId (Presto's speculative execution attempt ID)
//  4. Append this stage to the flattened list for ORM consumption
//  5. Add this stage's plan to the assembled query plan map
func (s *StageInfo) processForInsert(flattened *[]*StageInfo, queryPlan map[string]RawPlanWrapper) error {
	// Stage IDs are formatted as "queryId.index"; we only keep the index for the database.
	if index := strings.IndexByte(s.StageId, '.'); index >= 0 && index+1 < len(s.StageId) {
		s.StageId = s.StageId[index+1:]
	}
	// Trino exposes stats directly on StageInfo as "stageStats", while Presto nests them
	// under latestAttemptExecutionInfo.stats. Try Trino first, fall back to Presto.
	stats := s.TrinoStats
	if stats == nil && s.LatestAttemptExecutionInfo != nil {
		stats = s.LatestAttemptExecutionInfo.Stats
	}
	if stats != nil && stats.GcInfoJson != nil {
		stats.GcInfo = new(StageGcInfo)
		if err := json.Unmarshal(*stats.GcInfoJson, stats.GcInfo); err != nil {
			return err
		}
		s.StageExecutionId = stats.GcInfo.StageExecutionId
	}
	*flattened = append(*flattened, s)

	if s.Plan != nil {
		queryPlan[s.StageId] = RawPlanWrapper{
			Plan: json.RawMessage(s.Plan.JsonRepresentation),
		}
	}
	return nil
}

// PrepareForInsert recursively walks the nested stage tree (Presto/older Trino format),
// calling processForInsert on each node to flatten it into the list.
func (s *StageInfo) PrepareForInsert(flattened *[]*StageInfo, queryPlan map[string]RawPlanWrapper) error {
	if s == nil {
		return nil
	}
	if err := s.processForInsert(flattened, queryPlan); err != nil {
		return err
	}
	for _, child := range s.SubStages {
		if err := child.PrepareForInsert(flattened, queryPlan); err != nil {
			return err
		}
	}
	s.SubStages = nil
	return nil
}

// unmarshalFlatStage unmarshals a single stage from the newer Trino flat format.
// In this format, subStages is an array of stage ID strings (e.g. ["queryId.1", "queryId.2"])
// rather than nested StageInfo objects. We use a type alias with a json.RawMessage override
// for subStages so the unmarshal succeeds regardless of the subStages element type.
// The subStages references are not needed because the stages are already provided as a flat list.
func unmarshalFlatStage(data json.RawMessage) (*StageInfo, error) {
	// stageAlias avoids infinite recursion if StageInfo had a custom UnmarshalJSON.
	type stageAlias StageInfo
	aux := &struct {
		*stageAlias
		// Shadow StageInfo.SubStages to absorb it as raw JSON instead of []*StageInfo.
		SubStages json.RawMessage `json:"subStages"`
	}{
		stageAlias: &stageAlias{},
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return nil, err
	}
	return (*StageInfo)(aux.stageAlias), nil
}
