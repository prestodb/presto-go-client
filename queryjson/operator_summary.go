package queryjson

import (
	"encoding/json"
)

// OperatorSummary contains per-operator statistics from a query execution.
type OperatorSummary struct {
	StageId                     int              `json:"stageId" presto_query_operator_stats:"stage_id"`
	StageExecutionId            int              `json:"stageExecutionId" presto_query_operator_stats:"stage_execution_id"`
	PipelineId                  int              `json:"pipelineId" presto_query_operator_stats:"pipeline_id"`
	OperatorId                  int              `json:"operatorId" presto_query_operator_stats:"operator_id"`
	PlanNodeId                  string           `json:"planNodeId" presto_query_operator_stats:"plan_node_id"`
	OperatorType                string           `json:"operatorType" presto_query_operator_stats:"operator_type"`
	TotalDrivers                int              `json:"totalDrivers" presto_query_operator_stats:"total_drivers"`
	AddInputCalls               int              `json:"addInputCalls" presto_query_operator_stats:"add_input_calls"`
	AddInputWall                Duration         `json:"addInputWall" presto_query_operator_stats:"add_input_wall_ms"`
	AddInputCpu                 Duration         `json:"addInputCpu" presto_query_operator_stats:"add_input_cpu_ms"`
	AddInputAllocation          SISize           `json:"addInputAllocation" presto_query_operator_stats:"add_input_allocation_bytes"`
	RawInputDataSize            SISize           `json:"rawInputDataSize" presto_query_operator_stats:"raw_input_data_size_bytes"`
	RawInputPositions           int              `json:"rawInputPositions" presto_query_operator_stats:"raw_input_positions"`
	InputDataSize               SISize           `json:"inputDataSize" presto_query_operator_stats:"input_data_size_bytes"`
	InputPositions              int              `json:"inputPositions" presto_query_operator_stats:"input_positions"`
	SumSquaredInputPositions    float64          `json:"sumSquaredInputPositions" presto_query_operator_stats:"sum_squared_input_positions"`
	GetOutputCalls              int              `json:"getOutputCalls" presto_query_operator_stats:"get_output_calls"`
	GetOutputWall               Duration         `json:"getOutputWall" presto_query_operator_stats:"get_output_wall_ms"`
	GetOutputCpu                Duration         `json:"getOutputCpu" presto_query_operator_stats:"get_output_cpu_ms"`
	GetOutputAllocation         SISize           `json:"getOutputAllocation" presto_query_operator_stats:"get_output_allocation_bytes"`
	OutputDataSize              SISize           `json:"outputDataSize" presto_query_operator_stats:"output_data_size_bytes"`
	OutputPositions             int              `json:"outputPositions" presto_query_operator_stats:"output_positions"`
	PhysicalWrittenDataSize     SISize           `json:"physicalWrittenDataSize" presto_query_operator_stats:"physical_written_data_size_bytes"`
	BlockedWall                 Duration         `json:"blockedWall" presto_query_operator_stats:"blocked_wall_ms"`
	FinishCalls                 int              `json:"finishCalls" presto_query_operator_stats:"finish_calls"`
	FinishWall                  Duration         `json:"finishWall" presto_query_operator_stats:"finish_wall_ms"`
	FinishCpu                   Duration         `json:"finishCpu" presto_query_operator_stats:"finish_cpu_ms"`
	FinishAllocation            SISize           `json:"finishAllocation" presto_query_operator_stats:"finish_allocation_bytes"`
	UserMemoryReservation       SISize           `json:"userMemoryReservation" presto_query_operator_stats:"user_memory_reservation_bytes"`
	RevocableMemoryReservation  SISize           `json:"revocableMemoryReservation" presto_query_operator_stats:"revocable_memory_reservation_bytes"`
	SystemMemoryReservation     SISize           `json:"systemMemoryReservation" presto_query_operator_stats:"system_memory_reservation_bytes"`
	PeakUserMemoryReservation   SISize           `json:"peakUserMemoryReservation" presto_query_operator_stats:"peak_user_memory_reservation_bytes"`
	PeakSystemMemoryReservation SISize           `json:"peakSystemMemoryReservation" presto_query_operator_stats:"peak_system_memory_reservation_bytes"`
	PeakTotalMemoryReservation  SISize           `json:"peakTotalMemoryReservation" presto_query_operator_stats:"peak_total_memory_reservation_bytes"`
	SpilledDataSize             SISize           `json:"spilledDataSize" presto_query_operator_stats:"spilled_data_size_bytes"`
	Info                        *json.RawMessage `json:"info,omitempty" presto_query_operator_stats:"info"`
	RuntimeStats                *json.RawMessage `json:"runtimeStats" presto_query_operator_stats:"runtime_stats"`
}
