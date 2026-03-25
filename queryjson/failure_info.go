package queryjson

// FailureInfo represents a query failure's type and message.
type FailureInfo struct {
	Type    *string `json:"type,omitempty" presto_query_statistics:"failure_type"`
	Message *string `json:"message,omitempty" presto_query_statistics:"failure_message"`
}

// ErrorCode represents a Presto error code with its numeric code, name, and type.
type ErrorCode struct {
	Code *int    `json:"code,omitempty" presto_query_statistics:"error_code"`
	Name *string `json:"name,omitempty" presto_query_statistics:"error_code_name"`
	Type *string `json:"type,omitempty" presto_query_statistics:"error_category"`
}
