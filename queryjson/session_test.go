package queryjson

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSessionPrepareForInsert_Empty(t *testing.T) {
	s := &Session{}
	s.PrepareForInsert()
	assert.Equal(t, "{}", s.SessionPropertiesJSON)
}

func TestSessionPrepareForInsert_SystemProperties(t *testing.T) {
	s := &Session{
		SystemProperties: map[string]string{
			"optimize_hash_generation": "true",
		},
	}
	s.PrepareForInsert()
	assert.Equal(t, "{optimize_hash_generation=true}", s.SessionPropertiesJSON)
}

func TestSessionPrepareForInsert_CatalogProperties(t *testing.T) {
	s := &Session{
		CatalogProperties: map[string]map[string]string{
			"hive": {"orc_compression": "ZSTD"},
		},
	}
	s.PrepareForInsert()
	assert.Equal(t, "{hive.orc_compression=ZSTD}", s.SessionPropertiesJSON)
}

func TestSessionPrepareForInsert_Mixed(t *testing.T) {
	s := &Session{
		SystemProperties: map[string]string{
			"query_max_memory": "1GB",
		},
		CatalogProperties: map[string]map[string]string{
			"hive": {"orc_compression": "ZSTD"},
		},
	}
	s.PrepareForInsert()

	// Output is now sorted deterministically
	assert.Equal(t, "{hive.orc_compression=ZSTD, query_max_memory=1GB}", s.SessionPropertiesJSON)
}

func TestSessionPrepareForInsert_Deterministic(t *testing.T) {
	s := &Session{
		SystemProperties: map[string]string{
			"query_max_memory":         "1GB",
			"optimize_hash_generation": "true",
			"task_concurrency":         "8",
		},
		CatalogProperties: map[string]map[string]string{
			"hive":  {"orc_compression": "ZSTD", "bucket_execution": "true"},
			"delta": {"vacuum_min_retention": "7d"},
		},
	}

	// Run multiple times to verify determinism (map iteration is randomized).
	expected := "{delta.vacuum_min_retention=7d, hive.bucket_execution=true, hive.orc_compression=ZSTD, optimize_hash_generation=true, query_max_memory=1GB, task_concurrency=8}"
	for i := 0; i < 20; i++ {
		s.SessionPropertiesJSON = "" // reset
		s.PrepareForInsert()
		assert.Equal(t, expected, s.SessionPropertiesJSON, "iteration %d", i)
	}
}

func TestSessionPrepareForInsert_NilReceiver(t *testing.T) {
	// Verify that calling PrepareForInsert on a nil *Session does not panic,
	// matching the nil-safety of CollectSessionProperties.
	var s *Session
	assert.NotPanics(t, func() {
		s.PrepareForInsert()
	})
}
