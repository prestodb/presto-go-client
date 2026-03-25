package query_json

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSessionPrepareForInsert_Empty(t *testing.T) {
	s := &Session{}
	s.PrepareForInsert()
	assert.Equal(t, "{}", s.SessionPropertiesJson)
}

func TestSessionPrepareForInsert_SystemProperties(t *testing.T) {
	s := &Session{
		SystemProperties: map[string]string{
			"optimize_hash_generation": "true",
		},
	}
	s.PrepareForInsert()
	assert.Equal(t, "{optimize_hash_generation=true}", s.SessionPropertiesJson)
}

func TestSessionPrepareForInsert_CatalogProperties(t *testing.T) {
	s := &Session{
		CatalogProperties: map[string]map[string]string{
			"hive": {"orc_compression": "ZSTD"},
		},
	}
	s.PrepareForInsert()
	assert.Equal(t, "{hive.orc_compression=ZSTD}", s.SessionPropertiesJson)
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

	// Map iteration order is non-deterministic, so accept either ordering
	expected1 := "{query_max_memory=1GB, hive.orc_compression=ZSTD}"
	expected2 := "{hive.orc_compression=ZSTD, query_max_memory=1GB}"
	assert.True(t, s.SessionPropertiesJson == expected1 || s.SessionPropertiesJson == expected2,
		"got %s", s.SessionPropertiesJson)
}
