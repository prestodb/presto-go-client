package queryjson

import (
	"encoding/json"
	"sort"
	"strings"
)

// Session represents the session context from a Presto query JSON.
type Session struct {
	TransactionId     *string                      `json:"transactionId,omitempty" presto_query_creation_info:"transaction_id" presto_query_statistics:"transaction_id"`
	Schema            *string                      `json:"schema,omitempty" presto_query_creation_info:"schema_name" presto_query_statistics:"schema_name"`
	Catalog           *string                      `json:"catalog,omitempty" presto_query_creation_info:"catalog_name" presto_query_statistics:"catalog_name"`
	SystemProperties  map[string]string            `json:"systemProperties"`
	CatalogProperties map[string]map[string]string `json:"catalogProperties"`
	User              *string                      `json:"user,omitempty" presto_query_creation_info:"user" presto_query_statistics:"user"`
	Principal         *string                      `json:"principal,omitempty" presto_query_creation_info:"principal" presto_query_statistics:"principal"`
	RemoteUserAddress *string                      `json:"remoteUserAddress,omitempty" presto_query_creation_info:"remote_client_address" presto_query_statistics:"remote_client_address"`
	Source            *string                      `json:"source,omitempty" presto_query_creation_info:"source" presto_query_statistics:"source"`
	ResourceEstimates *json.RawMessage             `json:"resourceEstimates,omitempty" presto_query_creation_info:"resource_estimates" presto_query_statistics:"resource_estimates"`
	UserAgent         *string                      `json:"userAgent,omitempty" presto_query_creation_info:"user_agent" presto_query_statistics:"user_agent"`
	ClientTags        *json.RawMessage             `json:"clientTags,omitempty" presto_query_creation_info:"client_tags" presto_query_statistics:"client_tags"`

	SessionPropertiesJson string `presto_query_creation_info:"session_properties_json" presto_query_statistics:"session_properties_json"`
}

// PrepareForInsert formats session properties into a {key=value, ...} string for database
// insertion. This uses the Presto session properties wire format (not standard JSON).
func (s *Session) PrepareForInsert() {
	// Collect all key=value pairs, then sort for deterministic output.
	pairs := make([]string, 0, len(s.SystemProperties))
	for k, v := range s.SystemProperties {
		pairs = append(pairs, k+"="+v)
	}

	// Catalog properties: sort catalogs, then sort keys within each catalog.
	catalogs := make([]string, 0, len(s.CatalogProperties))
	for catalog := range s.CatalogProperties {
		catalogs = append(catalogs, catalog)
	}
	sort.Strings(catalogs)
	for _, catalog := range catalogs {
		props := s.CatalogProperties[catalog]
		keys := make([]string, 0, len(props))
		for k := range props {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			pairs = append(pairs, catalog+"."+k+"="+props[k])
		}
	}

	// Sort system properties (appended first) together with catalog properties.
	sort.Strings(pairs)
	s.SessionPropertiesJson = "{" + strings.Join(pairs, ", ") + "}"
}

// CollectSessionProperties returns a flattened map of all session properties
// (system + catalog-scoped). Catalog-scoped properties are keyed as "catalog.key".
func (s *Session) CollectSessionProperties() map[string]any {
	sessionParams := make(map[string]any)
	if s == nil {
		return sessionParams
	}
	for k, v := range s.SystemProperties {
		sessionParams[k] = v
	}
	for catalog, catalogProps := range s.CatalogProperties {
		for k, v := range catalogProps {
			sessionParams[catalog+"."+k] = v
		}
	}
	return sessionParams
}
