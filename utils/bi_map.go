package utils

// BiMap is a bidirectional map that allows lookups in both directions.
// It maintains two internal maps to provide efficient lookups by either key or value.
// Both key and value types must be comparable.
// BiMap is supposed to be immutable, it does not provide methods to update its content.
type BiMap[K comparable, V comparable] struct {
	a map[K]V // Forward mapping (key -> value)
	b map[V]K // Reverse mapping (value -> key)
}

// NewBiMap creates a new bidirectional map from the provided input map.
// It builds the reverse mapping automatically.
// Note: If the input map contains duplicate values, the reverse mapping will
// only contain an arbitrary key associated with that value (Go map iteration
// order is non-deterministic).
//
// Parameters:
//   - input: The initial key-value mapping
//
// Returns:
//   - A new BiMap with both forward and reverse mappings
func NewBiMap[K comparable, V comparable](input map[K]V) *BiMap[K, V] {
	// Create new internal maps to ensure immutability via defensive copying
	a := make(map[K]V, len(input))
	b := make(map[V]K, len(input))

	for k, v := range input {
		a[k] = v
		b[v] = k
	}

	return &BiMap[K, V]{
		a: a,
		b: b,
	}
}

// Lookup finds a value by its key in the forward mapping.
//
// Parameters:
//   - key: The key to look up
//
// Returns:
//   - The corresponding value
//   - A boolean indicating whether the key was found
func (m *BiMap[K, V]) Lookup(key K) (V, bool) {
	value, ok := m.a[key]
	return value, ok
}

// DirectLookup finds a value by its key without checking if the key exists.
// If the key doesn't exist, it returns the zero value for type V.
//
// Parameters:
//   - key: The key to look up
//
// Returns:
//   - The corresponding value, or zero value if not found
func (m *BiMap[K, V]) DirectLookup(key K) V {
	return m.a[key]
}

// RLookup finds a key by its value in the reverse mapping.
//
// Parameters:
//   - value: The value to look up
//
// Returns:
//   - The corresponding key
//   - A boolean indicating whether the value was found
func (m *BiMap[K, V]) RLookup(value V) (K, bool) {
	key, ok := m.b[value]
	return key, ok
}

// DirectRLookup finds a key by its value without checking if the value exists.
// If the value doesn't exist, it returns the zero value for type K.
//
// Parameters:
//   - value: The value to look up
//
// Returns:
//   - The corresponding key, or zero value if not found
func (m *BiMap[K, V]) DirectRLookup(value V) K {
	return m.b[value]
}
