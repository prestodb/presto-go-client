package presto

import (
	"fmt"
	"github.com/prestodb/presto-go-client/utils"
	"strconv"
)

// RuntimeUnit represents the unit of measurement for a runtime metric.
type RuntimeUnit int8

const (
	// RuntimeUnitNone represents no specific unit
	RuntimeUnitNone RuntimeUnit = iota
	// RuntimeUnitNano represents nanoseconds
	RuntimeUnitNano
	// RuntimeUnitByte represents bytes
	RuntimeUnitByte
)

var runtimeUnitMap = utils.NewBiMap(map[RuntimeUnit]string{
	RuntimeUnitNone: "NONE",
	RuntimeUnitNano: "NANO",
	RuntimeUnitByte: "BYTE",
})

// String returns the string representation of the RuntimeUnit.
// For unknown values, it returns the numeric representation.
func (u RuntimeUnit) String() string {
	if value, ok := runtimeUnitMap.Lookup(u); ok {
		return value
	}
	return strconv.Itoa(int(u))
}

// ParseRuntimeUnit parses a string into a RuntimeUnit.
// If the string is unknown, it defaults to RuntimeUnitNone and returns an error.
func ParseRuntimeUnit(str string) (RuntimeUnit, error) {
	if key, ok := runtimeUnitMap.RLookup(str); ok {
		return key, nil
	}
	return RuntimeUnitNone, fmt.Errorf("unknown RuntimeUnit string: %s", str)
}

// MarshalText implements the encoding.TextMarshaler interface.
func (u RuntimeUnit) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (u *RuntimeUnit) UnmarshalText(text []byte) error {
	var err error
	*u, err = ParseRuntimeUnit(string(text))
	return err
}
