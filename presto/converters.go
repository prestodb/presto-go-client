package presto

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type rowConverter struct {
	fields     []string
	converters []driver.ValueConverter
}

func (c *rowConverter) typeName() string {
	return "row"
}

// ConvertValue implements driver.ValueConverter interface to provide
// conversion for row column types. The resulting value will be a
// map[string]any.
func (c *rowConverter) ConvertValue(v any) (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	vs, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("presto: row converter needs []any and received %T", v)
	}
	if len(vs) != len(c.fields) {
		return nil, fmt.Errorf("presto: row converter has wrong number of elements: %d, expected: %d", len(vs), len(c.fields))
	}
	res := make(map[string]any)
	for i, f := range c.fields {
		if vs[i] == nil {
			continue
		}

		sub, err := c.converters[i].ConvertValue(vs[i])
		if err != nil {
			return nil, fmt.Errorf("presto: converting sub property of row: %w", err)
		}
		if sub != nil {
			res[f] = sub
		}
	}
	return res, nil
}

func newComplexConverter(ts typeSignature) (driver.ValueConverter, error) {
	if ts.RawType != "row" {
		return newTypeConverter(ts.RawType), nil
	}

	var c rowConverter
	// Field names.
	for _, fd := range ts.LiteralArguments {
		var fn string
		if err := json.Unmarshal(fd, &fn); err != nil {
			return nil, fmt.Errorf("presto: parsing field name for row converter: %w", err)
		}
		c.fields = append(c.fields, fn)
	}
	// Field converters.
	for _, tas := range ts.TypeArguments {
		var fts typeSignature
		if err := json.Unmarshal(tas, &fts); err != nil {
			return nil, fmt.Errorf("presto: parsing field type for row converter: %w", err)
		}
		conv, err := newComplexConverter(fts)
		if err != nil {
			return nil, fmt.Errorf("presto: creating nested converted for row converter: %w", err)
		}
		c.converters = append(c.converters, conv)
	}
	return &c, nil
}
