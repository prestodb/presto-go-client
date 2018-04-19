package presto

import "testing"

func TestSerial(t *testing.T) {
	scenarios := []struct {
		name           string
		value          interface{}
		expectedError  bool
		expectedSerial string
	}{
		{
			name:           "basic string",
			value:          "hello world",
			expectedSerial: `'hello world'`,
		},
		{
			name:           "single quoted string",
			value:          "hello world's",
			expectedSerial: `'hello world''s'`,
		},
		{
			name:           "double quoted string",
			value:          `hello "world"`,
			expectedSerial: `'hello "world"'`,
		},
		{
			name:           "int8",
			value:          int8(100),
			expectedSerial: "100",
		},
		{
			name:           "int16",
			value:          int16(100),
			expectedSerial: "100",
		},
		{
			name:           "int32",
			value:          int32(100),
			expectedSerial: "100",
		},
		{
			name:           "int",
			value:          int(100),
			expectedSerial: "100",
		},
		{
			name:           "int64",
			value:          int64(100),
			expectedSerial: "100",
		},
		{
			name:          "uint8",
			value:         uint8(100),
			expectedError: true,
		},
		{
			name:           "uint16",
			value:          uint16(100),
			expectedSerial: "100",
		},
		{
			name:           "uint32",
			value:          uint32(100),
			expectedSerial: "100",
		},
		{
			name:           "uint",
			value:          uint(100),
			expectedSerial: "100",
		},
		{
			name:           "uint64",
			value:          uint64(100),
			expectedSerial: "100",
		},
		{
			name:          "byte",
			value:         byte('a'),
			expectedError: true,
		},
		{
			name:           "valid Numeric",
			value:          Numeric("10"),
			expectedSerial: "10",
		},
		{
			name:          "invalid Numeric",
			value:         Numeric("not-a-number"),
			expectedError: true,
		},
		{
			name:           "bool true",
			value:          true,
			expectedSerial: "true",
		},
		{
			name:           "bool false",
			value:          false,
			expectedSerial: "false",
		},
		{
			name:          "nil",
			value:         nil,
			expectedError: true,
		},
		{
			name:          "slice typed nil",
			value:         []interface{}(nil),
			expectedError: true,
		},
		{
			name:           "valid slice",
			value:          []interface{}{1, 2},
			expectedSerial: "ARRAY[1, 2]",
		},
		{
			name:           "valid empty",
			value:          []interface{}{},
			expectedSerial: "ARRAY[]",
		},
		{
			name:          "invalid slice contents",
			value:         []interface{}{1, byte('a')},
			expectedError: true,
		},
	}

	for i := range scenarios {
		scenario := scenarios[i]

		t.Run(scenario.name, func(t *testing.T) {
			s, err := Serial(scenario.value)
			if err != nil {
				if scenario.expectedError {
					return
				}
				t.Fatal(err)
			}

			if scenario.expectedError {
				t.Fatal("missing an expected error")
			}

			if scenario.expectedSerial != s {
				t.Fatalf("mismatched serial, got %q expected %q", s, scenario.expectedSerial)
			}
		})
	}
}
