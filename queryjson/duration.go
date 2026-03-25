package queryjson

import (
	"encoding/json"
	"fmt"
	"time"

	str2duration "github.com/xhit/go-str2duration/v2"
)

// Duration wraps time.Duration with custom JSON marshaling/unmarshaling.
// It can parse durations from both float64 (milliseconds) and string formats.
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalJSON(bytes []byte) error {
	var v any
	if err := json.Unmarshal(bytes, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		// Milliseconds
		d.Duration = time.Duration(value * 1e6)
		return nil
	case string:
		var err error
		d.Duration, err = str2duration.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid duration")
	}
}
