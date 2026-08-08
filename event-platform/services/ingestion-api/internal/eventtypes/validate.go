package eventtypes

import (
	"encoding/json"
	"fmt"
)

func ValidatePayload(cfg EventTypeConfig, payloadJSON string) error {
	if len(cfg.PayloadRules) == 0 {
		return nil
	}
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &fields); err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	for _, rule := range cfg.PayloadRules {
		v, present := fields[rule.Field]
		if rule.Required && !present {
			return fmt.Errorf("payload.%s is required", rule.Field)
		}
		if !present {
			continue
		}
		s, ok := v.(string)
		if ok && rule.MaxLength > 0 && len(s) > rule.MaxLength {
			return fmt.Errorf("payload.%s exceeds max length %d", rule.Field, rule.MaxLength)
		}
	}
	return nil
}
