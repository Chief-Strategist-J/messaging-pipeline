package eventtypes

import (
	"fmt"

	"github.com/buger/jsonparser"
)

func ValidatePayload(cfg EventTypeConfig, payloadJSON string) error {
	if len(cfg.PayloadRules) == 0 {
		return nil
	}
	data := []byte(payloadJSON)
	for _, rule := range cfg.PayloadRules {
		val, dataType, _, err := jsonparser.Get(data, rule.Field)
		if rule.Required && (err != nil || dataType == jsonparser.NotExist) {
			return fmt.Errorf("payload.%s is required", rule.Field)
		}
		if err != nil || dataType == jsonparser.NotExist {
			continue
		}
		if dataType == jsonparser.String && rule.MaxLength > 0 && len(val) > rule.MaxLength {
			return fmt.Errorf("payload.%s exceeds max length %d", rule.Field, rule.MaxLength)
		}
	}
	return nil
}
