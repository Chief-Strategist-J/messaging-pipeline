package events

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/buger/jsonparser"
)

const currencyField = "currency"

func PurchaseEnrichment(payloadJSON []byte) ([]byte, error) {
	if !json.Valid(payloadJSON) {
		return nil, errors.New("invalid JSON")
	}
	val, dataType, _, err := jsonparser.Get(payloadJSON, currencyField)
	if err != nil || dataType != jsonparser.String {
		return payloadJSON, nil
	}
	upper := strings.ToUpper(string(val))
	updated, err := jsonparser.Set(payloadJSON, []byte(`"`+upper+`"`), currencyField)
	if err != nil {
		return nil, err
	}
	return updated, nil
}
