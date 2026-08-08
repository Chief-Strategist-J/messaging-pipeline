package customprocessors

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/buger/jsonparser"
)

const currencyField = "currency"

func PurchaseEnrichment(payloadJSON string) (string, error) {
	data := []byte(payloadJSON)
	if !json.Valid(data) {
		return "", errors.New("invalid JSON")
	}
	val, dataType, _, err := jsonparser.Get(data, currencyField)
	if err != nil || dataType != jsonparser.String {
		return payloadJSON, nil
	}
	upper := strings.ToUpper(string(val))
	updated, err := jsonparser.Set(data, []byte(`"`+upper+`"`), currencyField)
	if err != nil {
		return "", err
	}
	return string(updated), nil
}
