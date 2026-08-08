package customprocessors

import "encoding/json"

const currencyField = "currency"

func PurchaseEnrichment(payloadJSON string) (string, error) {
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &fields); err != nil {
		return "", err
	}
	c, ok := fields[currencyField].(string)
	if ok {
		fields[currencyField] = toUpper(c)
	}
	out, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func toUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}
