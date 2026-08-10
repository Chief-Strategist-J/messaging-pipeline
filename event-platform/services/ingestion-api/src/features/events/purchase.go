package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/buger/jsonparser"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const currencyField = "currency"

func PurchaseEnrichment(ctx context.Context, payloadJSON []byte) ([]byte, error) {
	_, span := otel.Tracer("events").Start(ctx, "events.PurchaseEnrichment")
	defer span.End()

	if !json.Valid(payloadJSON) {
		return nil, errors.New("invalid JSON")
	}
	val, dataType, _, err := jsonparser.Get(payloadJSON, currencyField)
	if err != nil || dataType != jsonparser.String {
		return payloadJSON, nil
	}
	upper := strings.ToUpper(string(val))
	span.SetAttributes(
		attribute.String("currency.original", string(val)),
		attribute.String("currency.enriched", upper),
	)
	updated, err := jsonparser.Set(payloadJSON, []byte(`"`+upper+`"`), currencyField)
	if err != nil {
		return nil, err
	}
	return updated, nil
}
