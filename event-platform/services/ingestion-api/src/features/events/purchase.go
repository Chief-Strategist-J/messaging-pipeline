package events

import (
	"context"
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

	val, dataType, _, err := jsonparser.Get(payloadJSON, currencyField)
	if err != nil {
		if errors.Is(err, jsonparser.KeyPathNotFoundError) {
			return payloadJSON, nil
		}
		return nil, errors.New("invalid JSON")
	}
	if dataType != jsonparser.String {
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
