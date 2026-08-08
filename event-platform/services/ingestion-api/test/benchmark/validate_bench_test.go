package benchmark

import (
	"testing"

	"event-platform/ingestion-api/internal/eventtypes"
)

var benchCfgNoRules = eventtypes.EventTypeConfig{
	Name:  "heartbeat",
	Topic: "events.raw",
}

var benchCfgWithRules = eventtypes.EventTypeConfig{
	Name:  "page_view",
	Topic: "events.raw",
	PayloadRules: []eventtypes.FieldRule{
		{Field: "url", Required: true, MaxLength: 2048},
		{Field: "referrer", Required: false, MaxLength: 2048},
	},
}

const benchPayload = `{"url":"https://example.com/page","referrer":"https://google.com"}`

func BenchmarkValidatePayloadNoRules(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eventtypes.ValidatePayload(benchCfgNoRules, "any-string")
	}
}

func BenchmarkValidatePayloadWithRules(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eventtypes.ValidatePayload(benchCfgWithRules, benchPayload)
	}
}

func BenchmarkValidatePayloadLargePayload(b *testing.B) {
	largeURL := make([]byte, 1024)
	for i := range largeURL {
		largeURL[i] = 'a'
	}
	payload := `{"url":"` + string(largeURL) + `"}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eventtypes.ValidatePayload(benchCfgWithRules, payload)
	}
}
