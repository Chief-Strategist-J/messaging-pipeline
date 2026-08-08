package benchmark

import (
	"testing"

	"event-platform/ingestion-api/internal/customprocessors"
)

func BenchmarkPurchaseEnrichment_500KB(b *testing.B) {
	padding := make([]byte, 500*1024)
	for i := range padding {
		padding[i] = 'x'
	}
	payload := `{"amount_cents":100,"currency":"usd","data":"` + string(padding) + `"}`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = customprocessors.PurchaseEnrichment(payload)
	}
}
