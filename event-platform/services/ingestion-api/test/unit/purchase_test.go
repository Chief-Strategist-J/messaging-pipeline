package unit

import (
	"testing"

	"event-platform/ingestion-api/internal/customprocessors"
)

func TestPurchaseEnrichment(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSub  string
		wantErr  bool
	}{
		{
			name:    "lowercase currency gets uppercased",
			input:   `{"amount_cents":100,"currency":"usd"}`,
			wantSub: `"currency":"USD"`,
			wantErr: false,
		},
		{
			name:    "uppercase currency stays unchanged",
			input:   `{"amount_cents":200,"currency":"EUR"}`,
			wantSub: `"currency":"EUR"`,
			wantErr: false,
		},
		{
			name:    "mixed case currency gets uppercased",
			input:   `{"amount_cents":300,"currency":"gBp"}`,
			wantSub: `"currency":"GBP"`,
			wantErr: false,
		},
		{
			name:    "missing currency field is handled",
			input:   `{"amount_cents":100}`,
			wantSub: `"amount_cents"`,
			wantErr: false,
		},
		{
			name:    "invalid json returns error",
			input:   `{broken`,
			wantErr: true,
		},
		{
			name:    "empty json object",
			input:   `{}`,
			wantSub: "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := customprocessors.PurchaseEnrichment(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("PurchaseEnrichment() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.wantSub != "" {
				if !contains(result, tt.wantSub) {
					t.Errorf("expected result to contain %q, got %q", tt.wantSub, result)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
