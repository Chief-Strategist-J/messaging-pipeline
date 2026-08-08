package unit

import (
	"testing"

	"event-platform/ingestion-api/internal/eventtypes"
)

func TestValidatePayload(t *testing.T) {
	tests := []struct {
		name    string
		cfg     eventtypes.EventTypeConfig
		payload string
		wantErr bool
	}{
		{
			name:    "no rules skips validation entirely",
			cfg:     eventtypes.EventTypeConfig{Name: "heartbeat", Topic: "t"},
			payload: "not-even-json",
			wantErr: false,
		},
		{
			name: "required field present",
			cfg: eventtypes.EventTypeConfig{
				Name:         "pv",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "url", Required: true}},
			},
			payload: `{"url":"https://example.com"}`,
			wantErr: false,
		},
		{
			name: "required field missing",
			cfg: eventtypes.EventTypeConfig{
				Name:         "pv",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "url", Required: true}},
			},
			payload: `{"other":"value"}`,
			wantErr: true,
		},
		{
			name: "max length within limit",
			cfg: eventtypes.EventTypeConfig{
				Name:         "pv",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "url", Required: true, MaxLength: 10}},
			},
			payload: `{"url":"short"}`,
			wantErr: false,
		},
		{
			name: "max length exceeded",
			cfg: eventtypes.EventTypeConfig{
				Name:         "pv",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "url", Required: true, MaxLength: 5}},
			},
			payload: `{"url":"too-long-value"}`,
			wantErr: true,
		},
		{
			name: "invalid json payload",
			cfg: eventtypes.EventTypeConfig{
				Name:         "test",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "x", Required: true}},
			},
			payload: `{broken`,
			wantErr: true,
		},
		{
			name: "optional field absent is fine",
			cfg: eventtypes.EventTypeConfig{
				Name:         "test",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "opt", Required: false}},
			},
			payload: `{}`,
			wantErr: false,
		},
		{
			name: "non-string field ignores max length",
			cfg: eventtypes.EventTypeConfig{
				Name:         "test",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "count", Required: true, MaxLength: 3}},
			},
			payload: `{"count": 99999}`,
			wantErr: false,
		},
		{
			name: "empty json object with no required fields",
			cfg: eventtypes.EventTypeConfig{
				Name:         "test",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "x", Required: false, MaxLength: 10}},
			},
			payload: `{}`,
			wantErr: false,
		},
		{
			name: "multiple rules all pass",
			cfg: eventtypes.EventTypeConfig{
				Name:  "purchase",
				Topic: "t",
				PayloadRules: []eventtypes.FieldRule{
					{Field: "amount_cents", Required: true},
					{Field: "currency", Required: true, MaxLength: 3},
				},
			},
			payload: `{"amount_cents": 100, "currency": "USD"}`,
			wantErr: false,
		},
		{
			name: "multiple rules one fails",
			cfg: eventtypes.EventTypeConfig{
				Name:  "purchase",
				Topic: "t",
				PayloadRules: []eventtypes.FieldRule{
					{Field: "amount_cents", Required: true},
					{Field: "currency", Required: true},
				},
			},
			payload: `{"amount_cents": 100}`,
			wantErr: true,
		},
		{
			name: "max length exactly at boundary",
			cfg: eventtypes.EventTypeConfig{
				Name:         "test",
				Topic:        "t",
				PayloadRules: []eventtypes.FieldRule{{Field: "v", Required: true, MaxLength: 5}},
			},
			payload: `{"v":"exact"}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := eventtypes.ValidatePayload(tt.cfg, tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePayload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
