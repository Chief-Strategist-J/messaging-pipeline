package unit

import (
	"testing"

	"event-platform/ingestion-api/internal/eventtypes"
)

func TestRegisterAndGetCustomProcessor(t *testing.T) {
	called := false
	eventtypes.RegisterCustomProcessor("testProc", func(payload string) (string, error) {
		called = true
		return payload, nil
	})

	proc, ok := eventtypes.GetCustomProcessor("testProc")
	if !ok {
		t.Fatal("expected custom processor to be found")
	}
	result, err := proc(`{"key":"val"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected processor function to be called")
	}
	if result != `{"key":"val"}` {
		t.Errorf("expected payload passthrough, got %s", result)
	}
}

func TestGetCustomProcessorEmptyName(t *testing.T) {
	_, ok := eventtypes.GetCustomProcessor("")
	if ok {
		t.Error("expected false for empty processor name")
	}
}

func TestGetCustomProcessorUnregistered(t *testing.T) {
	_, ok := eventtypes.GetCustomProcessor("does_not_exist")
	if ok {
		t.Error("expected false for unregistered processor")
	}
}
