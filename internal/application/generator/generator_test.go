package generator

import (
	"testing"

	"github.com/google/uuid"
	"github.com/CaueTech/metron-engine/internal/domain"
)

func TestNewEventValidation(t *testing.T) {
	// Nil sourceID must fail initial validation
	_, err := NewEvent(uuid.Nil, domain.EventTypeOverheat)
	if err != ErrEmptySourceID {
		t.Errorf("expected ErrEmptySourceID, got %v", err)
	}

	// Unknown eventType must fail initial validation
	_, err = NewEvent(uuid.New(), domain.EventTypeUnknown)
	if err != ErrInvalidEventType {
		t.Errorf("expected ErrInvalidEventType, got %v", err)
	}
}

func TestNewEventChaosGeneration(t *testing.T) {
	validSourceID := uuid.New()
	validType := domain.EventTypeOverheat

	// Generate multiple events to exercise the different chaos index ranges
	for range 100 {
		ev, err := NewEvent(validSourceID, validType)
		if err != nil {
			t.Fatalf("NewEvent should not return an error when generating event with valid parameters: %v", err)
		}
		if ev == nil {
			t.Fatal("generated event cannot be nil")
		}
	}
}