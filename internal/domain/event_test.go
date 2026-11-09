package domain_test

import (
	"testing"
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/google/uuid"
)

func TestEventType_IsValid(t *testing.T) {
	tests := []struct {
		name      string
		eventType domain.EventType
		want      bool
	}{
		{"unknown is invalid", domain.EventTypeUnknown, false},
		{"overheat is valid", domain.EventTypeOverheat, true},
		{"mechanical warning is valid", domain.EventTypeMechanicalWarning, true},
		{"voltage drop is valid", domain.EventTypeVoltageDrop, true},
		{"maxEventType sentinel is invalid", domain.MaxEventType, false},
		{"beyond maxEventType is invalid", domain.MaxEventType + 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.eventType.IsValid(); got != tt.want {
				t.Errorf("EventType.IsValid() = %v, want %v for %v", got, tt.want, tt.eventType)
			}
		})
	}
}

func TestEvent_Getters(t *testing.T) {
	id := uuid.New()
	sourceID := uuid.New()
	eventType := domain.EventTypeOverheat
	timestamp := time.Now().UTC()

	event := domain.NewEvent(id, sourceID, eventType, timestamp)

	if event.ID() != id {
		t.Errorf("ID() = %v, want %v", event.ID(), id)
	}
	if event.SourceID() != sourceID {
		t.Errorf("SourceID() = %v, want %v", event.SourceID(), sourceID)
	}
	if event.Type() != eventType {
		t.Errorf("Type() = %v, want %v", event.Type(), eventType)
	}
	if !event.Timestamp().Equal(timestamp) {
		t.Errorf("Timestamp() = %v, want %v", event.Timestamp(), timestamp)
	}
}
