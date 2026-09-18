package application

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/CaueTech/metron-engine/internal/domain"
)

var (
	ErrInvalidEventType = errors.New("invalid or unknown event type")
	ErrEmptySourceID    = errors.New("source id cannot be empty")
)

func NewEvent(sourceID uuid.UUID, eventType EventType) (*Event, error) {
	if sourceID == uuid.Nil {
		return nil, ErrEmptySourceID
	}
	if !eventType.IsValid() {
		return nil, ErrInvalidEventType
	}

	return &Event{
		id:        uuid.New(),      // Generates uuid internamente
		sourceID:  sourceID,
		eventType: eventType,
		timestamp: time.Now().UTC(), // UTC timestamp format
	}, nil
}

// IsValid() validates the EventType based on 
func (t EventType) IsValid() bool {
	return t > EventTypeUnknown && t < maxEventType
}