package domain

import (
	"time"

	"github.com/google/uuid"
)

type EventType uint8

const (
	EventTypeUnknown EventType = iota
	EventTypeOverheat
	EventTypeMechanicalWarning
	EventTypeVoltageDrop
	MaxEventType
)

// IsValid valida se o tipo de evento está dentro das categorias discretas válidas
func (t EventType) IsValid() bool {
	return t > EventTypeUnknown && t < MaxEventType
}

type Event struct {
	id        uuid.UUID  // ID of the event
	sourceID  uuid.UUID  // ID of a abstract robot
	eventType EventType  // Discret typing
	timestamp time.Time  // When the event happened
}

// New Event instance with internal fields
func New(id, sourceID uuid.UUID, eventType EventType, timestamp time.Time) *Event {
	return &Event{
		id:        id,
		sourceID:  sourceID,
		eventType: eventType,
		timestamp: timestamp,
	}
}

// Exported getters to keep private fields
func (e *Event) ID() uuid.UUID        { return e.id }
func (e *Event) SourceID() uuid.UUID  { return e.sourceID }
func (e *Event) Type() EventType      { return e.eventType }
func (e *Event) Timestamp() time.Time { return e.timestamp }