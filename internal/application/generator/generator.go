package generator

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/CaueTech/metron-engine/internal/domain"
)

var (
	ErrInvalidEventType = errors.New("invalid or unknown event type")
	ErrEmptySourceID    = errors.New("source id cannot be empty")
	// Store last valid ID for chaos generation
	lastEventID uuid.UUID
)

func initValidator (sourceID uuid.UUID, eventType domain.EventType) (error){
	if sourceID == uuid.Nil {
		return ErrEmptySourceID
	}
	if !eventType.IsValid() {
		return ErrInvalidEventType
	}
	return nil
}

// randomTimestamp from 1970 to 2100
func randomTimestamp() time.Time {
	sec := rand.Int64N(4_102_444_800)
	return time.Unix(sec, 0).UTC()
}

func NewEvent(sourceID uuid.UUID, eventType domain.EventType) (*domain.Event, error) {
	err := initValidator(sourceID, eventType)

	if err != nil{
		return nil, err
	}

	chaosIndex := rand.Float32()
	eventID := uuid.New()
	evSourceID := sourceID
	evType := eventType
	evTimestamp := time.Now().UTC()
	switch {
	case chaosIndex < 0.80:
		// All OK
	case chaosIndex < 0.85:
		evSourceID = uuid.Nil
	case chaosIndex < 0.90:
		evSourceID = uuid.Nil
		evTimestamp = randomTimestamp()
	case chaosIndex < 0.95:
		// EventID will be invalid or repeated
		if lastEventID != uuid.Nil && rand.Float32() < 0.5 {
			eventID = lastEventID
		} else {
			eventID = uuid.Nil
		}
	default:
		eventID = uuid.Nil
		evSourceID = uuid.Nil
		evType = domain.EventTypeUnknown
		evTimestamp = time.Time{}
	}
	// Updates history
	if eventID != uuid.Nil && chaosIndex < 0.90 {
		lastEventID = eventID
	}
	return domain.New(eventID, evSourceID, evType, evTimestamp), nil
}