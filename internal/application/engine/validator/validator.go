package validator

import (
	"errors"
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/google/uuid"
)

const (
	maxFutureTolerance = 1 * time.Minute
	maxPastTolerance   = 24 * time.Hour
)

var (
	ErrNilEvent         = errors.New("event cannot be nil")
	ErrInvalidSourceID  = errors.New("source ID is nil")
	ErrInvalidEventID   = errors.New("event ID is nil")
	ErrTimestampFuture  = errors.New("event timestamp exceeds future tolerance")
	ErrTimestampPast    = errors.New("event timestamp exceeds past tolerance")
	ErrInvalidEventType = errors.New("event type is invalid")
	ErrDuplicateEvent   = errors.New("event ID already processed")
)

type Validator struct {
	window *slidingManager
}

func NewValidator(capacity int) *Validator {
	return &Validator{
		window: newSlidingManager(capacity),
	}
}

// ValidatePayload checks domain invariants and deduplicates by event ID.
// Returns nil if valid, or a sentinel error identifying the rejection reason
// so callers can route duplicates differently from DLQ-bound invalid events.
func (v *Validator) ValidatePayload(event *domain.Event, now time.Time) error {
	if event == nil {
		return ErrNilEvent
	}

	if event.SourceID() == uuid.Nil {
		return ErrInvalidSourceID
	}

	if event.Timestamp().After(now.Add(maxFutureTolerance)) {
		return ErrTimestampFuture
	}

	if event.Timestamp().Before(now.Add(-maxPastTolerance)) {
		return ErrTimestampPast
	}

	if !event.Type().IsValid() {
		return ErrInvalidEventType
	}

	if event.ID() == uuid.Nil {
		return ErrInvalidEventID
	}

	if !v.window.check(event.ID()) {
		return ErrDuplicateEvent
	}

	return nil
}