package validator

import(
	"errors"
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/google/uuid"
)

var(
	ErrInvalidField = errors.New("an event field was checked invalid")
)

const (
	maxFutureTolerance = 1 * time.Minute
	maxPastTolerance   = 24 * time.Hour 
)

// Still lacks EventID and EventType verification
func ValidatePayload(event *domain.Event, now time.Time) bool{
	if(event.SourceID() == uuid.Nil){
		return false
	}

	if(event.Timestamp().After(now.Add(maxFutureTolerance))){
		return false
	}

	if event.Timestamp().Before((now.Add(-maxPastTolerance))){
		return false
	}
	
	return true
}