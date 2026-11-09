package validator

import(
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/google/uuid"
)

const (
	maxFutureTolerance = 1 * time.Minute
	maxPastTolerance   = 24 * time.Hour 
)

type Validator struct{
	window *slidingManager
}

func NewValidator (capacity int) *Validator{
	return &Validator{
		window: newSlidingManager(capacity),
	}
}

// Still lacks EventID and EventType verification
func (v *Validator) ValidatePayload(event *domain.Event, now time.Time) bool{
	if(event.SourceID() == uuid.Nil){
		return false
	}

	if(event.Timestamp().After(now.Add(maxFutureTolerance))){
		return false
	}

	if event.Timestamp().Before((now.Add(-maxPastTolerance))){
		return false
	}

	e := event.Type()

	if e < domain.EventTypeUnknown || e >= domain.MaxEventType || e == domain.EventTypeUnknown{
		return false
	}
	
	if(event.ID() == uuid.Nil || !v.window.check(event.ID())){
		return false
	}
	
	return true
}