package validator

import (
	"testing"
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/google/uuid"
)

func TestSlidingManager_Deduplication(t *testing.T) {
	sm := newSlidingManager(3)
	id1 := uuid.New()
	id2 := uuid.New()

	// First time seeing id1: must return true
	if !sm.check(id1) {
		t.Errorf("expected id1 to be new, got duplicate")
	}

	// Immediate duplicate: must return false
	if sm.check(id1) {
		t.Errorf("expected id1 to be detected as duplicate, got new")
	}

	// Another new ID: must return true
	if !sm.check(id2) {
		t.Errorf("expected id2 to be new, got duplicate")
	}
}

func TestSlidingManager_Eviction(t *testing.T) {
	capacity := 2
	sm := newSlidingManager(capacity)

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	sm.check(id1) // queue: [id1]
	sm.check(id2) // queue: [id1, id2] - full

	// Adding id3 should evict the oldest (id1)
	sm.check(id3) // queue: [id2, id3]

	// id1 was evicted: checking it again should be allowed (returns true)
	if !sm.check(id1) {
		t.Errorf("expected evicted id1 to be treated as new, got rejected")
	}

	// Now id1 was re-added and evicted id2!
	// Re-checking id1 immediately must return false (duplicate)
	if sm.check(id1) {
		t.Errorf("expected id1 to be rejected as duplicate right after being added")
	}
}

func TestValidatePayload_Rules(t *testing.T) {
	now := time.Now().UTC()
	validSourceID := uuid.New()
	validType := domain.EventTypeOverheat
	validTimestamp := now

	tests := []struct {
		name      string
		event     *domain.Event
		wantValid bool
	}{
		{
			name:      "valid event",
			event:     domain.NewEvent(uuid.New(), validSourceID, validType, validTimestamp),
			wantValid: true,
		},
		{
			name:      "nil sourceID is rejected",
			event:     domain.NewEvent(uuid.New(), uuid.Nil, validType, validTimestamp),
			wantValid: false,
		},
		{
			name:      "nil event ID is rejected",
			event:     domain.NewEvent(uuid.Nil, validSourceID, validType, validTimestamp),
			wantValid: false,
		},
		{
			name:      "far future timestamp rejected",
			event:     domain.NewEvent(uuid.New(), validSourceID, validType, now.Add(2*time.Minute)),
			wantValid: false,
		},
		{
			name:      "far past timestamp rejected",
			event:     domain.NewEvent(uuid.New(), validSourceID, validType, now.Add(-25*time.Hour)),
			wantValid: false,
		},
		{
			name:      "unknown event type rejected",
			event:     domain.NewEvent(uuid.New(), validSourceID, domain.EventTypeUnknown, validTimestamp),
			wantValid: false,
		},
		{
			name:      "sentinel MaxEventType rejected",
			event:     domain.NewEvent(uuid.New(), validSourceID, domain.MaxEventType, validTimestamp),
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := NewValidator(100)
			got := val.ValidatePayload(tt.event, now)
			if got != tt.wantValid {
				t.Errorf("ValidatePayload() = %v, want %v", got, tt.wantValid)
			}
		})
	}
}

func TestValidatePayload_StatefulDeduplication(t *testing.T) {
	now := time.Now().UTC()
	val := NewValidator(10)

	sharedID := uuid.New()
	event1 := domain.NewEvent(sharedID, uuid.New(), domain.EventTypeOverheat, now)
	event2 := domain.NewEvent(sharedID, uuid.New(), domain.EventTypeOverheat, now)

	// First occurrence must pass
	if !val.ValidatePayload(event1, now) {
		t.Fatal("first event with sharedID should be valid")
	}

	// Reusing the same event ID on the same Validator instance must be rejected
	if val.ValidatePayload(event2, now) {
		t.Fatal("second event with duplicate ID should be rejected by Validator")
	}
}