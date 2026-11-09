package serial_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/CaueTech/metron-engine/internal/infra/serial"
	"github.com/google/uuid"
)

func TestSerialize_Deserialize_Roundtrip(t *testing.T) {
	id := uuid.New()
	sourceID := uuid.New()
	eventType := domain.EventTypeVoltageDrop
	// Protobuf timestamp is stored in milliseconds, so truncate to milliseconds
	timestamp := time.Now().UTC().Truncate(time.Millisecond)

	original := domain.NewEvent(id, sourceID, eventType, timestamp)

	payload, err := serial.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("Serialize produced empty payload")
	}

	restored, err := serial.Deserialize(payload)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if restored.ID() != original.ID() {
		t.Errorf("ID mismatch: got %v, want %v", restored.ID(), original.ID())
	}
	if restored.SourceID() != original.SourceID() {
		t.Errorf("SourceID mismatch: got %v, want %v", restored.SourceID(), original.SourceID())
	}
	if restored.Type() != original.Type() {
		t.Errorf("EventType mismatch: got %v, want %v", restored.Type(), original.Type())
	}
	if !restored.Timestamp().Equal(original.Timestamp()) {
		t.Errorf("Timestamp mismatch: got %v, want %v", restored.Timestamp(), original.Timestamp())
	}
}

func TestSerialize_NilEvent(t *testing.T) {
	_, err := serial.Serialize(nil)
	if err == nil {
		t.Fatal("expected error when serializing nil event, got nil")
	}
	if err != serial.ErrNullEvent {
		t.Errorf("expected ErrNullEvent, got: %v", err)
	}
}

func TestDeserialize_NilPayload(t *testing.T) {
	_, err := serial.Deserialize(nil)
	if err == nil {
		t.Fatal("expected error when deserializing nil payload, got nil")
	}
	if err != serial.ErrNullPayload {
		t.Errorf("expected ErrNullPayload, got: %v", err)
	}
}

func TestDeserialize_CorruptedBytes(t *testing.T) {
	corrupted := []byte{0xFF, 0xFE, 0xFD, 0x00, 0x12}
	_, err := serial.Deserialize(corrupted)
	if err == nil {
		t.Fatal("expected error when deserializing corrupted bytes, got nil")
	}
}

func TestSerialize_CheckNonEmptyPayload(t *testing.T) {
	event := domain.NewEvent(uuid.New(), uuid.New(), domain.EventTypeOverheat, time.Now().UTC())
	payload1, err1 := serial.Serialize(event)
	payload2, err2 := serial.Serialize(event)

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected serialization error: %v, %v", err1, err2)
	}

	if !bytes.Equal(payload1, payload2) {
		t.Error("deterministic serialization expected for the same event")
	}
}
