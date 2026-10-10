package generator

import (
	"context"
	"errors"
	"testing"

	"github.com/CaueTech/metron-engine/internal/domain"
)

// 1. MOCK / TEST DOUBLE
// Implements the EventPublisher interface without connecting to a real Kafka cluster.
type mockPublisher struct {
	publishedEvent *domain.Event
	shouldFail     bool
}

func (m *mockPublisher) Publish(ctx context.Context, event *domain.Event) error {
	if m.shouldFail {
		return errors.New("kafka cluster unreachable")
	}
	m.publishedEvent = event
	return nil
}

// 2. UNIT TEST SUITE
func TestGeneratorService_Run_Success(t *testing.T) {
	// --- ARRANGE ---
	mockPub := &mockPublisher{}
	service := NewGeneratorService(mockPub)
	ctx := context.Background()

	// --- ACT ---
	err := service.Run(ctx)

	// --- ASSERT ---
	if err != nil {
		t.Fatalf("expected success on Run(), but got error: %v", err)
	}

	if mockPub.publishedEvent == nil {
		t.Fatal("expected event to be published, but publishedEvent is nil")
	}
}

func TestGeneratorService_Run_PublisherError(t *testing.T) {
	// Simulating a messaging infrastructure failure
	mockPub := &mockPublisher{shouldFail: true}
	service := NewGeneratorService(mockPub)
	ctx := context.Background()

	err := service.Run(ctx)

	if err == nil{
		t.Fatalf("expected success on Run(), but go error: %v", err)
	}
	
	if mockPub.publishedEvent != nil {
		t.Error("should not hold a published event when publisher fails")
	}
}