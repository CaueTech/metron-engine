package domain

import (
	"context"
)

// MessageHandler processes a single consumed message.
// Returning nil signals the consumer that the message was handled (published,
// routed to DLQ, or deduplicated) and its offset can be safely committed.
// Returning a non-nil error prevents offset commit to preserve at-least-once semantics.
type MessageHandler func(ctx context.Context, msg Message) error

type EventConsumer interface {
	Consume(ctx context.Context, handler MessageHandler) error
	Close() error
}

type EventPublisher interface {
	Publish(ctx context.Context, event *Event) error
}