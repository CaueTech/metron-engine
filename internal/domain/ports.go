package domain

import (
	"context"
)

type EventConsumer interface{
	Consume(ctx context.Context)(<-chan Message, error)
	Close() error
}

type EventPublisher interface{
	Publish(ctx context.Context, event *Event) error
}