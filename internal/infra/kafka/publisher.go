package kafka

import (
	"context"
	"errors"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/CaueTech/metron-engine/internal/infra/proto"
	"github.com/segmentio/kafka-go"
)

var (
	ErrWriter     = errors.New("writing error ocurred while trying to publishing an event to Kafka")
)

// Wrapper to enable an Adapter when working with the generator package
type PublisherAdapter struct {
	writer *kafka.Writer
}

func NewPublisherAdapter(brokers []string, topic string) *PublisherAdapter {
	return &PublisherAdapter{
		writer: NewKafkaWriter(brokers, topic),
	}
}

func (k *PublisherAdapter) Publish(ctx context.Context, event *domain.Event) error {
	bytes, err := proto.Serialize(event)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Value: bytes,
		Key:   []byte(event.SourceID().String()),
	}

	err = k.writer.WriteMessages(ctx, message)
	if err != nil {
		return ErrWriter
	}

	return nil
}

func (k *PublisherAdapter) Close() error {
	return k.writer.Close()
}