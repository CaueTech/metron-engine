package kafka

import (
	"context"
	"errors"
	"time"
	"fmt"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/CaueTech/metron-engine/internal/infra/serial"
	"github.com/segmentio/kafka-go"
)

var (
	ErrWriterPublish = errors.New("Writing error ocurred while trying to publishing an event to Kafka")
	ErrWriterClose = errors.New("Couldn't close the kafka writer properly")
)

// Compile-time check to ensure PublisherAdapter satisfies domain.EventPublisher.
var _ domain.EventPublisher = (*PublisherAdapter)(nil)

// Wrapper to enable an Adapter when working with the generator package
type PublisherAdapter struct {
	writer *kafka.Writer
}

func newKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		// Here the brokers are unpacked in case it contains more than one element.
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		// Hash partitions deterministically by message Key (SourceID), preserving
		// per-source ordering and enabling partitioned stateful aggregation.
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		WriteTimeout: 10 * time.Second,
	}
}

func NewPublisherAdapter(brokers []string, topic string) *PublisherAdapter {
	return &PublisherAdapter{
		writer: newKafkaWriter(brokers, topic),
	}
}

func (k *PublisherAdapter) Publish(ctx context.Context, event *domain.Event) error {
	bytes, err := serial.Serialize(event)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Value: bytes,
		Key:   []byte(event.SourceID().String()),
	}

	err = k.writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWriterPublish, err)
	}

	return nil
}

func (k *PublisherAdapter) Close() error {
	if err := k.writer.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrWriterClose, err)
	}

	return nil
}