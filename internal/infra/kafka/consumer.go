package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/segmentio/kafka-go"
)

const commitTimeout = 5 * time.Second

// Compile-time check to ensure ConsumerAdapter satisfies domain.EventConsumer.
var _ domain.EventConsumer = (*ConsumerAdapter)(nil)

type ConsumerAdapter struct {
	reader *kafka.Reader
}

var (
	ErrReaderFetch  = errors.New("failed to fetch message from Kafka")
	ErrReaderCommit = errors.New("failed to commit Kafka offset")
	ErrReaderClose  = errors.New("failed to close Kafka reader")
)

func newKafkaReader(brokers []string, topic string, groupID string) *kafka.ReaderConfig {
	return &kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}

func NewConsumerAdapter(brokers []string, topic string, groupID string) *ConsumerAdapter {
	return &ConsumerAdapter{
		reader: kafka.NewReader(*newKafkaReader(brokers, topic, groupID)),
	}
	// May look weird, but NewReader() function needs a struct from ReaderConfig first
}

// Consume blocks the calling goroutine, fetching messages sequentially and invoking handler.
// Offsets are committed only when handler returns nil, preserving at-least-once delivery.
func (c *ConsumerAdapter) Consume(ctx context.Context, handler domain.MessageHandler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// Expected termination signals during graceful shutdown.
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("%w: %v", ErrReaderFetch, err)
		}

		messageEntity := domain.Message{
			Payload:   msg.Value,
			Timestamp: msg.Time,
		}

		if err := handler(ctx, messageEntity); err != nil {
			return err
		}

		// Use a bounded background context so an in-flight commit can complete
		// during graceful shutdown without hanging indefinitely if the broker is unreachable.
		commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err = c.reader.CommitMessages(commitCtx, msg)
		cancel()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrReaderCommit, err)
		}
	}
}

func (c *ConsumerAdapter) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("%w: %v", ErrReaderClose, err)
	}

	return nil
}