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
	reader  *kafka.Reader
	pending *kafka.Message
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
// If handler or CommitMessages fails, the uncommitted message is retained and redelivered
// on the next Consume call before fetching newer messages, preventing later offsets from
// being committed ahead of an uncommitted failure.
func (c *ConsumerAdapter) Consume(ctx context.Context, handler domain.MessageHandler) error {
	for {
		var msg kafka.Message
		if c.pending != nil {
			if err := ctx.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			msg = *c.pending
		} else {
			fetched, err := c.reader.FetchMessage(ctx)
			if err != nil {
				// Expected termination signals during graceful shutdown.
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("%w: %w", ErrReaderFetch, err)
			}
			msg = fetched
		}

		messageEntity := domain.Message{
			Payload:   msg.Value,
			Timestamp: msg.Time,
		}

		if err := handler(ctx, messageEntity); err != nil {
			c.pending = &msg
			return err
		}

		// Use a bounded background context so an in-flight commit can complete
		// during graceful shutdown without hanging indefinitely if the broker is unreachable.
		commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
		err := c.reader.CommitMessages(commitCtx, msg)
		cancel()
		if err != nil {
			c.pending = &msg
			return fmt.Errorf("%w: %w", ErrReaderCommit, err)
		}

		c.pending = nil
	}
}

func (c *ConsumerAdapter) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrReaderClose, err)
	}

	return nil
}