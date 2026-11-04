package kafka

import (
	"context"
	"errors"
	"io"
	"fmt"

	"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/segmentio/kafka-go"
)

type ConsumerAdapter struct {
	reader *kafka.Reader
}

var (
	ErrReaderFetch = errors.New("Something went wrong in ReadMessage()")
	ErrReaderClose = errors.New("Failed to close Kafka reader")
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

func (c *ConsumerAdapter) Consume(ctx context.Context) (<-chan domain.Message, <-chan error) {
	messages := make(chan domain.Message)
	errChan := make(chan error, 1)
	
	go func(){
		defer close(messages)
		defer close(errChan)

		for {
			msg, err := c.reader.FetchMessage(ctx)

			if err != nil{
				// These are expected errors, no need for signaling it to the terminal
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF){
					return
				}

				select{
				case errChan <- fmt.Errorf("%w: %v", ErrReaderFetch, err):
				default: // Just to prevent blocking in case of errors being unaccessible
				}
				return
			}

			messageEntity := domain.Message{
				Payload: msg.Value,
				Timestamp: msg.Time,
				Ack: func () error{
					return c.reader.CommitMessages(context.Background(), msg)
				},
			}

			select{
			case <-ctx.Done():
				return
			case messages <- messageEntity:
			}
		}
	}()

	return messages, errChan
}


func (c *ConsumerAdapter) Close() error {
	if err := c.reader.Close(); err != nil{
		return fmt.Errorf("%w: %v", ErrReaderClose, err)
	}

	if (c.reader == nil) {
		return nil
	}
	
	return nil
}