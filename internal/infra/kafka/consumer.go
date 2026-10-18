package kafka

import (
	//"context"
	"errors"

	//"github.com/CaueTech/metron-engine/internal/domain"
	"github.com/segmentio/kafka-go"
)

type ConsumerAdapter struct {
	reader *kafka.Reader
}

var (
	ErrReaderFetch = errors.New("Something went wrong in FetchMessage()")
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
	/*
		May look WEEIIIRD, but apparently the NewReader() function needs a struct from ReaderConfig first
	*/
}

/*
func (c *ConsumerAdapter) Consume(ctx context.Context) (<-chan domain.Message, error) {
	msgKafka, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, ErrReaderFetch
	}
	
}


func (c *ConsumerAdapter) Close() error {

}
*/