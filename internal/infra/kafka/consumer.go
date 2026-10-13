package kafka

import (
	/*
	"context"

	"github.com/CaueTech/metron-engine/internal/domain"
	*/
)

type ConsumerAdapter struct{
	brokers []string
	topic string
	// Missing some Kafka dependencies here [...]
}

func NewConsumerAdapter (brokers []string, topic string) (*ConsumerAdapter){
	return &ConsumerAdapter{
		brokers: brokers,
		topic: topic,
		// [...]
	}
}

/*
func (c * ConsumerAdapter) Consume(ctx context.Context)(<-chan Message, error){

}

func (c * ConsumerAdapter) Close() error{

}
*/