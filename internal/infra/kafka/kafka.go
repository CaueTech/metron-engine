package kafka

import{
	"github.com/segmetion/kafka-go"
}

func NewKafkaWriter(brokers []string, topic string)(&kafka.Writer){
	return &kafka.Writer{
		// Here the brokers are unpacked in case it contains more than one element.
		Addr: kafka.TCP(brokers...),
		Topic: topic,
		Balancer: &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		WriteTimeout: 10 * time.Second
	}
}