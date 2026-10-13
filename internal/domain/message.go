package domain

import (
	"time"
)

type Message struct{
	Payload []byte
	Timestamp time.Time

	/*
	May look weird but this defines a field inside the struct that has a function whose return value is err. The function marks the partition in Kafka as done so that we can move on for the next
	*/
	Ack func() error
}