package domain

import (
	"time"
)

type Message struct {
	Payload   []byte
	Timestamp time.Time
}