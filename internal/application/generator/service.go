package generator

import (
	"context"
	"errors"
	"github.com/CaueTech/metron-engine/internal/domain"
)

type EventPublisher interface{
	Publish(ctx context.Context, event *domain.Event) error
}

var(
	ErrGenEvent = errors.New("failed to generate event when running a GeneratorService")
)

type GeneratorService struct {
	publisher EventPublisher
}

func NewGeneratorService (pub EventPublisher) *GeneratorService{
	return &GeneratorService{
		publisher: pub,
	}
}

func (s *GeneratorService) Run(ctx context.Context) error{
	event, err := GenerateEvent()
	if err != nil{
		return ErrGenEvent
	}

	s.publisher.Publish(ctx, event)
	return nil
}