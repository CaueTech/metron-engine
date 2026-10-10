package generator

import (
	"context"
	"github.com/CaueTech/metron-engine/internal/domain"
)

type EventPublisher interface{
	Publish(ctx context.Context, event *domain.Event) error
}

type GeneratorService struct {
	publisher EventPublisher
}

func NewGeneratorService (pub EventPublisher) *GeneratorService{
	return &GeneratorService{
		publisher: pub,
	}
}

/*
 With the following function, essentially, we can run a GeneratorService without explictly needing a Kafka infra behind, enabling unit testing and leaving the responsabilities of the layers much cleaner.
*/
func (s *GeneratorService) Run(ctx context.Context) error{
	event, err := GenerateEvent()
	
	if err != nil{
		return err
	}

	if err := s.publisher.Publish(ctx, event); err != nil{
		return err
	}
	
	return nil
}