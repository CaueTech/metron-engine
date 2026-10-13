package generator

import (
	"context"
	"github.com/CaueTech/metron-engine/internal/domain"
)

type generatorService struct {
	publisher domain.EventPublisher
}

func NewGeneratorService (pub domain.EventPublisher) *generatorService{
	return &generatorService{
		publisher: pub,
	}
}

/*
 With the following function, essentially, we can run a GeneratorService without explictly needing a Kafka infra behind, enabling unit testing and leaving the responsabilities of the layers much cleaner.
*/
func (s *generatorService) Run(ctx context.Context) error{
	event, err := GenerateEvent()
	
	if err != nil{
		return err
	}

	if err := s.publisher.Publish(ctx, event); err != nil{
		return err
	}
	
	return nil
}