package validator

import (
	"sync"
	"github.com/google/uuid"
)

type slidingManager struct{
	mu sync.Mutex // This field is here for idiomatic reasons, it is equivalent to using sync.Mutex normally
	capacity int
	seen map[uuid.UUID]struct{}
	queue []uuid.UUID
}

/*
 WindowManager.seen field is expected to only store the uuid.UUID, so that we can do a direct search, but to not waste memory with a value for each field, we use struct{} so that it takes no memory for it
 */

func newSlidingManager(capacity int) *slidingManager{
	return &slidingManager{
		seen: make(map[uuid.UUID]struct{}, capacity),
		capacity: capacity,
		queue: make([]uuid.UUID, 0, capacity),
	}
}

func (s *slidingManager) check(id uuid.UUID) bool{
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.seen[id]; exists{
		return false
	}

	if len(s.queue) >= s.capacity{
		oldest := s.queue[0]
		s.queue = s.queue[1:]
		delete(s.seen, oldest)
	}

	s.seen[id] = struct{}{}
	s.queue = append(s.queue, id)

	return true
}