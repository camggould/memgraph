package sqlite

import (
	"context"
	"sync"

	memgraph "github.com/camggould/memgraph"
)

// subscribers is a tiny in-memory pub/sub registry. Handlers fire
// AFTER commit so they never see an uncommitted write.
type subscribers struct {
	mu      sync.RWMutex
	next    uint64
	handles map[uint64]memgraph.WriteHandler
}

func newSubscribers() *subscribers {
	return &subscribers{handles: make(map[uint64]memgraph.WriteHandler)}
}

func (s *subscribers) add(h memgraph.WriteHandler) memgraph.Unsubscribe {
	s.mu.Lock()
	id := s.next
	s.next++
	s.handles[id] = h
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.handles, id)
		s.mu.Unlock()
	}
}

func (s *subscribers) notifyNode(ctx context.Context, n memgraph.Node) {
	s.mu.RLock()
	hs := make([]memgraph.WriteHandler, 0, len(s.handles))
	for _, h := range s.handles {
		hs = append(hs, h)
	}
	s.mu.RUnlock()
	for _, h := range hs {
		h.OnNodeWritten(ctx, n)
	}
}

func (s *subscribers) notifyEdge(ctx context.Context, e memgraph.Edge) {
	s.mu.RLock()
	hs := make([]memgraph.WriteHandler, 0, len(s.handles))
	for _, h := range s.handles {
		hs = append(hs, h)
	}
	s.mu.RUnlock()
	for _, h := range hs {
		h.OnEdgeWritten(ctx, e)
	}
}
