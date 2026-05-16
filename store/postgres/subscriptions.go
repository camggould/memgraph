package postgres

import (
	"context"
	"sync"

	memgraph "github.com/camggould/memgraph"
)

// subscribers is a tiny in-memory pub/sub registry. Handlers fire AFTER
// commit so they never see an uncommitted write. Cross-process subscriptions
// (LISTEN/NOTIFY) are a future enhancement.
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

func (s *subscribers) snapshot() []memgraph.WriteHandler {
	s.mu.RLock()
	hs := make([]memgraph.WriteHandler, 0, len(s.handles))
	for _, h := range s.handles {
		hs = append(hs, h)
	}
	s.mu.RUnlock()
	return hs
}

func (s *subscribers) notifyNode(ctx context.Context, n memgraph.Node) {
	for _, h := range s.snapshot() {
		h.OnNodeWritten(ctx, n)
	}
}

func (s *subscribers) notifyEdge(ctx context.Context, e memgraph.Edge) {
	for _, h := range s.snapshot() {
		h.OnEdgeWritten(ctx, e)
	}
}

func (s *subscribers) notifyGraph(ctx context.Context, g memgraph.Graph) {
	for _, h := range s.snapshot() {
		h.OnGraphCreated(ctx, g)
	}
}
