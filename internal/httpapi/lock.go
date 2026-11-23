package httpapi

import (
	"context"
	"sync"
	"time"
)

type keyedLocks struct {
	mu    sync.Mutex
	items map[string]chan struct{}
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{items: make(map[string]chan struct{})}
}

func (l *keyedLocks) acquire(ctx context.Context, key string, wait time.Duration) (func(), bool) {
	l.mu.Lock()
	ch, ok := l.items[key]
	if !ok {
		ch = make(chan struct{}, 1)
		l.items[key] = ch
	}
	l.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	case <-timer.C:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}
