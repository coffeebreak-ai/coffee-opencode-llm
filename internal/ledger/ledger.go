package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

type Ledger struct {
	store        *store.Store
	reserveTTLMS int64
}

func New(st *store.Store, reserveTTLMS int64) *Ledger {
	return &Ledger{store: st, reserveTTLMS: reserveTTLMS}
}

// Reserve releases expired reservations first: the ledger cleans up lazily, at
// startup and on every reserve, so no background timer is needed.
func (l *Ledger) Reserve(ctx context.Context, requestID, modelID string, quota catalog.Quota) (*store.Reservation, error) {
	if _, err := l.store.ReleaseStaleReservations(ctx, l.reserveTTLMS); err != nil {
		return nil, fmt.Errorf("clean stale reservations: %w", err)
	}
	return l.store.Reserve(ctx, requestID, modelID, quota)
}

func (l *Ledger) Commit(ctx context.Context, reservation store.Reservation) error {
	return l.store.Commit(ctx, reservation)
}

func (l *Ledger) Release(ctx context.Context, reservation store.Reservation) error {
	return l.store.Release(ctx, reservation)
}

func (l *Ledger) Remaining(ctx context.Context, modelID string, quota catalog.Quota) (int64, bool, error) {
	return l.store.Remaining(ctx, modelID, quota)
}

func (l *Ledger) CoolingUntil(ctx context.Context, modelID, group string) (int64, error) {
	return l.store.CoolingUntil(ctx, modelID, group)
}

func (l *Ledger) OnRateLimit(ctx context.Context, modelID, group string, retryAfterMS, cooldownMS, maxMS int64) (int64, error) {
	wait := retryAfterMS
	if wait <= 0 {
		wait = cooldownMS
	}
	if wait > maxMS {
		wait = maxMS
	}
	if err := l.store.SetCooldown(ctx, modelID, group, wait); err != nil {
		return 0, err
	}
	return time.Now().UTC().UnixMilli() + wait, nil
}

func (l *Ledger) Cleanup(ctx context.Context, ttlMS int64) (int64, error) {
	return l.store.ReleaseStaleReservations(ctx, ttlMS)
}

func (l *Ledger) Learn(ctx context.Context, ownerKind, ownerID, window string, observed int64) error {
	if observed < 0 {
		return fmt.Errorf("ledger: observed limit %d is negative", observed)
	}
	return l.store.LearnLimit(ctx, ownerKind, ownerID, window, observed)
}

var ErrNoQuota = store.ErrNoQuota

func IsNoQuota(err error) bool {
	return errors.Is(err, ErrNoQuota)
}
