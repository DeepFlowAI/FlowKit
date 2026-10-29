package telecom

import (
	"context"
	"log/slog"
	"time"
)

// SweeperConfig controls the lease expiry scanner.
type SweeperConfig struct {
	// Interval between scans. Defaults to 5s when zero.
	Interval time.Duration
	// Logger; defaults to slog.Default().
	Logger *slog.Logger
}

// RunSweeper scans the Catalog at Interval until ctx is cancelled. For every
// Provider whose lease IsDead, its Trunks are removed (RemoveProvider).
// Providers that enter the stale window are logged once at warn level.
//
// The Catalog itself is responsible for emitting telecom.lease.stale /
// telecom.trunk.deregistered events (via EventSink); sweeper just calls
// RemoveProvider, which triggers them.
func (c *Catalog) RunSweeper(ctx context.Context, cfg SweeperConfig) {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "telecom-sweeper")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	staleSeen := make(map[string]time.Time)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := c.clockNow()
			for _, lease := range c.Providers() {
				switch {
				case lease.IsDead(now):
					if _, err := c.removeProviderWithReason(lease.ProviderID, ReasonLeaseExpired); err != nil {
						log.Warn("sweeper: remove failed", "provider_id", lease.ProviderID, "err", err)
					} else {
						log.Info("sweeper: provider removed (lease expired beyond stale window)",
							"provider_id", lease.ProviderID,
							"last_heartbeat", lease.LastHeartbeat,
							"expired_at", lease.ExpiresAt)
					}
					delete(staleSeen, lease.ProviderID)
				case lease.IsStale(now):
					if _, seen := staleSeen[lease.ProviderID]; !seen {
						staleUntil := lease.ExpiresAt.Add(lease.StaleAcceptable)
						log.Warn("sweeper: provider stale (heartbeat stopped)",
							"provider_id", lease.ProviderID,
							"last_heartbeat", lease.LastHeartbeat,
							"stale_until", staleUntil)
						c.emit(EventLeaseStale, map[string]any{
							"provider_id": lease.ProviderID,
							"stale_until": staleUntil,
						})
						staleSeen[lease.ProviderID] = now
					}
				default:
					delete(staleSeen, lease.ProviderID)
				}
			}
		}
	}
}

// clockNow exposes c.now under the read lock for tests / sweeper.
func (c *Catalog) clockNow() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now()
}
