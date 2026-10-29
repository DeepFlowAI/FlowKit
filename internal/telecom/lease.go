package telecom

import (
	"fmt"
	"time"
)

// ProviderLease tracks one Provider's heartbeat state.
type ProviderLease struct {
	ProviderID      string
	Revision        int64
	LeaseTTL        time.Duration
	StaleAcceptable time.Duration
	LastHeartbeat   time.Time
	ExpiresAt       time.Time // zero = never expires (system Provider)
	TrunkIDs        []string  // sorted
}

// IsExpired reports whether the lease has passed its hard expiry (i.e.
// LeaseTTL elapsed). Never-expiring leases (system Provider) return false.
func (l *ProviderLease) IsExpired(now time.Time) bool {
	if l.ExpiresAt.IsZero() {
		return false
	}
	return now.After(l.ExpiresAt)
}

// IsStale reports whether the lease has expired but is still within the
// stale_acceptable window. During this window inbound calls are still
// accepted but outbound originate is rejected.
func (l *ProviderLease) IsStale(now time.Time) bool {
	if !l.IsExpired(now) {
		return false
	}
	if l.StaleAcceptable <= 0 {
		return false
	}
	return now.Before(l.ExpiresAt.Add(l.StaleAcceptable))
}

// IsDead reports whether the lease has passed both expiry and stale window
// — caller should hard-delete this Provider's trunks.
func (l *ProviderLease) IsDead(now time.Time) bool {
	if !l.IsExpired(now) {
		return false
	}
	return now.After(l.ExpiresAt.Add(l.StaleAcceptable))
}

func (l *ProviderLease) clone() *ProviderLease {
	c := *l
	c.TrunkIDs = append([]string(nil), l.TrunkIDs...)
	return &c
}

// StaleRevisionError indicates the submitted snapshot revision is older
// than what the Catalog currently holds for this Provider.
type StaleRevisionError struct {
	ProviderID      string
	CurrentRevision int64
	Submitted       int64
}

func (e *StaleRevisionError) Error() string {
	return fmt.Sprintf("stale revision: provider %q current=%d submitted=%d",
		e.ProviderID, e.CurrentRevision, e.Submitted)
}

// TrunkIDConflictError indicates the trunk_id is already owned by a
// different Provider.
type TrunkIDConflictError struct {
	TrunkID          string
	ExistingProvider string
}

func (e *TrunkIDConflictError) Error() string {
	return fmt.Sprintf("trunk_id %q already owned by provider %q",
		e.TrunkID, e.ExistingProvider)
}

// ProviderNotFoundError is returned by Heartbeat / RemoveProvider when the
// provider_id is unknown.
type ProviderNotFoundError struct {
	ProviderID string
}

func (e *ProviderNotFoundError) Error() string {
	return fmt.Sprintf("provider %q not found", e.ProviderID)
}
