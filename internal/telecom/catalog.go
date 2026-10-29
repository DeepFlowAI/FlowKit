package telecom

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// SystemProviderID is the reserved provider_id for the env-var-loaded Provider.
// External Providers attempting to register under this name are rejected.
const SystemProviderID = "system"

// TrunkType enumerates which directions a trunk supports.
type TrunkType string

const (
	TrunkInbound  TrunkType = "inbound"
	TrunkOutbound TrunkType = "outbound"
)

// PeerEndpoint identifies an upstream SIP peer (carrier-side) by IP/CIDR+port.
type PeerEndpoint struct {
	IP   string // dotted-quad or CIDR (e.g. "203.0.113.10" or "203.0.113.0/24")
	Port int    // 0 = any
}

// OutboundProfile is the per-trunk outbound gateway config used for originate.
type OutboundProfile struct {
	Server       string // host or host:port
	Port         int    // ignored if Server already has :port
	User         string
	Pass         string
	Realm        string
	DID          string // default caller_id fallback
	CalleePrefix string // prepended to bare callee digits (e.g. "035")
}

// Ready reports whether this outbound profile is usable.
func (p OutboundProfile) Ready() bool {
	return strings.TrimSpace(p.Server) != ""
}

// TrunkEntry is one row in the Catalog.
type TrunkEntry struct {
	ID             string
	ProviderID     string
	TrunkTypes     []TrunkType
	Status         string // "enabled" | "disabled"
	PeerEndpoints  []PeerEndpoint
	Outbound       OutboundProfile
}

// SupportsInbound returns true when this trunk accepts inbound INVITEs.
func (t *TrunkEntry) SupportsInbound() bool {
	if t.Status != "" && t.Status != "enabled" {
		return false
	}
	for _, tt := range t.TrunkTypes {
		if tt == TrunkInbound {
			return true
		}
	}
	return false
}

// SupportsOutbound returns true when this trunk can originate outbound calls.
func (t *TrunkEntry) SupportsOutbound() bool {
	if t.Status != "" && t.Status != "enabled" {
		return false
	}
	for _, tt := range t.TrunkTypes {
		if tt == TrunkOutbound {
			return true
		}
	}
	return false
}

// Snapshot is the full set of trunks owned by one Provider.
// PUT /snapshot is total replacement (declarative state), not append.
type Snapshot struct {
	ProviderID         string
	Revision           int64
	LeaseTTL           time.Duration // 0 = never expires (system Provider only)
	StaleAcceptable    time.Duration // grace window after lease expires
	Trunks             []TrunkEntry
}

// Catalog is the thread-safe registry of trunks across all Providers.
type Catalog struct {
	mu        sync.RWMutex
	now       func() time.Time
	trunks    map[string]*TrunkEntry    // trunk_id → entry
	providers map[string]*ProviderLease // provider_id → lease
	ipIndex   *IPIndex
	events    EventSink
}

// NewCatalog returns an empty Catalog. Inject a clock for tests; nil uses time.Now.
func NewCatalog(clock func() time.Time) *Catalog {
	if clock == nil {
		clock = time.Now
	}
	return &Catalog{
		now:       clock,
		trunks:    make(map[string]*TrunkEntry),
		providers: make(map[string]*ProviderLease),
		ipIndex:   NewIPIndex(),
		events:    noopSink{},
	}
}

// ApplySnapshot replaces all trunks owned by snap.ProviderID with snap.Trunks.
// Returns the new lease (caller may inspect ExpiresAt) or an error if
// validation fails or revision is stale.
func (c *Catalog) ApplySnapshot(snap Snapshot) (*ProviderLease, error) {
	if err := validateSnapshot(&snap); err != nil {
		return nil, err
	}

	type pendingEvent struct {
		method  string
		payload map[string]any
	}
	var pending []pendingEvent
	var leaseClone *ProviderLease

	c.mu.Lock()
	if existing, ok := c.providers[snap.ProviderID]; ok {
		if snap.Revision > 0 && snap.Revision <= existing.Revision {
			c.mu.Unlock()
			return nil, &StaleRevisionError{
				ProviderID:      snap.ProviderID,
				CurrentRevision: existing.Revision,
				Submitted:       snap.Revision,
			}
		}
	}

	// Reject trunk_ids owned by other Providers.
	for i := range snap.Trunks {
		t := &snap.Trunks[i]
		if owner, ok := c.trunks[t.ID]; ok && owner.ProviderID != snap.ProviderID {
			c.mu.Unlock()
			return nil, &TrunkIDConflictError{TrunkID: t.ID, ExistingProvider: owner.ProviderID}
		}
	}

	// Compute the diff before mutating, so we can emit precise events.
	oldIDs := map[string]struct{}{}
	if existing, ok := c.providers[snap.ProviderID]; ok {
		for _, id := range existing.TrunkIDs {
			oldIDs[id] = struct{}{}
		}
	}
	newIDs := make(map[string]struct{}, len(snap.Trunks))
	for i := range snap.Trunks {
		newIDs[snap.Trunks[i].ID] = struct{}{}
	}
	var droppedIDs []string
	for id := range oldIDs {
		if _, kept := newIDs[id]; !kept {
			droppedIDs = append(droppedIDs, id)
		}
	}

	for id := range oldIDs {
		delete(c.trunks, id)
	}

	now := c.now()
	lease := &ProviderLease{
		ProviderID:      snap.ProviderID,
		Revision:        snap.Revision,
		LeaseTTL:        snap.LeaseTTL,
		StaleAcceptable: snap.StaleAcceptable,
		LastHeartbeat:   now,
		TrunkIDs:        make([]string, 0, len(snap.Trunks)),
	}
	if snap.LeaseTTL > 0 {
		lease.ExpiresAt = now.Add(snap.LeaseTTL)
	} else {
		lease.ExpiresAt = time.Time{}
	}

	newTrunks := make(map[string]*TrunkEntry, len(snap.Trunks))
	for i := range snap.Trunks {
		t := snap.Trunks[i]
		t.ProviderID = snap.ProviderID
		newTrunks[t.ID] = &t
		lease.TrunkIDs = append(lease.TrunkIDs, t.ID)
	}
	sort.Strings(lease.TrunkIDs)

	for id, t := range newTrunks {
		c.trunks[id] = t
	}
	c.providers[snap.ProviderID] = lease

	c.rebuildIPIndex()

	// Build event list (deterministic by trunk_id) while still under the
	// lock, but fire them after Unlock to avoid re-entrant lock acquires.
	for _, id := range lease.TrunkIDs {
		if _, wasThere := oldIDs[id]; !wasThere {
			pending = append(pending, pendingEvent{
				method: EventTrunkRegistered,
				payload: map[string]any{
					"trunk_id":    id,
					"provider_id": snap.ProviderID,
					"revision":    snap.Revision,
				},
			})
		}
	}
	for _, id := range droppedIDs {
		pending = append(pending, pendingEvent{
			method: EventTrunkDeregistered,
			payload: map[string]any{
				"trunk_id":    id,
				"provider_id": snap.ProviderID,
				"reason":      ReasonRemoved,
			},
		})
	}
	leaseClone = lease.clone()
	c.mu.Unlock()

	for _, ev := range pending {
		c.emit(ev.method, ev.payload)
	}
	return leaseClone, nil
}

// Heartbeat extends the lease for an existing provider; trunks unchanged.
func (c *Catalog) Heartbeat(providerID string, ttl time.Duration) (*ProviderLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.providers[providerID]
	if !ok {
		return nil, &ProviderNotFoundError{ProviderID: providerID}
	}
	if ttl > 0 {
		lease.LeaseTTL = ttl
	}
	lease.LastHeartbeat = c.now()
	if lease.LeaseTTL > 0 {
		lease.ExpiresAt = lease.LastHeartbeat.Add(lease.LeaseTTL)
	}
	return lease.clone(), nil
}

// RemoveProvider drops all trunks owned by providerID.
// Returns the list of removed trunk_ids (for drain accounting).
//
// reason is one of [ReasonRemoved, ReasonLeaseExpired]; it is attached to
// the emitted telecom.trunk.deregistered event payload so subscribers can
// distinguish admin actions from heartbeat failures. RemoveProvider
// always uses ReasonRemoved (admin DELETE / snapshot drop); the sweeper
// calls removeProviderWithReason for the lease-expired path.
func (c *Catalog) RemoveProvider(providerID string) ([]string, error) {
	return c.removeProviderWithReason(providerID, ReasonRemoved)
}

func (c *Catalog) removeProviderWithReason(providerID, reason string) ([]string, error) {
	c.mu.Lock()
	lease, ok := c.providers[providerID]
	if !ok {
		c.mu.Unlock()
		return nil, &ProviderNotFoundError{ProviderID: providerID}
	}
	removed := append([]string(nil), lease.TrunkIDs...)
	for _, id := range lease.TrunkIDs {
		delete(c.trunks, id)
	}
	delete(c.providers, providerID)
	c.rebuildIPIndex()
	c.mu.Unlock()

	for _, id := range removed {
		c.emit(EventTrunkDeregistered, map[string]any{
			"trunk_id":    id,
			"provider_id": providerID,
			"reason":      reason,
		})
	}
	return removed, nil
}

// LookupByPeer returns the trunk_id whose peer_endpoints best match (ip, port).
// Longest-prefix wins; ties broken by trunk_id lexicographic order.
func (c *Catalog) LookupByPeer(ip string, port int) (trunkID string, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ipIndex.Lookup(ip, port)
}

// GetTrunk returns a copy of the trunk entry by id, or nil if not found.
func (c *Catalog) GetTrunk(trunkID string) *TrunkEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.trunks[trunkID]
	if !ok {
		return nil
	}
	copy := *t
	return &copy
}

// Providers returns a snapshot of all current provider leases.
func (c *Catalog) Providers() []*ProviderLease {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*ProviderLease, 0, len(c.providers))
	for _, l := range c.providers {
		out = append(out, l.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProviderID < out[j].ProviderID })
	return out
}

// Trunks returns a snapshot of all current trunks across all providers.
func (c *Catalog) Trunks() []*TrunkEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*TrunkEntry, 0, len(c.trunks))
	for _, t := range c.trunks {
		copy := *t
		out = append(out, &copy)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// rebuildIPIndex regenerates the IPIndex from current trunks. Must hold c.mu.
func (c *Catalog) rebuildIPIndex() {
	idx := NewIPIndex()
	for _, t := range c.trunks {
		if !t.SupportsInbound() {
			continue
		}
		for _, pe := range t.PeerEndpoints {
			idx.Add(pe.IP, pe.Port, t.ID)
		}
	}
	c.ipIndex = idx
}

func validateSnapshot(snap *Snapshot) error {
	if strings.TrimSpace(snap.ProviderID) == "" {
		return fmt.Errorf("provider_id required")
	}
	seen := make(map[string]struct{}, len(snap.Trunks))
	for i := range snap.Trunks {
		t := &snap.Trunks[i]
		if strings.TrimSpace(t.ID) == "" {
			return fmt.Errorf("trunks[%d].id required", i)
		}
		if _, dup := seen[t.ID]; dup {
			return fmt.Errorf("duplicate trunk_id %q in snapshot", t.ID)
		}
		seen[t.ID] = struct{}{}
		if len(t.TrunkTypes) == 0 {
			return fmt.Errorf("trunks[%d].trunk_types required", i)
		}
		if t.SupportsInbound() && len(t.PeerEndpoints) == 0 {
			return fmt.Errorf("trunks[%d] inbound requires peer_endpoints", i)
		}
		if t.SupportsOutbound() && !t.Outbound.Ready() {
			return fmt.Errorf("trunks[%d] outbound requires outbound.server", i)
		}
	}
	return nil
}
