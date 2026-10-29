package telecom

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RegisterServer mounts the HTTP API used by external Providers (CC clients)
// to register / heartbeat / delete their owned Trunks.
//
// All routes live under /api/v1/telecom/. Auth: every request must include
// the configured X-API-Key header.
type RegisterServer struct {
	catalog       *Catalog
	apiKey        string
	leaseMinSec   int
	leaseMaxSec   int
	defaultTTLSec int
}

// RegisterServerOptions configures the HTTP API.
type RegisterServerOptions struct {
	APIKey         string // required; empty → all requests rejected
	LeaseMinSec    int    // default 30
	LeaseMaxSec    int    // default 300
	DefaultTTLSec  int    // default 90 (used when snapshot omits lease_ttl_sec)
}

// NewRegisterServer wires a register API onto the given Catalog.
func NewRegisterServer(c *Catalog, opts RegisterServerOptions) *RegisterServer {
	min := opts.LeaseMinSec
	if min <= 0 {
		min = 30
	}
	max := opts.LeaseMaxSec
	if max <= 0 {
		max = 300
	}
	def := opts.DefaultTTLSec
	if def <= 0 {
		def = 90
	}
	return &RegisterServer{
		catalog:       c,
		apiKey:        opts.APIKey,
		leaseMinSec:   min,
		leaseMaxSec:   max,
		defaultTTLSec: def,
	}
}

// Mount registers the routes on mux under the /api/v1/telecom/ prefix.
func (s *RegisterServer) Mount(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/telecom/registrars/", s.routeProvider)
	mux.HandleFunc("/api/v1/telecom/catalog", s.handleCatalog)
}

// HTTP wire types.

type snapshotRequest struct {
	Revision           int64          `json:"revision,omitempty"`
	LeaseTTLSec        int            `json:"lease_ttl_sec,omitempty"`
	StaleAcceptableSec int            `json:"stale_acceptable_sec,omitempty"`
	Trunks             []trunkRequest `json:"trunks"`
}

type trunkRequest struct {
	ID            string             `json:"id"`
	TrunkTypes    []string           `json:"trunk_types"`
	Status        string             `json:"status,omitempty"`
	PeerEndpoints []peerEndpointReq  `json:"peer_endpoints,omitempty"`
	Outbound      *outboundProfileReq `json:"outbound,omitempty"`
}

type peerEndpointReq struct {
	IP   string `json:"ip"`
	Port int    `json:"port,omitempty"`
}

type outboundProfileReq struct {
	Server       string `json:"server"`
	Port         int    `json:"port,omitempty"`
	User         string `json:"user,omitempty"`
	Pass         string `json:"pass,omitempty"`
	Realm        string `json:"realm,omitempty"`
	DID          string `json:"did,omitempty"`
	CalleePrefix string `json:"callee_prefix,omitempty"`
}

type heartbeatRequest struct {
	LeaseTTLSec int `json:"lease_ttl_sec,omitempty"`
}

type snapshotResponse struct {
	ProviderID     string    `json:"provider_id"`
	Revision       int64     `json:"revision"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
	StaleUntil     time.Time `json:"stale_until,omitempty"`
	TrunkCount     int       `json:"trunk_count"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// routeProvider dispatches by HTTP method + sub-path under
// /api/v1/telecom/registrars/{provider_id}[/snapshot|/heartbeat].
func (s *RegisterServer) routeProvider(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/telecom/registrars/")
	if rest == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(rest, "/")
	providerID := parts[0]
	if providerID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "missing provider_id")
		return
	}
	if providerID == SystemProviderID {
		writeError(w, http.StatusConflict, "provider_id_conflict",
			"\"system\" is reserved for env-var Provider")
		return
	}
	switch {
	case len(parts) == 1:
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleDelete(w, r, providerID)
	case len(parts) == 2 && parts[1] == "snapshot":
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleSnapshot(w, r, providerID)
	case len(parts) == 2 && parts[1] == "heartbeat":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleHeartbeat(w, r, providerID)
	default:
		http.NotFound(w, r)
	}
}

func (s *RegisterServer) authorized(r *http.Request) bool {
	if strings.TrimSpace(s.apiKey) == "" {
		return false // misconfigured / disabled
	}
	return r.Header.Get("X-API-Key") == s.apiKey
}

func (s *RegisterServer) handleSnapshot(w http.ResponseWriter, r *http.Request, providerID string) {
	body, herr := readJSONBody(r)
	if herr != nil {
		writeError(w, herr.Status, "bad_request", herr.Detail)
		return
	}

	// Strict decode: reject unknown top-level fields so CC notices when it
	// tries to push phone_numbers / tenant_id by mistake.
	var req snapshotRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "unknown_field", err.Error())
		return
	}

	leaseSec := req.LeaseTTLSec
	if leaseSec == 0 {
		leaseSec = s.defaultTTLSec
	}
	if leaseSec < s.leaseMinSec || leaseSec > s.leaseMaxSec {
		writeError(w, http.StatusBadRequest, "bad_request",
			fmt.Sprintf("lease_ttl_sec %d out of bounds [%d,%d]", leaseSec, s.leaseMinSec, s.leaseMaxSec))
		return
	}
	if req.StaleAcceptableSec < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "stale_acceptable_sec must be >= 0")
		return
	}

	snap := Snapshot{
		ProviderID:      providerID,
		Revision:        req.Revision,
		LeaseTTL:        time.Duration(leaseSec) * time.Second,
		StaleAcceptable: time.Duration(req.StaleAcceptableSec) * time.Second,
		Trunks:          make([]TrunkEntry, 0, len(req.Trunks)),
	}
	for _, tr := range req.Trunks {
		entry := TrunkEntry{
			ID:     strings.TrimSpace(tr.ID),
			Status: strings.ToLower(strings.TrimSpace(tr.Status)),
		}
		if entry.Status == "" {
			entry.Status = "enabled"
		}
		for _, tt := range tr.TrunkTypes {
			entry.TrunkTypes = append(entry.TrunkTypes, TrunkType(strings.ToLower(strings.TrimSpace(tt))))
		}
		for _, pe := range tr.PeerEndpoints {
			entry.PeerEndpoints = append(entry.PeerEndpoints, PeerEndpoint{
				IP: strings.TrimSpace(pe.IP), Port: pe.Port,
			})
		}
		if tr.Outbound != nil {
			entry.Outbound = OutboundProfile{
				Server:       strings.TrimSpace(tr.Outbound.Server),
				Port:         tr.Outbound.Port,
				User:         strings.TrimSpace(tr.Outbound.User),
				Pass:         tr.Outbound.Pass,
				Realm:        strings.TrimSpace(tr.Outbound.Realm),
				DID:          strings.TrimSpace(tr.Outbound.DID),
				CalleePrefix: strings.TrimSpace(tr.Outbound.CalleePrefix),
			}
		}
		snap.Trunks = append(snap.Trunks, entry)
	}

	lease, err := s.catalog.ApplySnapshot(snap)
	if err != nil {
		var stale *StaleRevisionError
		var conflict *TrunkIDConflictError
		switch {
		case errors.As(err, &stale):
			writeError(w, http.StatusConflict, "stale_revision", err.Error())
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "trunk_id_conflict", err.Error())
		default:
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		}
		return
	}

	resp := snapshotResponse{
		ProviderID:     providerID,
		Revision:       lease.Revision,
		LeaseExpiresAt: lease.ExpiresAt,
		TrunkCount:     len(lease.TrunkIDs),
	}
	if !lease.ExpiresAt.IsZero() && lease.StaleAcceptable > 0 {
		resp.StaleUntil = lease.ExpiresAt.Add(lease.StaleAcceptable)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *RegisterServer) handleHeartbeat(w http.ResponseWriter, r *http.Request, providerID string) {
	body, herr := readJSONBody(r)
	if herr != nil {
		writeError(w, herr.Status, "bad_request", herr.Detail)
		return
	}
	var req heartbeatRequest
	if len(body) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "unknown_field", err.Error())
			return
		}
	}

	var ttl time.Duration
	if req.LeaseTTLSec > 0 {
		if req.LeaseTTLSec < s.leaseMinSec || req.LeaseTTLSec > s.leaseMaxSec {
			writeError(w, http.StatusBadRequest, "bad_request",
				fmt.Sprintf("lease_ttl_sec %d out of bounds [%d,%d]", req.LeaseTTLSec, s.leaseMinSec, s.leaseMaxSec))
			return
		}
		ttl = time.Duration(req.LeaseTTLSec) * time.Second
	}

	lease, err := s.catalog.Heartbeat(providerID, ttl)
	if err != nil {
		var nf *ProviderNotFoundError
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "provider_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	resp := snapshotResponse{
		ProviderID:     providerID,
		Revision:       lease.Revision,
		LeaseExpiresAt: lease.ExpiresAt,
		TrunkCount:     len(lease.TrunkIDs),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *RegisterServer) handleDelete(w http.ResponseWriter, _ *http.Request, providerID string) {
	if _, err := s.catalog.RemoveProvider(providerID); err != nil {
		var nf *ProviderNotFoundError
		if errors.As(err, &nf) {
			writeError(w, http.StatusNotFound, "provider_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// catalogResponse is the GET /catalog response shape; passwords are redacted.
type catalogResponse struct {
	Providers  []providerView `json:"providers"`
	Trunks     []trunkView    `json:"trunks"`
	TrunkCount int            `json:"trunk_count"`
}

type providerView struct {
	ID             string    `json:"id"`
	Revision       int64     `json:"revision"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
	TrunkCount     int       `json:"trunk_count"`
}

type trunkView struct {
	ID            string            `json:"id"`
	ProviderID    string            `json:"provider_id"`
	TrunkTypes    []string          `json:"trunk_types"`
	Status        string            `json:"status"`
	PeerEndpoints []peerEndpointReq `json:"peer_endpoints,omitempty"`
	Outbound      *outboundView     `json:"outbound,omitempty"`
}

type outboundView struct {
	Server       string `json:"server"`
	Port         int    `json:"port,omitempty"`
	User         string `json:"user,omitempty"`
	Pass         string `json:"pass,omitempty"` // always "***"
	Realm        string `json:"realm,omitempty"`
	DID          string `json:"did,omitempty"`
	CalleePrefix string `json:"callee_prefix,omitempty"`
}

func (s *RegisterServer) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "")
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := catalogResponse{}
	for _, p := range s.catalog.Providers() {
		resp.Providers = append(resp.Providers, providerView{
			ID:             p.ProviderID,
			Revision:       p.Revision,
			LeaseExpiresAt: p.ExpiresAt,
			TrunkCount:     len(p.TrunkIDs),
		})
	}
	for _, t := range s.catalog.Trunks() {
		tv := trunkView{
			ID:         t.ID,
			ProviderID: t.ProviderID,
			Status:     t.Status,
		}
		for _, tt := range t.TrunkTypes {
			tv.TrunkTypes = append(tv.TrunkTypes, string(tt))
		}
		for _, pe := range t.PeerEndpoints {
			tv.PeerEndpoints = append(tv.PeerEndpoints, peerEndpointReq(pe))
		}
		if t.Outbound.Ready() {
			ov := &outboundView{
				Server: t.Outbound.Server, Port: t.Outbound.Port,
				User: t.Outbound.User, Realm: t.Outbound.Realm,
				DID: t.Outbound.DID, CalleePrefix: t.Outbound.CalleePrefix,
			}
			if t.Outbound.Pass != "" {
				ov.Pass = "***"
			}
			tv.Outbound = ov
		}
		resp.Trunks = append(resp.Trunks, tv)
	}
	resp.TrunkCount = len(resp.Trunks)
	writeJSON(w, http.StatusOK, resp)
}

// --- helpers ---

type httpErr struct {
	Status int
	Detail string
}

func readJSONBody(r *http.Request) ([]byte, *httpErr) {
	const limit = 1 << 20 // 1 MiB
	r.Body = http.MaxBytesReader(nil, r.Body, limit)
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(r.Body); err != nil {
		return nil, &httpErr{Status: http.StatusBadRequest, Detail: err.Error()}
	}
	return body.Bytes(), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorResponse{Error: code, Message: msg})
}
