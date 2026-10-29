package rpc

import (
	"context"
	"encoding/json"
	"time"
)

// ServerInfo identifies this Flowkit instance to clients during system.hello.
type ServerInfo struct {
	Name        string     // "flowkit"
	Version     string     // semver
	NodeID      string     // from config (node.id) — logical cluster member
	InstanceID  string     // process identity (hostname or INSTANCE_ID env)
	InstanceURL string     // direct URL for skipping LB (optional)
	ICEServers  []string   // STUN/TURN URLs for browser WebRTC clients
	SIP         *SIPStatus // optional SIP UA summary for browser harnesses
}

// SIPStatus summarizes SIP trunk/listen config exposed to clients.
type SIPStatus struct {
	Enabled        bool   `json:"enabled"`
	Listen         string `json:"listen,omitempty"`
	DID            string `json:"did,omitempty"`
	ExtIP          string `json:"ext_ip,omitempty"`
	InboundMode    string `json:"inbound_mode,omitempty"`     // optional client default hint (notify|bridge|llm); not enforced server-side
	KernelAutoLLM  bool   `json:"kernel_auto_llm,omitempty"` // kernel InboundLLMAgent; independent of inbound_mode
}

// HelloParams is sent by the client as the first message after WS upgrade.
type HelloParams struct {
	SDK          string   `json:"sdk"`          // e.g. "flowkit-sdk-py"
	Version      string   `json:"version"`      // sdk version
	Capabilities []string `json:"capabilities"` // optional feature flags
}

// HelloResult is returned to the client on a successful hello.
type HelloResult struct {
	Server      string     `json:"server"`
	Version     string     `json:"version"`
	NodeID      string     `json:"node_id"`
	InstanceID  string     `json:"instance_id,omitempty"`
	InstanceURL string     `json:"instance_url,omitempty"`
	ICEServers  []string   `json:"ice_servers,omitempty"`
	SIP         *SIPStatus `json:"sip,omitempty"`
	Time        time.Time  `json:"time"`
}

// ShutdownResult is sent in response to system.shutdown.
type ShutdownResult struct {
	OK bool `json:"ok"`
}

// HealthParams is broadcast periodically as a notification (no response).
type HealthParams struct {
	Calls   int     `json:"calls"`              // active call count
	Conns   int     `json:"conns"`              // active rpc connections
	CPU     float64 `json:"cpu,omitempty"`      // optional, 0-1
	MemMB   uint64  `json:"mem_mb,omitempty"`   // optional, RSS in MiB
	UptimeS uint64  `json:"uptime_s,omitempty"` // seconds since process start
}

// ShutdownHook is invoked by system.shutdown after the response is sent. It
// is the wiring layer's job to actually terminate the process.
type ShutdownHook func()

// RegisterSystemMethods wires the protocol-mandated system.* methods into reg.
//
//	system.hello     — client capability exchange; gates further requests
//	system.shutdown  — graceful stop request (calls hook asynchronously)
func RegisterSystemMethods(reg *Registry, info ServerInfo, onShutdown ShutdownHook) {
	reg.Register("system.hello", func(_ context.Context, conn *Conn, params json.RawMessage) (any, *Error) {
		var p HelloParams
		if rpcErr := DecodeParams(params, &p); rpcErr != nil {
			return nil, rpcErr
		}
		conn.MarkHelloOK()
		return HelloResult{
			Server:      info.Name,
			Version:     info.Version,
			NodeID:      info.NodeID,
			InstanceID:  info.InstanceID,
			InstanceURL: info.InstanceURL,
			ICEServers:  info.ICEServers,
			SIP:         info.SIP,
			Time:        time.Now().UTC(),
		}, nil
	})

	reg.Register("system.shutdown", func(_ context.Context, _ *Conn, _ json.RawMessage) (any, *Error) {
		if onShutdown != nil {
			// Run hook asynchronously so the response can fly first.
			go onShutdown()
		}
		return ShutdownResult{OK: true}, nil
	})
}

// HealthBroadcaster periodically sends `system.health` notifications to every
// connection on the given server. It runs until ctx is cancelled.
type HealthBroadcaster struct {
	srv      *Server
	interval time.Duration
	stats    func() HealthParams
}

// NewHealthBroadcaster creates a broadcaster. stats() is called once per tick
// to produce the payload (typically returns process metrics + call count).
func NewHealthBroadcaster(srv *Server, interval time.Duration, stats func() HealthParams) *HealthBroadcaster {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &HealthBroadcaster{
		srv:      srv,
		interval: interval,
		stats:    stats,
	}
}

// Run blocks until ctx is cancelled, emitting one notification per interval.
func (h *HealthBroadcaster) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			payload := HealthParams{Conns: h.srv.ConnCount()}
			if h.stats != nil {
				payload = h.stats()
				if payload.Conns == 0 {
					payload.Conns = h.srv.ConnCount()
				}
			}
			n, err := NewNotification("system.health", payload)
			if err != nil {
				continue
			}
			h.srv.BroadcastNotification(n)
		}
	}
}
