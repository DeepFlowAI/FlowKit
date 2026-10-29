// Package sip wraps the emiago/sipgo UserAgent + Server + Client for
// flowkit's SIP transport. M3 scope: UDP only, PCMU only, no auth backend
// (REGISTER accepts anything).
package sip

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"

	"github.com/DeepFlowAI/flowkit/internal/extension"
)

// Config controls UA startup. ExtIP="auto" makes the UA detect a non-loopback
// IPv4 address; otherwise the value is advertised verbatim in Contact / SDP.
type Config struct {
	Listen    string // e.g. "0.0.0.0:5060"; empty disables SIP
	AltListen string // optional second UDP bind, e.g. "0.0.0.0:5070"; empty disables
	ExtIP     string // "auto" | explicit IP
	UserAgent string // value of the User-Agent header
	Hostname  string // hostname for Contact URIs

	// ContactPort, when non-zero, overrides the port advertised in Contact
	// headers (INVITE/re-INVITE). Needed when a front-end (e.g. Kamailio)
	// owns the public SIP port and this UA listens on a different local port.
	// Zero (the default) derives the Contact port from Listen.
	ContactPort int

	// RTP port range from which per-call sockets are allocated.
	RTPPortMin int
	RTPPortMax int

	Logger *slog.Logger
}

// IncomingCall is what OnIncomingCall receives. Real INVITE handling lives
// in inbound.go; this type just carries the inbound context so the wrapper
// stays small.
type IncomingCall struct {
	Request     *sip.Request
	Transaction sip.ServerTransaction
}

// IncomingHandler is the user-supplied callback for inbound INVITEs.
type IncomingHandler func(IncomingCall)

// ByeHandler is invoked before the default 200 OK reply is sent so the
// caller can locate and tear down the matching Call.
type ByeHandler func(req *sip.Request)

// AckHandler is invoked when an ACK arrives confirming a 200 OK answer.
type AckHandler func(req *sip.Request, tx sip.ServerTransaction)

// CancelHandler is invoked when a CANCEL arrives for an in-flight INVITE.
type CancelHandler func(req *sip.Request)

// UA is flowkit's wrapper around sipgo. One UA per process.
type UA struct {
	cfg Config
	log *slog.Logger

	ua     *sipgo.UserAgent
	srv    *sipgo.Server
	client *sipgo.Client

	mu              sync.Mutex
	incomingHandler IncomingHandler
	byeHandler      ByeHandler
	ackHandler      AckHandler
	cancelHandler   CancelHandler
	extReg          *extension.Registry // optional; nil = REGISTER not recorded
	listener        net.PacketConn
	altListener     net.PacketConn // optional second UDP bind (ALG-bypass port)

	startOnce sync.Once
	stopOnce  sync.Once
	stopped   atomic.Bool
	// shutdown is closed by Stop so Start returns without waiting for the
	// UDP read loop (sipgo ServeUDP) to unwind — that loop is owned by the
	// PacketConn close, not by the Start goroutine.
	shutdown chan struct{}
}

// New constructs a UA but does not yet bind. Call Start to listen.
func New(cfg Config) (*UA, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "flowkit"
	}
	hostname := cfg.Hostname
	if hostname == "" {
		hostname = "flowkit.local"
	}
	if cfg.ExtIP == "" || cfg.ExtIP == "auto" {
		ip, err := detectExtIP()
		if err != nil {
			return nil, fmt.Errorf("sip: auto-detect ExtIP: %w", err)
		}
		cfg.ExtIP = ip
	}
	if cfg.RTPPortMin == 0 {
		cfg.RTPPortMin = 16384
	}
	if cfg.RTPPortMax == 0 {
		cfg.RTPPortMax = 32767
	}

	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent(cfg.UserAgent),
		sipgo.WithUserAgentHostname(hostname),
	)
	if err != nil {
		return nil, fmt.Errorf("sip: new ua: %w", err)
	}
	srv, err := sipgo.NewServer(ua)
	if err != nil {
		return nil, fmt.Errorf("sip: new server: %w", err)
	}
	// Advertise the external IP in the outbound Via (not the 0.0.0.0 bind
	// address) so gateways can route responses back, and enable rport so a
	// NAT'd response returns to the observed source port. Without this,
	// outbound INVITEs carry "Via: SIP/2.0/UDP 0.0.0.0:..." and strict carriers
	// silently drop them (no 100/4xx), so the call never rings.
	client, err := sipgo.NewClient(
		ua,
		sipgo.WithClientHostname(cfg.ExtIP),
		sipgo.WithClientNAT(),
	)
	if err != nil {
		return nil, fmt.Errorf("sip: new client: %w", err)
	}

	u := &UA{
		cfg:      cfg,
		log:      cfg.Logger.With("component", "sip"),
		ua:       ua,
		srv:      srv,
		client:   client,
		shutdown: make(chan struct{}),
	}

	// Register default handlers that we always provide. Custom INVITE
	// handling fans out through u.incomingHandler when set.
	srv.OnOptions(u.handleOptions)
	srv.OnRegister(u.handleRegister)
	srv.OnInvite(u.handleInvite)
	srv.OnBye(u.handleBye)
	srv.OnAck(u.handleAck)
	srv.OnCancel(u.handleCancel)

	return u, nil
}

// ExtIP returns the external IPv4 address used in Contact / SDP.
func (u *UA) ExtIP() string { return u.cfg.ExtIP }

// RTPPortRange returns the configured min/max ports for per-call media.
func (u *UA) RTPPortRange() (int, int) { return u.cfg.RTPPortMin, u.cfg.RTPPortMax }

// Client exposes the sipgo client for outbound originate.
func (u *UA) Client() *sipgo.Client { return u.client }

// OnIncomingCall registers the handler invoked for every inbound INVITE.
// Pass nil to clear. Only one handler is supported.
func (u *UA) OnIncomingCall(h IncomingHandler) {
	u.mu.Lock()
	u.incomingHandler = h
	u.mu.Unlock()
}

// OnBye registers a callback invoked when a BYE arrives. The default
// transaction reply (200 OK) is sent regardless of whether a handler is
// registered.
func (u *UA) OnBye(h ByeHandler) {
	u.mu.Lock()
	u.byeHandler = h
	u.mu.Unlock()
}

// OnAck registers a callback invoked when an ACK arrives.
func (u *UA) OnAck(h AckHandler) {
	u.mu.Lock()
	u.ackHandler = h
	u.mu.Unlock()
}

// OnCancel registers a callback invoked when a CANCEL arrives.
func (u *UA) OnCancel(h CancelHandler) {
	u.mu.Lock()
	u.cancelHandler = h
	u.mu.Unlock()
}

// SetExtensionRegistry wires the trust-network extension registrar. When set,
// REGISTER requests record/refresh/remove the caller's Contact binding so the
// kernel can originate calls to a phone by extension number. Pass nil (the
// default) to leave REGISTER handling unchanged (always 200 OK, nothing stored).
func (u *UA) SetExtensionRegistry(reg *extension.Registry) {
	u.mu.Lock()
	u.extReg = reg
	u.mu.Unlock()
}

// ExtensionRegistry returns the registrar wired by SetExtensionRegistry, or
// nil when extensions are disabled. Inbound signalling reads it to tell a call
// placed by one of our own phones from one arriving off a carrier trunk.
func (u *UA) ExtensionRegistry() *extension.Registry {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.extReg
}

// LocalAddr returns the actual bound UDP address (useful after Listen on :0).
func (u *UA) LocalAddr() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.listener == nil {
		return ""
	}
	return u.listener.LocalAddr().String()
}

// Start binds the UDP listener(s) and blocks until ctx is cancelled or Stop.
// Ownership: the caller that creates the UA must run Start (typically in a
// goroutine) and ensure Stop (or ctx cancel) on process/test teardown so this
// wait returns. ServeUDP read loops are separate; Stop closes their PacketConns.
func (u *UA) Start(ctx context.Context) error {
	var startErr error
	u.startOnce.Do(func() {
		listenAddr := u.cfg.Listen
		if listenAddr == "" {
			startErr = errors.New("sip: cfg.Listen is empty")
			return
		}
		// We bind manually so we can expose LocalAddr() before ListenAndServe
		// starts blocking; sipgo's Server.ServeUDP accepts an existing socket.
		pc, err := net.ListenPacket("udp", listenAddr)
		if err != nil {
			startErr = fmt.Errorf("sip: listen %s: %w", listenAddr, err)
			return
		}
		u.mu.Lock()
		u.listener = pc
		u.mu.Unlock()
		u.log.Info("sip listening", "addr", pc.LocalAddr().String(), "ext_ip", u.cfg.ExtIP)

		go func() {
			if err := u.srv.ServeUDP(pc); err != nil && !u.stopped.Load() {
				u.log.Error("sip serve exited", "err", err)
			}
		}()

		// Optional second listener on a non-standard port. Consumer routers (e.g.
		// Xiaomi) run a SIP ALG that inspects only port 5060 and rewrites the
		// SDP/Contact of any phone signalling through it, breaking media. Letting a
		// desk phone register to an alternate port sidesteps the ALG entirely.
		// Same sipgo Server → same REGISTER/INVITE/BYE handlers and registrar.
		if alt := u.cfg.AltListen; alt != "" && alt != u.cfg.Listen {
			altPC, altErr := net.ListenPacket("udp", alt)
			if altErr != nil {
				// Non-fatal: the primary listener already serves the trunk.
				u.log.Error("sip alt listen failed", "addr", alt, "err", altErr)
			} else {
				u.mu.Lock()
				u.altListener = altPC
				u.mu.Unlock()
				u.log.Info("sip alt listening", "addr", altPC.LocalAddr().String())
				go func() {
					if err := u.srv.ServeUDP(altPC); err != nil && !u.stopped.Load() {
						u.log.Error("sip alt serve exited", "err", err)
					}
				}()
			}
		}
	})
	if startErr != nil {
		return startErr
	}
	select {
	case <-ctx.Done():
		_ = u.Stop()
	case <-u.shutdown:
	}
	return nil
}

// Stop closes listeners and the sipgo stack, and unblocks Start. Idempotent.
// The creator of the UA is the owner: call Stop on shutdown (main defer / test cleanup).
func (u *UA) Stop() error {
	var err error
	u.stopOnce.Do(func() {
		u.stopped.Store(true)
		close(u.shutdown)
		u.mu.Lock()
		ln := u.listener
		altLn := u.altListener
		u.listener = nil
		u.altListener = nil
		u.mu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		if altLn != nil {
			_ = altLn.Close()
		}
		if u.srv != nil {
			_ = u.srv.Close()
		}
		if u.client != nil {
			_ = u.client.Close()
		}
	})
	return err
}

// --- default handlers (REGISTER/OPTIONS accept; INVITE/BYE/ACK delegate) ---

func (u *UA) handleOptions(req *sip.Request, tx sip.ServerTransaction) {
	resp := sip.NewResponseFromRequest(req, 200, "OK", nil)
	resp.AppendHeader(sip.NewHeader("Allow", "REGISTER, INVITE, ACK, BYE, CANCEL, OPTIONS"))
	_ = tx.Respond(resp)
}

func (u *UA) handleRegister(req *sip.Request, tx sip.ServerTransaction) {
	// Accept any REGISTER (trust-network registrar: no digest challenge). When
	// an extension registry is wired, record the Contact binding so the kernel
	// can originate to a phone by extension number.
	contact := req.Contact()
	// The ip:port the REGISTER actually arrived from. Direct (no proxy): this
	// is the phone's public NAT binding. Behind Kamailio: req.Source() is the
	// proxy (127.0.0.1:5070) — use effectivePeerSource (X-Src-Addr) so the
	// registrar stores the real phone address, not the proxy.
	src := req.Source()
	peerSrc := effectivePeerSource(req)

	// Grant the requested lease, capped by SIP_REGISTER_MAX_EXPIRES (env,
	// seconds; default 120, 0 = no cap). The cap matters because the
	// extension registrar is in-memory: after a kernel restart it is empty
	// until each phone re-REGISTERs, so a short lease bounds that recovery
	// window (a phone can only be rung once it has re-registered). RFC 3261:
	// the value the registrar returns in Expires is authoritative, so the
	// phone adopts this refresh interval — no per-phone config needed. A
	// requested Expires of 0 means the phone is de-registering.
	requested := 3600 * time.Second
	if contact != nil {
		requested = registerExpires(req, contact)
	}
	grantedSecs := int(requested / time.Second)
	if max := registerMaxExpiresSeconds(); max > 0 && grantedSecs > max {
		grantedSecs = max
	}

	aor := ""
	if h := req.To(); h != nil {
		aor = h.Address.User
	}
	contactStr := ""
	if contact != nil {
		contactStr = contact.Address.String()
	}
	recorded := false

	u.mu.Lock()
	reg := u.extReg
	u.mu.Unlock()
	if reg != nil && contact != nil {
		if aor != "" {
			if grantedSecs == 0 {
				reg.Deregister(aor, extension.ReasonRemoved)
			} else {
				// Bind the AOR to the phone's observed public address, not its
				// self-reported (often private) Contact — this is where the
				// kernel sends INVITEs to ring the phone.
				bindContact := contact.Address
				if host, portStr, err := net.SplitHostPort(peerSrc); err == nil {
					if port, err := strconv.Atoi(portStr); err == nil {
						bindContact = sip.Uri{User: aor, Host: host, Port: port}
					}
				}
				reg.Register(aor, bindContact, time.Duration(grantedSecs)*time.Second)
			}
			recorded = true
		}
	}

	u.log.Debug("sip register",
		"aor", aor,
		"src", src,
		"peer", peerSrc,
		"contact", contactStr,
		"requested_expires", int(requested/time.Second),
		"granted_expires", grantedSecs,
		"recorded", recorded,
	)

	resp := sip.NewResponseFromRequest(req, 200, "OK", nil)
	if contact != nil {
		resp.AppendHeader(&sip.ContactHeader{Address: contact.Address})
	}
	resp.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(grantedSecs)))
	// Force the 200 OK back to the observed source (the NAT pinhole), not the
	// Via sent-by (a private address when the phone omitted rport), so a NAT'd
	// phone actually receives it and registers.
	if src != "" {
		resp.SetDestination(src)
	}
	_ = tx.Respond(resp)
}

// registerExpires resolves the requested binding lifetime from the Expires
// header, falling back to the Contact "expires" param, then a 3600s default.
func registerExpires(req *sip.Request, contact *sip.ContactHeader) time.Duration {
	if h := req.GetHeader("Expires"); h != nil {
		if secs, err := strconv.Atoi(strings.TrimSpace(h.Value())); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	if v, ok := contact.Params.Get("expires"); ok {
		if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return 3600 * time.Second
}

func (u *UA) handleInvite(req *sip.Request, tx sip.ServerTransaction) {
	u.mu.Lock()
	handler := u.incomingHandler
	u.mu.Unlock()
	if handler == nil {
		resp := sip.NewResponseFromRequest(req, 503, "No handler", nil)
		_ = tx.Respond(resp)
		return
	}
	handler(IncomingCall{Request: req, Transaction: tx})
}

func (u *UA) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	resp := sip.NewResponseFromRequest(req, 200, "OK", nil)
	_ = tx.Respond(resp)
	u.log.Debug("bye received", "from", req.From().Address.String())
	u.mu.Lock()
	h := u.byeHandler
	u.mu.Unlock()
	if h != nil {
		h(req)
	}
}

func (u *UA) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	// ACKs travel without a transaction layer reply.
	u.mu.Lock()
	h := u.ackHandler
	u.mu.Unlock()
	if h != nil {
		h(req, tx)
	}
}

func (u *UA) handleCancel(req *sip.Request, tx sip.ServerTransaction) {
	resp := sip.NewResponseFromRequest(req, 200, "OK", nil)
	_ = tx.Respond(resp)
	u.mu.Lock()
	h := u.cancelHandler
	u.mu.Unlock()
	if h != nil {
		h(req)
	}
}

// detectExtIP picks a non-loopback IPv4 address from the local interfaces.
// Used when cfg.ExtIP is empty or "auto".
func detectExtIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipn.IP.To4()
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		return ip.String(), nil
	}
	return "127.0.0.1", nil // last-resort fallback for fully isolated test envs
}

// AllocRTPPort tries to find a free UDP port in the configured range. The
// returned packet conn is bound; callers must Close() it.
//
// Only even ports are handed out. A peer sends its RTCP reports to the odd
// port one above the RTP port it is talking to (RFC 3550 §11), so allocating
// consecutively would give the next call a socket that the previous call's
// peer is already sending to. Those reports parse as RTP — same version bits
// — and the receiver would latch its send address onto the reporter, sending
// that call's audio to a peer that has nothing to do with it. Skipping the
// odd ports leaves them unbound, so the reports land nowhere.
func (u *UA) AllocRTPPort() (net.PacketConn, error) {
	first := u.cfg.RTPPortMin
	if first%2 != 0 {
		first++
	}
	for port := first; port <= u.cfg.RTPPortMax; port += 2 {
		pc, err := net.ListenPacket("udp", net.JoinHostPort("0.0.0.0", strconv.Itoa(port)))
		if err == nil {
			return pc, nil
		}
	}
	return nil, fmt.Errorf("sip: no free RTP port in [%d, %d]", u.cfg.RTPPortMin, u.cfg.RTPPortMax)
}
