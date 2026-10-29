package rpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/config"
	"github.com/gorilla/websocket"
)

// Server is a JSON-RPC 2.0 server that accepts connections over WebSocket and
// optionally Unix Socket. It is constructed via New and run via Start.
type Server struct {
	cfg     config.RPCConfig
	handler Handler
	log     *slog.Logger

	upgrader websocket.Upgrader

	// mu guards httpServer / httpListener / unixListener.
	mu           sync.RWMutex
	httpServer   *http.Server
	httpListener net.Listener
	unixListener net.Listener

	conns sync.Map // connID → *Conn

	startedOnce sync.Once
	stopOnce    sync.Once
	readyOnce   sync.Once
	stopped     chan struct{}
	ready       chan struct{} // closed after listeners are bound (or after Start returns)

	// Optional production-hardening hooks (set via setters; nil = noop).
	// drainCheck reports whether the kernel is in drain mode (M4).
	drainCheck   func() bool
	instanceID   string // "X-Flowkit-Instance" header value
	startedAt    time.Time
	callCounter  func() (active, total int)
	extraRoutes  []extraRoute // registered before Start
}

// extraRoute carries a path prefix and the handler that should serve it on
// the same HTTP mux as /ws + /health.
type extraRoute struct {
	pattern string
	handler http.Handler
}

// RegisterHTTP mounts an extra handler under the given path pattern on the
// same HTTP mux used by /ws and /health. Must be called BEFORE Start.
//
// Use case: telecom register API (PUT /api/v1/telecom/registrars/...) lives
// next to the WS endpoint so a single port serves both.
func (s *Server) RegisterHTTP(pattern string, handler http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extraRoutes = append(s.extraRoutes, extraRoute{pattern: pattern, handler: handler})
}

// signalReady closes the ready channel exactly once.
func (s *Server) signalReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

// New constructs a Server. Pass a Handler implementation (task ⑤ provides the
// real one; tests use HandlerFunc).
func New(cfg config.RPCConfig, handler Handler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if handler == nil {
		// Sane default so a misconfigured server still rejects gracefully.
		handler = HandlerFunc{}
	}
	return &Server{
		cfg:     cfg,
		handler: handler,
		log:     log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  readBufSize,
			WriteBufferSize: writeBufSize,
			// M1: accept all origins (tighten in M4 via config).
			CheckOrigin: func(*http.Request) bool { return true },
		},
		stopped:   make(chan struct{}),
		ready:     make(chan struct{}),
		startedAt: time.Now(),
	}
}

// SetDrainCheck registers a probe used by the /health endpoint to decide
// between 200 OK and 503 draining. Wire this from cmd/flowkit:
//
//	rpcServer.SetDrainCheck(mgr.IsDraining)
func (s *Server) SetDrainCheck(fn func() bool) {
	s.drainCheck = fn
}

// SetInstanceID configures the X-Flowkit-Instance header value applied to
// /health and /metrics responses.
func (s *Server) SetInstanceID(id string) {
	s.instanceID = id
}

// SetCallCounter wires a callback that returns (active, total) counts —
// used by /metrics. Pass session.Manager.Count for active; total can be
// tracked separately by the kernel.
func (s *Server) SetCallCounter(fn func() (active, total int)) {
	s.callCounter = fn
}

// Ready returns a channel that is closed after listeners are bound (or after
// bind fails — in which case Start returns the error).
func (s *Server) Ready() <-chan struct{} { return s.ready }

// Addr returns the actual bound TCP address (useful after Listen on :0).
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.httpListener == nil {
		return ""
	}
	return s.httpListener.Addr().String()
}

// UnixAddr returns the unix socket path or "" if unix is disabled.
func (s *Server) UnixAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unixListener == nil {
		return ""
	}
	return s.unixListener.Addr().String()
}

// ConnCount returns the current number of live connections.
func (s *Server) ConnCount() int {
	n := 0
	s.conns.Range(func(_, _ any) bool { n++; return true })
	return n
}

// Start binds the listeners and serves until Stop or ctx cancellation.
// It returns nil after a graceful Stop, or an error if startup failed.
func (s *Server) Start(ctx context.Context) error {
	var startErr error
	s.startedOnce.Do(func() {
		startErr = s.start(ctx)
		// Always signal ready so test code waiting on <-Ready() doesn't block
		// forever if bind fails.
		s.signalReady()
	})
	if startErr != nil {
		return startErr
	}
	// Block until ctx canceled or Stop called.
	select {
	case <-ctx.Done():
		_ = s.Stop(context.Background())
		return nil
	case <-s.stopped:
		return nil
	}
}

func (s *Server) start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/ws", s.RequireAuth(http.HandlerFunc(s.handleWS)))
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	s.mu.RLock()
	extras := append([]extraRoute(nil), s.extraRoutes...)
	s.mu.RUnlock()
	for _, route := range extras {
		mux.Handle(route.pattern, route.handler)
	}

	var httpListener, unixListener net.Listener
	var httpServer *http.Server

	if s.cfg.Listen != "" {
		ln, err := net.Listen("tcp", s.cfg.Listen)
		if err != nil {
			return fmt.Errorf("rpc: listen tcp %s: %w", s.cfg.Listen, err)
		}
		httpListener = ln
		httpServer = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		s.log.Info("rpc tcp listening", "addr", ln.Addr().String())
	}

	if s.cfg.Unix != "" {
		_ = os.Remove(s.cfg.Unix)
		ul, err := net.Listen("unix", s.cfg.Unix)
		if err != nil {
			if httpListener != nil {
				_ = httpListener.Close()
			}
			return fmt.Errorf("rpc: listen unix %s: %w", s.cfg.Unix, err)
		}
		unixListener = ul
		s.log.Info("rpc unix listening", "path", ul.Addr().String())
	}

	if httpListener == nil && unixListener == nil {
		return errors.New("rpc: at least one of rpc.listen / rpc.unix is required")
	}

	// Publish fields under lock so concurrent Addr()/Stop() see them.
	s.mu.Lock()
	s.httpListener = httpListener
	s.httpServer = httpServer
	s.unixListener = unixListener
	s.mu.Unlock()

	// Signal listeners are bound BEFORE spawning serving goroutines so tests
	// can call Addr() safely (happens-before via channel close).
	s.signalReady()

	if httpServer != nil {
		go func() {
			if err := httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("http server stopped", "err", err)
			}
		}()
	}
	if unixListener != nil {
		go s.acceptUnix(ctx, unixListener)
	}
	return nil
}

// RequireAuth wraps h so that, when rpc.api_key is configured, requests
// without a matching "Authorization: Bearer <key>" header get 401.
func (s *Server) RequireAuth(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			s.log.Warn("rpc auth rejected", "path", r.URL.Path, "remote", r.RemoteAddr)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) authorized(r *http.Request) bool {
	if s.cfg.APIKey == "" {
		return true
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		// Browsers cannot set headers on a WebSocket handshake, so they pass the
		// key in the query string instead. Proxies may log it; prefer the header.
		if r.Header.Get("Authorization") != "" {
			return false
		}
		token = r.URL.Query().Get("api_key")
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(token)), []byte(s.cfg.APIKey)) == 1
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("ws upgrade failed", "err", err)
		return
	}
	c := newWSConn(s, ws)
	s.conns.Store(c.id, c)
	c.log.Debug("ws connected")

	ctx := context.Background()
	go c.run(ctx)
}

func (s *Server) acceptUnix(ctx context.Context, ln net.Listener) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopped:
			return
		default:
		}
		conn, err := ln.Accept()
		if err != nil {
			if isClosedListener(err) {
				return
			}
			s.log.Warn("unix accept error", "err", err)
			continue
		}
		c := newUnixConn(s, conn)
		s.conns.Store(c.id, c)
		c.log.Debug("unix connected")
		go c.run(ctx)
	}
}

func isClosedListener(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

// removeConn is called by Conn.run when a connection terminates.
func (s *Server) removeConn(id string) {
	s.conns.Delete(id)
}

// Stop performs a graceful shutdown. Listeners stop accepting first; existing
// connections are closed; the function returns when all goroutines exit or
// the provided context is cancelled.
func (s *Server) Stop(ctx context.Context) error {
	var err error
	s.stopOnce.Do(func() {
		close(s.stopped)
		s.mu.RLock()
		httpServer := s.httpServer
		unixListener := s.unixListener
		s.mu.RUnlock()

		if httpServer != nil {
			if e := httpServer.Shutdown(ctx); e != nil {
				err = e
			}
		}
		if unixListener != nil {
			_ = unixListener.Close()
		}
		s.conns.Range(func(_, v any) bool {
			if conn, ok := v.(*Conn); ok {
				_ = conn.Close()
			}
			return true
		})
	})
	return err
}

// BroadcastNotification sends the same notification to every live connection.
func (s *Server) BroadcastNotification(n *Notification) {
	if n == nil {
		return
	}
	s.conns.Range(func(_, v any) bool {
		if c, ok := v.(*Conn); ok {
			_ = c.SendNotification(n)
		}
		return true
	})
}
