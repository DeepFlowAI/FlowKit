package rpc

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// applyInstanceHeader adds X-Flowkit-Instance to the response if SetInstanceID
// has been called.
func (s *Server) applyInstanceHeader(w http.ResponseWriter) {
	if s.instanceID != "" {
		w.Header().Set("X-Flowkit-Instance", s.instanceID)
	}
}

// handleHealth returns 200 OK with `{"status":"ok"}` normally, or 503 with
// `{"status":"draining"}` once SetDrainCheck() reports the kernel is draining.
// Load balancers can use the status code to stop routing new traffic to a
// pod that's about to terminate.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.applyInstanceHeader(w)
	w.Header().Set("Content-Type", "application/json")
	if s.drainCheck != nil && s.drainCheck() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"draining"}`))
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleMetrics serves a Prometheus text-format snapshot of process state.
// We hand-roll the format so the kernel keeps zero Prometheus dependency —
// scrape targets and OTLP/Grafana stacks both accept this body.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.applyInstanceHeader(w)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	var b strings.Builder

	// Per-process counters
	uptime := time.Since(s.startedAt).Seconds()
	b.WriteString("# HELP flowkit_uptime_seconds Seconds since the kernel started.\n")
	b.WriteString("# TYPE flowkit_uptime_seconds counter\n")
	fmt.Fprintf(&b, "flowkit_uptime_seconds %.3f\n", uptime)

	conns := s.ConnCount()
	b.WriteString("# HELP flowkit_conns_active Active RPC connections.\n")
	b.WriteString("# TYPE flowkit_conns_active gauge\n")
	fmt.Fprintf(&b, "flowkit_conns_active %d\n", conns)

	active, total := 0, 0
	if s.callCounter != nil {
		active, total = s.callCounter()
	}
	b.WriteString("# HELP flowkit_calls_active Currently active calls.\n")
	b.WriteString("# TYPE flowkit_calls_active gauge\n")
	fmt.Fprintf(&b, "flowkit_calls_active %d\n", active)

	b.WriteString("# HELP flowkit_calls_total Calls created since kernel start.\n")
	b.WriteString("# TYPE flowkit_calls_total counter\n")
	fmt.Fprintf(&b, "flowkit_calls_total %d\n", total)

	draining := 0
	if s.drainCheck != nil && s.drainCheck() {
		draining = 1
	}
	b.WriteString("# HELP flowkit_draining 1 if the kernel is in shutdown drain mode.\n")
	b.WriteString("# TYPE flowkit_draining gauge\n")
	fmt.Fprintf(&b, "flowkit_draining %d\n", draining)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	b.WriteString("# HELP flowkit_goroutines Number of live goroutines.\n")
	b.WriteString("# TYPE flowkit_goroutines gauge\n")
	fmt.Fprintf(&b, "flowkit_goroutines %d\n", runtime.NumGoroutine())
	b.WriteString("# HELP flowkit_mem_alloc_bytes Bytes allocated and in use by the Go heap.\n")
	b.WriteString("# TYPE flowkit_mem_alloc_bytes gauge\n")
	fmt.Fprintf(&b, "flowkit_mem_alloc_bytes %d\n", mem.Alloc)
	b.WriteString("# HELP flowkit_mem_sys_bytes Bytes obtained from the OS for the runtime.\n")
	b.WriteString("# TYPE flowkit_mem_sys_bytes gauge\n")
	fmt.Fprintf(&b, "flowkit_mem_sys_bytes %d\n", mem.Sys)
	b.WriteString("# HELP flowkit_mem_heap_inuse_bytes Bytes in heap spans currently in use.\n")
	b.WriteString("# TYPE flowkit_mem_heap_inuse_bytes gauge\n")
	fmt.Fprintf(&b, "flowkit_mem_heap_inuse_bytes %d\n", mem.HeapInuse)
	b.WriteString("# HELP flowkit_mem_heap_objects Number of allocated heap objects.\n")
	b.WriteString("# TYPE flowkit_mem_heap_objects gauge\n")
	fmt.Fprintf(&b, "flowkit_mem_heap_objects %d\n", mem.HeapObjects)
	b.WriteString("# HELP flowkit_mem_stack_inuse_bytes Bytes in stack spans currently in use.\n")
	b.WriteString("# TYPE flowkit_mem_stack_inuse_bytes gauge\n")
	fmt.Fprintf(&b, "flowkit_mem_stack_inuse_bytes %d\n", mem.StackInuse)

	// Info metric (labels carry version + instance_id)
	b.WriteString("# HELP flowkit_build_info Static build / instance information.\n")
	b.WriteString("# TYPE flowkit_build_info gauge\n")
	fmt.Fprintf(&b, "flowkit_build_info{instance_id=%q} 1\n", s.instanceID)

	_, _ = w.Write([]byte(b.String()))
}
