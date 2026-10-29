package rpc

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// httpGET resolves the test server's URL and returns (status code, body).
func httpGET(t *testing.T, url string) (int, string, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), resp.Header
}

func TestHealth_Default_200(t *testing.T) {
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	code, body, _ := httpGET(t, "http://"+addr+"/health")
	require.Equal(t, 200, code)
	require.Contains(t, body, `"status":"ok"`)
}

func TestHealth_Draining_503(t *testing.T) {
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	srv.SetDrainCheck(func() bool { return true })

	code, body, _ := httpGET(t, "http://"+addr+"/health")
	require.Equal(t, 503, code)
	require.Contains(t, body, `"status":"draining"`)
}

func TestHealth_InstanceHeader(t *testing.T) {
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	srv.SetInstanceID("flowkit-test-7")
	_, _, hdrs := httpGET(t, "http://"+addr+"/health")
	require.Equal(t, "flowkit-test-7", hdrs.Get("X-Flowkit-Instance"))
}

func TestMetrics_PrometheusFormat(t *testing.T) {
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	srv.SetInstanceID("flowkit-m")
	srv.SetCallCounter(func() (int, int) { return 2, 17 })

	code, body, hdrs := httpGET(t, "http://"+addr+"/metrics")
	require.Equal(t, 200, code)
	require.Contains(t, hdrs.Get("Content-Type"), "text/plain")
	require.Equal(t, "flowkit-m", hdrs.Get("X-Flowkit-Instance"))

	// Required metric names + types
	for _, want := range []string{
		"# TYPE flowkit_uptime_seconds counter",
		"# TYPE flowkit_conns_active gauge",
		"# TYPE flowkit_calls_active gauge",
		"# TYPE flowkit_calls_total counter",
		"# TYPE flowkit_draining gauge",
		"# TYPE flowkit_goroutines gauge",
		"# TYPE flowkit_mem_alloc_bytes gauge",
		"# TYPE flowkit_mem_sys_bytes gauge",
		"# TYPE flowkit_mem_heap_inuse_bytes gauge",
		"# TYPE flowkit_mem_heap_objects gauge",
		"# TYPE flowkit_mem_stack_inuse_bytes gauge",
		`flowkit_build_info{instance_id="flowkit-m"} 1`,
	} {
		require.True(t, strings.Contains(body, want), "metrics body missing %q", want)
	}

	// Values from our counter
	require.Contains(t, body, "flowkit_calls_active 2")
	require.Contains(t, body, "flowkit_calls_total 17")
}

func TestMetrics_DrainingFlag(t *testing.T) {
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	srv.SetDrainCheck(func() bool { return true })

	_, body, _ := httpGET(t, "http://"+addr+"/metrics")
	require.Contains(t, body, "flowkit_draining 1")
}

func TestMetrics_NoCallCounter_ZeroValues(t *testing.T) {
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	_, body, _ := httpGET(t, "http://"+addr+"/metrics")
	require.Contains(t, body, "flowkit_calls_active 0")
	require.Contains(t, body, "flowkit_calls_total 0")
}

func TestHealth_LatencyUnder100ms(t *testing.T) {
	// Drain flip → /health responds 503 within 100ms.
	_, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()
	addr := srv.Addr()

	srv.SetDrainCheck(func() bool { return true })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+addr+"/health", nil)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 503, resp.StatusCode)
	require.Less(t, elapsed, 100*time.Millisecond, "/health should respond fast under drain (%v)", elapsed)
}
