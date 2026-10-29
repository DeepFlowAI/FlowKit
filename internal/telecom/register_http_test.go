package telecom

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) (*httptest.Server, *Catalog, *RegisterServer) {
	t.Helper()
	c := newTestCatalog()
	rs := NewRegisterServer(c, RegisterServerOptions{APIKey: "secret-key"})
	mux := http.NewServeMux()
	rs.Mount(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, c, rs
}

func doRequest(t *testing.T, ts *httptest.Server, method, path, apiKey, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestRegisterHTTP_UnauthorizedWithoutKey(t *testing.T) {
	ts, _, _ := newTestServer(t)
	resp := doRequest(t, ts, http.MethodPut,
		"/api/v1/telecom/registrars/p1/snapshot",
		"", // no key
		`{"trunks": []}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", resp.StatusCode)
	}
}

func TestRegisterHTTP_SnapshotHappyPath(t *testing.T) {
	ts, c, _ := newTestServer(t)
	body := `{
		"revision": 1,
		"lease_ttl_sec": 90,
		"trunks": [
			{
				"id": "p1_in",
				"trunk_types": ["inbound"],
				"peer_endpoints": [{"ip": "203.0.113.10", "port": 5060}]
			}
		]
	}`
	resp := doRequest(t, ts, http.MethodPut,
		"/api/v1/telecom/registrars/p1/snapshot",
		"secret-key", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var out snapshotResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.ProviderID != "p1" || out.Revision != 1 || out.TrunkCount != 1 {
		t.Errorf("response: %+v", out)
	}
	if id, ok := c.LookupByPeer("203.0.113.10", 5060); !ok || id != "p1_in" {
		t.Errorf("catalog lookup after snapshot: id=%q ok=%v", id, ok)
	}
}

func TestRegisterHTTP_StaleRevision(t *testing.T) {
	ts, _, _ := newTestServer(t)
	body := `{"revision": 5, "trunks": [{"id":"a","trunk_types":["inbound"],"peer_endpoints":[{"ip":"1.1.1.1"}]}]}`
	resp := doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first apply: %d", resp.StatusCode)
	}
	older := `{"revision": 3, "trunks": [{"id":"a","trunk_types":["inbound"],"peer_endpoints":[{"ip":"1.1.1.1"}]}]}`
	resp = doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", older)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("stale: got %d want 409", resp.StatusCode)
	}
	var er errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&er)
	if er.Error != "stale_revision" {
		t.Errorf("error code: %s", er.Error)
	}
}

func TestRegisterHTTP_UnknownField(t *testing.T) {
	ts, _, _ := newTestServer(t)
	body := `{"phone_numbers": ["+86..."], "trunks": []}`
	resp := doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", resp.StatusCode)
	}
	var er errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&er)
	if er.Error != "unknown_field" {
		t.Errorf("error code: got %q want unknown_field", er.Error)
	}
}

func TestRegisterHTTP_LeaseTTLBounds(t *testing.T) {
	ts, _, _ := newTestServer(t)
	for _, ttl := range []int{10, 400} {
		body := `{"lease_ttl_sec": ` + itoaTest(ttl) + `, "trunks": []}`
		resp := doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("ttl=%d: got %d want 400", ttl, resp.StatusCode)
		}
	}
}

func TestRegisterHTTP_SystemProviderReserved(t *testing.T) {
	ts, _, _ := newTestServer(t)
	resp := doRequest(t, ts, http.MethodPut,
		"/api/v1/telecom/registrars/system/snapshot",
		"secret-key", `{"trunks":[]}`)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("system reserved: got %d want 409", resp.StatusCode)
	}
}

func TestRegisterHTTP_Heartbeat(t *testing.T) {
	ts, _, _ := newTestServer(t)
	body := `{"trunks": [{"id":"a","trunk_types":["inbound"],"peer_endpoints":[{"ip":"1.1.1.1"}]}]}`
	if resp := doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("setup: %d", resp.StatusCode)
	}
	hb := doRequest(t, ts, http.MethodPost, "/api/v1/telecom/registrars/p1/heartbeat", "secret-key", `{}`)
	if hb.StatusCode != http.StatusOK {
		t.Errorf("heartbeat: got %d want 200", hb.StatusCode)
	}
	// Unknown provider → 404
	hb2 := doRequest(t, ts, http.MethodPost, "/api/v1/telecom/registrars/nope/heartbeat", "secret-key", `{}`)
	if hb2.StatusCode != http.StatusNotFound {
		t.Errorf("heartbeat unknown: got %d want 404", hb2.StatusCode)
	}
}

func TestRegisterHTTP_Delete(t *testing.T) {
	ts, c, _ := newTestServer(t)
	body := `{"trunks": [{"id":"a","trunk_types":["inbound"],"peer_endpoints":[{"ip":"1.1.1.1"}]}]}`
	_ = doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", body)
	resp := doRequest(t, ts, http.MethodDelete, "/api/v1/telecom/registrars/p1", "secret-key", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete: got %d want 204", resp.StatusCode)
	}
	if len(c.Trunks()) != 0 {
		t.Errorf("catalog should be empty after DELETE, got %d trunks", len(c.Trunks()))
	}
}

func TestRegisterHTTP_GetCatalog_RedactsPasswords(t *testing.T) {
	ts, _, _ := newTestServer(t)
	body := `{"trunks": [{
		"id":"a",
		"trunk_types":["outbound"],
		"outbound": {"server":"sip.example.com","port":5060,"user":"u","pass":"supersecret"}
	}]}`
	_ = doRequest(t, ts, http.MethodPut, "/api/v1/telecom/registrars/p1/snapshot", "secret-key", body)

	resp := doRequest(t, ts, http.MethodGet, "/api/v1/telecom/catalog", "secret-key", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog: %d", resp.StatusCode)
	}
	raw, _ := readAllBody(resp)
	if strings.Contains(raw, "supersecret") {
		t.Errorf("catalog response leaked password: %s", raw)
	}
	if !strings.Contains(raw, `"pass":"***"`) {
		t.Errorf("catalog response should redact pass: %s", raw)
	}
}

// --- tiny helpers (avoid pulling fmt / strconv into a test-only file) ---

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func readAllBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 512)
	for {
		n, err := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			if err.Error() == "EOF" {
				return string(buf), nil
			}
			return string(buf), nil
		}
	}
}
