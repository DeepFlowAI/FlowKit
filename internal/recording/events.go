package recording

import (
	"encoding/json"
	"log/slog"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
)

// Notifier broadcasts recording lifecycle events to SDK clients.
type Notifier interface {
	Notify(method, callID string, params map[string]any)
}

// RPCNotifier adapts rpc.Server (or any Broadcaster) to Notifier.
type RPCNotifier struct {
	Broadcast func(*rpc.Notification)
	Log       *slog.Logger
}

// Notify implements Notifier.
func (n RPCNotifier) Notify(method, callID string, params map[string]any) {
	if n.Broadcast == nil {
		return
	}
	out := map[string]any{"call_id": callID}
	for k, v := range params {
		out[k] = v
	}
	raw, err := json.Marshal(out)
	if err != nil {
		if n.Log != nil {
			n.Log.Debug("recording notify marshal failed", "method", method, "err", err)
		}
		return
	}
	ntf := &rpc.Notification{JSONRPC: "2.0", Method: method, Params: raw}
	n.Broadcast(ntf)
}
