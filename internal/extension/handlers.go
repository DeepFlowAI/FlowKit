package extension

import (
	"context"
	"encoding/json"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
)

// RegistrarInfo describes the SIP registrar address a phone should REGISTER to.
// Empty fields mean "not configured" and are rendered as JSON null.
type RegistrarInfo struct {
	Host      string
	Port      int
	Transport string
}

// RegisterRPC wires the extension.* and system.registrar_info methods into the
// registry. info supplies the static registrar address returned by
// system.registrar_info.
func (r *Registry) RegisterRPC(reg *rpc.Registry, info RegistrarInfo) {
	reg.Register("extension.query", r.handleQuery)
	reg.Register("extension.list", r.handleList)
	reg.Register("extension.dump", r.handleDump)
	reg.Register("extension.load", r.handleLoad)
	reg.Register("system.registrar_info", func(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
		return r.handleRegistrarInfo(info), nil
	})
}

type queryParams struct {
	Extension string `json:"extension"`
}

func (r *Registry) handleQuery(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p queryParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Extension == "" {
		return nil, rpc.ErrInvalidParams("missing extension")
	}
	contact, ok := r.Lookup(p.Extension)
	if !ok {
		return map[string]any{
			"extension":  p.Extension,
			"registered": false,
		}, nil
	}
	return map[string]any{
		"extension":  p.Extension,
		"registered": true,
		"contact":    contact.String(),
	}, nil
}

func (r *Registry) handleList(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	return map[string]any{"extensions": r.List()}, nil
}

// handleDump returns the same live binding set as extension.list. Kept as a
// distinct method so blue/green cutover scripts can call an intent-named RPC.
func (r *Registry) handleDump(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	return map[string]any{"extensions": r.List()}, nil
}

type loadParams struct {
	Extensions []BindingView `json:"extensions"`
}

func (r *Registry) handleLoad(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p loadParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	result := r.Restore(p.Extensions)
	return map[string]any{
		"loaded":  result.Loaded,
		"skipped": result.Skipped,
	}, nil
}

func (r *Registry) handleRegistrarInfo(info RegistrarInfo) any {
	var host any
	if info.Host != "" {
		host = info.Host
	}
	var port any
	if info.Port > 0 {
		port = info.Port
	}
	var transport any
	if info.Transport != "" {
		transport = info.Transport
	}
	return map[string]any{
		"host":      host,
		"port":      port,
		"transport": transport,
	}
}
