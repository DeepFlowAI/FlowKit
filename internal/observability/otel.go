// Package observability 提供 flowkit 内核的可观测能力——
//
// 核心设计：
//
//  1. 标准 OpenTelemetry SDK，不绑定任何后端。
//     用户配 OTEL_EXPORTER_OTLP_ENDPOINT 就能接到任意 OTLP 后端
//     （Jaeger / Grafana Tempo / Honeycomb / 自建 GreptimeDB 等）。
//
//  2. 完全通过环境变量配置——内核代码不写死任何 endpoint/secret。
//     如果没配 OTEL_EXPORTER_OTLP_ENDPOINT，自动降级为 noop（只打 stderr 日志）。
//
//  3. logs + traces 同时支持。
//     - logs：通过 slog bridge，所有 slog.Info/Error 自动写入 OTel logs
//     - traces：业务代码用 tracer.Start("xxx") 打 span
//     - genai：LLM 调用 span 带 gen_ai.* 属性，符合 OTel GenAI 语义约定
//
// 使用方式（main.go）：
//
//	shutdown, err := observability.Init(ctx, "flowkit-kernel")
//	if err != nil { log.Fatal(err) }
//	defer shutdown(context.Background())
//
//	// 之后整个进程的 slog.Default() / otel.Tracer() 都自动接入了
package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ScopeName 是 logger / tracer 的 scope 名——所有 flowkit 内核日志都用这个，
// 方便在 Grafana 里按 scope_name='flowkit' 过滤。
const ScopeName = "flowkit"

// ShutdownFunc 用于进程退出时刷新缓冲日志/traces。务必 defer 调用。
type ShutdownFunc func(context.Context) error

// Init 初始化 OTel SDK——
//
//   - 设置全局 TracerProvider 和 LoggerProvider
//   - 把 slog.Default() 接到 OTel logs（之后 slog.Info() 自动上报）
//   - 同时保留一份 stderr 输出（本地开发调试用）
//
// 如果环境变量 OTEL_EXPORTER_OTLP_ENDPOINT 没配置，返回一个 noop shutdown，
// 内核仍然能跑（只是 traces 不上报、slog 只输出到 stderr）。
//
// 期望的环境变量（标准 OTel）：
//
//	OTEL_SERVICE_NAME              如 "flowkit-kernel"（也可由参数传）
//	OTEL_EXPORTER_OTLP_ENDPOINT    如 "https://logs.example.com"
//	OTEL_EXPORTER_OTLP_HEADERS     如 "Authorization=Basic xxx"（traces 与 logs 共用）
//	OTEL_EXPORTER_OTLP_PROTOCOL    "http/protobuf"（默认）
//	OTEL_TRACES_PIPELINE_NAME      可选，仅加到 traces endpoint 的 x-greptime-pipeline-name
//	OTEL_LOGS_TABLE_NAME           可选，仅加到 logs endpoint 的 X-Greptime-Log-Table-Name
//
// 后端共用的私有 header（如鉴权）通过 OTEL_EXPORTER_OTLP_HEADERS 配置；
// 仅针对 traces 或 logs 的后端路由 header 用上面两个独立变量，避免互相串到对方端点。
func Init(ctx context.Context, defaultServiceName string) (ShutdownFunc, error) {
	// stderr 始终保留——本地开发不依赖 OTLP 后端就能看到日志。
	// Level 跟随 LOG_LEVEL 环境变量；未设则 info。这样 SetupSlog 设的级别
	// 不会被这里默默覆盖。OTLP bridge 必须共用同一级别，否则
	// otelslog 默认全开 DEBUG，GreptimeDB 会灌满生产 DEBUG。
	logLevel := parseLevel(os.Getenv("LOG_LEVEL"))
	stderrHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// 无 OTLP 后端——只设 stderr 日志，不挂 SDK
		slog.SetDefault(slog.New(stderrHandler))
		slog.Info("observability: OTLP endpoint 未配置，仅启用 stderr 日志（设 OTEL_EXPORTER_OTLP_ENDPOINT 启用 OTLP）")
		return func(context.Context) error { return nil }, nil
	}

	serviceName := envOr("OTEL_SERVICE_NAME", defaultServiceName)
	host, _ := os.Hostname()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(envOr("FLOWKIT_VERSION", "dev")),
			semconv.HostName(host),
			semconv.DeploymentEnvironment(envOr("DEPLOY_ENV", "dev")),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	traceShutdown, err := initTraces(ctx, res, endpoint)
	if err != nil {
		return nil, fmt.Errorf("traces: %w", err)
	}

	logShutdown, err := initLogs(ctx, res, endpoint, stderrHandler, logLevel)
	if err != nil {
		_ = traceShutdown(ctx)
		return nil, fmt.Errorf("logs: %w", err)
	}

	slog.Info("observability initialized",
		"service", serviceName,
		"otlp_endpoint", endpoint,
		"host", host,
	)

	return func(shutdownCtx context.Context) error {
		ctx2, cancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		defer cancel()
		errT := traceShutdown(ctx2)
		errL := logShutdown(ctx2)
		if errT != nil {
			return errT
		}
		return errL
	}, nil
}

// initTraces 创建 TracerProvider 并设为全局。
//
// 路径自动追加 /v1/traces，header 通过 OTEL_EXPORTER_OTLP_HEADERS 传。
func initTraces(ctx context.Context, res *resource.Resource, baseEndpoint string) (ShutdownFunc, error) {
	// Some OTLP backends use a non-standard path prefix (/v1/otlp/v1/traces).
	// Standard OTel SDK defaults append /v1/traces; use EndpointURL when the
	// base endpoint has no /v1/ suffix but the backend expects a custom prefix.
	tracesURL := pickURL(baseEndpoint, "/v1/otlp/v1/traces", "/v1/traces")

	// GreptimeDB needs the pipeline header to flatten span attributes into
	// query-friendly columns. It must go to the traces endpoint ONLY—sending it
	// to logs would make GreptimeDB apply a trace pipeline to logs and reject them.
	// Other backends ignore unknown headers.
	headers := parseHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"))
	if pipeline := envOr("OTEL_TRACES_PIPELINE_NAME", ""); pipeline != "" {
		headers["x-greptime-pipeline-name"] = pipeline
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(tracesURL),
		otlptracehttp.WithHeaders(headers),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // 内核不采样——业务侧再做
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// initLogs 创建 LoggerProvider 并把 slog 桥接过去。
func initLogs(ctx context.Context, res *resource.Resource, baseEndpoint string, alsoStderr slog.Handler, minLevel slog.Level) (ShutdownFunc, error) {
	logsURL := pickURL(baseEndpoint, "/v1/otlp/v1/logs", "/v1/logs")

	// The log table-name header is for the logs endpoint ONLY—it routes log
	// records to the configured table. Other backends ignore unknown headers.
	headers := parseHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"))
	if table := envOr("OTEL_LOGS_TABLE_NAME", ""); table != "" {
		headers["X-Greptime-Log-Table-Name"] = table
	}

	exp, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(logsURL),
		otlploghttp.WithHeaders(headers),
	)
	if err != nil {
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp,
			sdklog.WithExportTimeout(5*time.Second),
		)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	// slog → OTel logs（同时保留 stderr 输出）。
	// otelslog 默认 Enabled 全开；套一层 minLevel，让 OTLP 与 stderr 共用 LOG_LEVEL。
	otelHandler := otelslog.NewHandler(ScopeName,
		otelslog.WithLoggerProvider(lp),
	)
	slog.SetDefault(slog.New(multiHandler{
		min: minLevel,
		a:   otelHandler,
		b:   alsoStderr,
	}))

	return lp.Shutdown, nil
}

// ─── 工具 ───

// pickURL 决定要给 OTLP exporter 的完整 URL：
//   - 如果用户传的就是带路径的完整 URL（含 "/v1/"），直接用它
//   - 否则按 customSuffix 拼（兼容非标准 /v1/otlp/v1/xx 路径）
func pickURL(base, customSuffix, standardSuffix string) string {
	base = strings.TrimRight(base, "/")
	if strings.Contains(base, "/v1/") {
		return base
	}
	suffix := standardSuffix
	// OTEL_KERNEL_ENDPOINT_STYLE=custom 时使用 customSuffix（默认 custom）。
	if endpointStyle := envOr("OTEL_KERNEL_ENDPOINT_STYLE", "custom"); endpointStyle == "custom" || endpointStyle == "legacy" || endpointStyle == "deepflow" {
		suffix = customSuffix
	}
	return base + suffix
}

func parseHeaders(s string) map[string]string {
	out := map[string]string{}
	if s == "" {
		return out
	}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		i := strings.Index(kv, "=")
		if i < 0 {
			continue
		}
		out[strings.TrimSpace(kv[:i])] = strings.TrimSpace(kv[i+1:])
	}
	return out
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

// multiHandler 同时把日志写到 OTel + stderr。
//
// slog 1.21 没有内建的 multi-handler，所以自己实现一个最简版。
// min 是 LOG_LEVEL 下限：OTLP bridge 本身不读 LOG_LEVEL，必须在这里统一裁剪。
type multiHandler struct {
	min  slog.Level
	a, b slog.Handler
}

func (m multiHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	if lvl < m.min {
		return false
	}
	return m.a.Enabled(ctx, lvl) || m.b.Enabled(ctx, lvl)
}
func (m multiHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < m.min {
		return nil
	}
	if m.a.Enabled(ctx, r.Level) {
		_ = m.a.Handle(ctx, r.Clone())
	}
	if m.b.Enabled(ctx, r.Level) {
		_ = m.b.Handle(ctx, r.Clone())
	}
	return nil
}
func (m multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return multiHandler{min: m.min, a: m.a.WithAttrs(attrs), b: m.b.WithAttrs(attrs)}
}
func (m multiHandler) WithGroup(name string) slog.Handler {
	return multiHandler{min: m.min, a: m.a.WithGroup(name), b: m.b.WithGroup(name)}
}
