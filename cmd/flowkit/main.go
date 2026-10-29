// Command flowkit is the production entry point for the Flowkit voice kernel.
// It loads configuration, wires observability, transports, providers, and
// the JSON-RPC server, then runs until SIGTERM triggers graceful shutdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/agentmedia"
	"github.com/DeepFlowAI/flowkit/internal/asr"
	asraliyun "github.com/DeepFlowAI/flowkit/internal/asr/aliyun"
	asrcustom "github.com/DeepFlowAI/flowkit/internal/asr/custom"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/calls"
	"github.com/DeepFlowAI/flowkit/internal/config"
	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/DeepFlowAI/flowkit/internal/extension"
	"github.com/DeepFlowAI/flowkit/internal/instance"
	"github.com/DeepFlowAI/flowkit/internal/llm"
	"github.com/DeepFlowAI/flowkit/internal/media/aec"
	"github.com/DeepFlowAI/flowkit/internal/media/denoise"
	"github.com/DeepFlowAI/flowkit/internal/media/spk"
	"github.com/DeepFlowAI/flowkit/internal/media/turn"
	"github.com/DeepFlowAI/flowkit/internal/media/vad"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/recording"
	"github.com/DeepFlowAI/flowkit/internal/room"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/store"
	"github.com/DeepFlowAI/flowkit/internal/telecom"
	flowsip "github.com/DeepFlowAI/flowkit/internal/transport/sip"
	"github.com/DeepFlowAI/flowkit/internal/transport/webrtc"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	ttsaliyun "github.com/DeepFlowAI/flowkit/internal/tts/aliyun"
	ttscustom "github.com/DeepFlowAI/flowkit/internal/tts/custom"
	ttsmock "github.com/DeepFlowAI/flowkit/internal/tts/mock"
	"github.com/DeepFlowAI/flowkit/internal/tts/textproc"
)

// shutdownGraceDefault is the default time we wait for active calls to
// finish on SIGTERM before force-closing survivors. Override via env
// SHUTDOWN_GRACE_SECONDS.
const shutdownGraceDefault = 30 * time.Second

const (
	serverName    = "flowkit"
	serverVersion = "0.1.0"
)

func main() {
	cfgPath := flag.String("c", "flowkit.yaml", "path to YAML config")
	versionFlag := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("%s %s\n", serverName, serverVersion)
		return
	}

	if err := run(*cfgPath); err != nil {
		fmt.Fprintf(os.Stderr, "flowkit: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath string) error {
	rootCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 1. Config
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// 2. Observability — defer shutdown so traces flush on exit
	shutdownObs, err := observability.InitFromConfig(rootCtx, cfg.Observability)
	if err != nil {
		return fmt.Errorf("observability: %w", err)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = shutdownObs(ctx)
	}()
	observability.InitDialogLogFromEnv()
	observability.InitASRStreamLogFromEnv()
	observability.InitLLMRequestLogFromEnv()
	log := slog.Default()

	// Dialog-behaviour thresholds (SIP_LLM_*): parse once, reject contradictory
	// combinations before any call can observe them, then dump the effective set.
	dialogPol, err := dialogpolicy.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("dialogpolicy: %w", err)
	}
	dialogpolicy.Set(dialogPol)
	dialogPol.LogEffective(log)

	log.Info("flowkit starting",
		"version", serverVersion,
		"node_id", cfg.Node.ID,
		"rpc_listen", cfg.RPC.Listen,
		"asr", cfg.ASR.Provider,
		"tts", cfg.TTS.Provider,
		"llm", cfg.LLM.Provider,
		"store", cfg.Store.Type,
		"dialog_log", observability.DialogLogEnabled(),
		"llm_request_log", observability.LLMRequestLogEnabled(),
	)

	// 3. Store
	st, err := buildStore(cfg.Store)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer func() { _ = st.Close() }()

	// 4. Session manager + instance identity
	mgr := session.NewManager(log)
	recCfg, err := recording.FromYAML(cfg.Recording)
	if err != nil {
		return fmt.Errorf("recording config: %w", err)
	}
	recMgr, err := recording.NewManager(recCfg, log)
	if err != nil {
		return fmt.Errorf("recording: %w", err)
	}
	inst := instance.New()
	log.Info("instance identity", "id", inst.ID, "url", inst.URL)

	// 5. RPC registry (system.hello registered after LLM init — see step 7c).
	registry := rpc.NewRegistry(log)
	serverInfo := rpc.ServerInfo{
		Name:        serverName,
		Version:     serverVersion,
		NodeID:      cfg.Node.ID,
		InstanceID:  inst.ID,
		InstanceURL: inst.URL,
		ICEServers:  cfg.WebRTC.ICEServers,
	}
	if cfg.SIP.Listen != "" {
		mode := cfg.SIP.InboundMode
		if mode == "" {
			mode = "notify"
		}
		serverInfo.SIP = &rpc.SIPStatus{
			Enabled:     true,
			Listen:      cfg.SIP.Listen,
			DID:         cfg.SIP.TrunkDID,
			InboundMode: mode,
		}
	}

	// 6. Provider factories (per-call ASR / per-call TTS).
	asrFactory, err := buildASRFactory(cfg.ASR)
	if err != nil {
		return fmt.Errorf("asr factory: %w", err)
	}
	ttsFactory, err := buildTTSFactory(cfg.TTS)
	if err != nil {
		return fmt.Errorf("tts factory: %w", err)
	}

	// 7. Register call.* methods (answer/hangup/say/listen/stop).
	callHandlers := calls.Register(registry, mgr, calls.ProviderFactories{
		ASR: asrFactory,
		TTS: ttsFactory,
	}, log)

	// Keepalive bridges keep the surviving (customer) leg up when the agent
	// leg dies, so a browser refresh can media-reconnect. The sweep bounds the
	// grace window; without it an orphaned leg would leak forever.
	sweep := 45 * time.Second
	if v := strings.TrimSpace(os.Getenv("KEEPALIVE_SWEEP_MS")); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms >= 0 {
			sweep = time.Duration(ms) * time.Millisecond
		}
	}
	callHandlers.SetKeepaliveSweep(sweep)

	// 7a. Standalone ASR/TTS for non-call consumers (VoiceSpeed agent console).
	// Uses a dedicated provider config when set (cfg.Agent.*), else the
	// call-path config — so a SIP deployment can keep serving aliyun on calls
	// while exposing the private FunASR/CosyVoice models on this path.
	//
	// Browser realtime ASR: endpointing is driven by the neural VAD in
	// internal/agentmedia (flush the instant the speaker stops — integrated-VAD
	// behavior like Aliyun NLS). The provider's own RMS silence-timer is left
	// only as a long fallback for when the VAD is unavailable; at the SIP
	// default of 200 ms it would chop utterances mid-sentence and garble FunASR's
	// offline decode. We also use the native 2pass chunk size. SIP is untouched.
	agentASRSrc := cfg.ASR
	if cfg.Agent.ASR.Provider != "" {
		agentASRSrc = cfg.Agent.ASR
	}
	agentASRFactory := asrFactory
	if agentASRSrc.Provider == "custom" {
		cfgMap := map[string]any{}
		for k, v := range agentASRSrc.Config {
			cfgMap[k] = v
		}
		cfgMap["endpoint_silence_ms"] = envInt("AGENT_ASR_ENDPOINT_SILENCE_MS", 1500)
		cfgMap["chunk_size"] = "5,10,5" // FunASR 2pass native
		agentASRFactory, err = buildASRFactory(config.ProviderConfig{Provider: "custom", Config: cfgMap})
		if err != nil {
			return fmt.Errorf("agent asr factory: %w", err)
		}
	} else if cfg.Agent.ASR.Provider != "" {
		agentASRFactory, err = buildASRFactory(cfg.Agent.ASR)
		if err != nil {
			return fmt.Errorf("agent asr factory: %w", err)
		}
	}
	agentTTSFactory := ttsFactory
	if cfg.Agent.TTS.Provider != "" {
		agentTTSFactory, err = buildTTSFactory(cfg.Agent.TTS)
		if err != nil {
			return fmt.Errorf("agent tts factory: %w", err)
		}
	}
	agentmedia.Register(registry, agentASRFactory, agentTTSFactory, log)

	// 7b. LLM chat (server-side provider from flowkit.yaml + LLM_* env).
	llmSettings := llm.MergeSettings(cfg.LLM)
	llmClient, err := llm.NewProvider(llmSettings)
	if err != nil {
		return fmt.Errorf("llm: %w", err)
	}
	llmHandlers := llm.RegisterChat(registry, llmClient, llmSettings.SystemPrompt, log, mgr)
	if llmClient != nil {
		log.Info("llm enabled", "provider", llmClient.Name(), "model", llmClient.Model())
	} else {
		log.Info("llm disabled", "reason", "LLM_PROVIDER / LLM_API_KEY not set")
	}

	// Optional neural VAD (Silero) — distinguishes human voice from noise so real
	// speech barges in immediately while noise neither interrupts nor records.
	// Falls back to the RMS energy gate when off or if onnxruntime can't load.
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("VAD_ENABLED"))); v == "1" || v == "true" || v == "yes" {
		lib := os.Getenv("VAD_ONNXRUNTIME_LIB")
		if lib == "" {
			lib = "/usr/lib/libonnxruntime.so"
		}
		pool := runtime.NumCPU()
		if err := vad.Init(lib, pool); err != nil {
			log.Warn("vad init failed; using RMS gate", "err", err, "lib", lib)
		} else if p, terr := vad.SelfTest(); terr != nil {
			log.Warn("vad self-test failed; inference contract wrong", "err", terr, "lib", lib)
		} else {
			log.Info("vad enabled (silero)", "lib", lib, "pool", pool, "selftest_prob", p)
		}
	}

	// Optional Smart Turn v3 end-of-turn detection (SIP call.listen). When on,
	// the ASR endpoint is model-driven: on a pause it predicts whether the user
	// actually finished (flush now) or paused mid-thought (keep listening until
	// the long fallback). Shares onnxruntime with the VAD. Falls back to the
	// FunASR silence timer if onnxruntime can't load.
	if envBool("TURN_DETECTION_ENABLED", false) {
		lib := os.Getenv("VAD_ONNXRUNTIME_LIB")
		if lib == "" {
			lib = "/usr/lib/libonnxruntime.so"
		}
		pool := runtime.NumCPU()
		if err := turn.Init(lib, pool); err != nil {
			log.Warn("turn detection init failed; using silence endpoint", "err", err, "lib", lib)
		} else if p, terr := turn.SelfTest(); terr != nil {
			log.Warn("turn detection self-test failed; inference contract wrong", "err", terr, "lib", lib)
		} else {
			log.Info("turn detection enabled (smart-turn v3)", "lib", lib, "pool", pool, "selftest_prob", p)
		}
	}

	// Optional neural speech enhancement (GTCRN) on the mic uplink. Strips
	// environmental noise — traffic, wind, fans — before VAD/ASR, so a VAD-less
	// ASR stops hallucinating sentences out of a noisy street. Runs as the second
	// stage of the AEC capture pipeline; shares onnxruntime with the VAD.
	if envBool("DENOISE_ENABLED", false) {
		lib := os.Getenv("VAD_ONNXRUNTIME_LIB")
		if lib == "" {
			lib = "/usr/lib/libonnxruntime.so"
		}
		pool := runtime.NumCPU()
		if err := denoise.Init(lib, pool); err != nil {
			log.Warn("denoise init failed; uplink stays raw", "err", err, "lib", lib)
		} else {
			log.Info("denoise enabled (gtcrn)", "lib", lib, "pool", pool)
		}
	}

	// Optional speaker gate (CAM++) on finalised utterances. Denoising cannot
	// remove other people's speech, so on a street or in an open office
	// bystanders still reach a VAD-less ASR as fluent sentences. This enrolls the
	// caller's voice on their first solid utterance and drops the ones that
	// belong to somebody else. Shares onnxruntime with the VAD.
	if envBool("SPK_GATE_ENABLED", false) {
		lib := os.Getenv("VAD_ONNXRUNTIME_LIB")
		if lib == "" {
			lib = "/usr/lib/libonnxruntime.so"
		}
		pool := runtime.NumCPU()
		if err := spk.Init(lib, pool); err != nil {
			log.Warn("speaker gate init failed; every voice is accepted", "err", err, "lib", lib)
		} else {
			log.Info("speaker gate enabled (campplus)", "lib", lib, "pool", pool)
		}
	}

	// Optional WebRTC AEC3 echo cancellation (SIP only). Removes the played TTS
	// echo from the mic uplink before VAD/ASR so external-speaker/telephony echo
	// does not trigger false barge-in. NS/AGC are independently toggleable.
	if envBool("AEC_ENABLED", false) {
		aecOpt := aec.Options{
			NSEnabled:     envBool("AEC_NS_ENABLED", true),
			NSLevel:       envInt("AEC_NS_LEVEL", 2),
			AGCEnabled:    envBool("AEC_AGC_ENABLED", false),
			StreamDelayMS: envInt("AEC_STREAM_DELAY_MS", 0),
		}
		calls.EnableAEC(mgr, aecOpt, log)
		log.Info("aec enabled (webrtc aec3)",
			"compiled_in", aec.Available(),
			"ns", aecOpt.NSEnabled, "ns_level", aecOpt.NSLevel,
			"agc", aecOpt.AGCEnabled, "stream_delay_ms", aecOpt.StreamDelayMS)
	}

	// 7c. system.hello (after LLM — exposes sip.kernel_auto_llm accurately).
	if serverInfo.SIP != nil {
		wantKernelAutoLLM := envBool("SIP_KERNEL_AUTO_LLM", false)
		serverInfo.SIP.KernelAutoLLM = wantKernelAutoLLM && llmClient != nil
		if wantKernelAutoLLM && llmClient == nil {
			log.Warn("SIP_KERNEL_AUTO_LLM=true but LLM not configured — kernel auto-agent disabled")
		}
	}
	rpc.RegisterSystemMethods(registry, serverInfo, func() {
		log.Info("shutdown requested via rpc")
		cancel()
	})

	// 8. RPC server (needed first so signalling can broadcast through it).
	rpcServer := rpc.New(cfg.RPC, registry, log)
	// Now that the broadcaster exists, let call handlers fan out transcript
	// events (call.user_text / call.assistant_text) for chat-UI mirroring.
	callHandlers.SetBroadcaster(rpcServer)
	callHandlers.SetOutboundURLAllowedHosts(parseStringCSV(os.Getenv("FLOWKIT_OUTBOUND_URL_ALLOWED_HOSTS")))
	recording.RegisterRPC(registry, recMgr, mgr)
	if recMgr != nil {
		recMgr.SetNotifier(recording.RPCNotifier{
			Broadcast: rpcServer.BroadcastNotification,
			Log:       log,
		})
		recMgr.RegisterHooks(mgr)
		callHandlers.SetBridgeObserver(recMgr)
		log.Info("recording enabled", "storage", recCfg.Storage.Backend, "layout", recCfg.Layout, "remote_gain", recCfg.RemoteGain)
	}

	// Optional dual-leg live transcript on bridged (human↔human) calls. Each
	// leg is transcribed independently and pushed as call.bridge_transcript.
	//
	// Transcription wants full sentences, not the IVR call-path's snappy
	// barge-in tuning — a 200 ms endpoint silence chops human speech into
	// fragments and FunASR prefixes each with a comma. So for the custom FunASR
	// provider we override to the native 2pass chunk and a longer endpoint
	// silence, exactly like the agent console path above.
	if envBool("BRIDGE_ASR_ENABLED", false) {
		bridgeASRFactory := asrFactory
		if cfg.ASR.Provider == "custom" {
			cfgMap := map[string]any{}
			for k, v := range cfg.ASR.Config {
				cfgMap[k] = v
			}
			cfgMap["endpoint_silence_ms"] = envInt("BRIDGE_ASR_ENDPOINT_SILENCE_MS", 800)
			cfgMap["chunk_size"] = "5,10,5"
			bridgeASRFactory, err = buildASRFactory(config.ProviderConfig{Provider: "custom", Config: cfgMap})
			if err != nil {
				return fmt.Errorf("bridge asr factory: %w", err)
			}
		}
		callHandlers.SetBridgeASR(true, bridgeASRFactory)
		log.Info("bridge ASR enabled (dual-leg live transcript)")
	}
	rpcServer.SetInstanceID(inst.ID)
	rpcServer.SetDrainCheck(mgr.IsDraining)
	rpcServer.SetCallCounter(func() (int, int) {
		return mgr.Count(), int(mgr.TotalCreated())
	})
	// WS-PCM media leg (Bridge-B): a client (VoiceSpeed backend bridging a browser)
	// streams 16k int16 PCM here and flowkit runs the full converse core on it.
	rpcServer.RegisterHTTP("/media/pcm", rpcServer.RequireAuth(callHandlers.PCMSessionHandler()))
	startDebugServer(rootCtx, log)

	// 9. WebRTC signalling — registers webrtc.offer / webrtc.ice methods.
	webrtcSignal := webrtc.RegisterSignalMethods(
		registry,
		func() (*webrtc.Peer, error) {
			natIP := instance.ResolvePublicIP(cfg.WebRTC.NAT1To1IP, inst.URL)
			var natIPs []string
			if natIP != "" {
				natIPs = []string{natIP}
			}
			var gatherWait time.Duration
			if len(natIPs) > 0 {
				gatherWait = webrtc.DefaultICEGatherMaxWait
			}
			return webrtc.New(webrtc.Config{
				ICEServers:       cfg.WebRTC.ICEServers,
				NAT1To1IPs:       natIPs,
				ICEGatherMaxWait: gatherWait,
				Logger:           log,
			})
		},
		mgr,
		rpcServer,
		log,
	)
	defer webrtcSignal.Shutdown()

	// 9b. Telecom Catalog (M6). Holds Trunks from either the static `system`
	// Provider (env vars) or dynamic Providers registered via HTTP. Even when
	// register is disabled, the catalog still services inbound/outbound
	// lookups from the static Provider.
	catalog := telecom.NewCatalog(nil)
	catalog.SetEventSink(telecomEventSink{server: rpcServer, log: log})
	if cfg.Telecom.Static.Enabled {
		settings := telecomSystemSettings(cfg.SIP)
		snap := telecom.BuildSystemSnapshot(settings)
		if len(snap.Trunks) > 0 {
			if _, err := catalog.ApplySnapshot(snap); err != nil {
				log.Warn("telecom: system snapshot rejected", "err", err)
			} else {
				log.Info("telecom: system Provider loaded from env",
					"trunks", len(snap.Trunks))
			}
		}
	}
	if cfg.Telecom.Register.Enabled {
		if strings.TrimSpace(cfg.Telecom.Register.APIKey) == "" {
			log.Warn("telecom: register enabled but FLOWKIT_TELECOM_API_KEY is empty — all requests will be rejected")
		}
		reg := telecom.NewRegisterServer(catalog, telecom.RegisterServerOptions{
			APIKey:        cfg.Telecom.Register.APIKey,
			LeaseMinSec:   cfg.Telecom.Register.LeaseMinSec,
			LeaseMaxSec:   cfg.Telecom.Register.LeaseMaxSec,
			DefaultTTLSec: 90,
		})
		mux := http.NewServeMux()
		reg.Mount(mux)
		rpcServer.RegisterHTTP("/api/v1/telecom/", mux)
		go catalog.RunSweeper(rootCtx, telecom.SweeperConfig{
			Interval: time.Duration(cfg.Telecom.Register.SweepIntervalMS) * time.Millisecond,
			Logger:   log,
		})
		log.Info("telecom: register http api enabled", "lease_min_sec", cfg.Telecom.Register.LeaseMinSec, "lease_max_sec", cfg.Telecom.Register.LeaseMaxSec)
	}

	// 9c. Extension registrar (trust-network SIP registrar). When enabled,
	// inbound REGISTERs are recorded so calls can be originated to a phone by
	// extension number (OpenDesk "call the agent's handset"). Gated off by
	// default — leaves REGISTER handling and originate unchanged when disabled.
	var extReg *extension.Registry
	if cfg.Telecom.Extensions.Enabled {
		extReg = extension.NewRegistry(nil)
		extReg.SetEventSink(extensionEventSink{server: rpcServer, log: log})
		host, port := sipRegistrarAddr(cfg.SIP)
		extReg.RegisterRPC(registry, extension.RegistrarInfo{
			Host:      host,
			Port:      port,
			Transport: cfg.SIP.Transport,
		})
		sweep := time.Duration(cfg.Telecom.Extensions.SweepIntervalMS) * time.Millisecond
		go extReg.RunSweeper(rootCtx, sweep)
		callHandlers.SetExtensionResolver(extReg)
		log.Info("extension registrar enabled", "sweep_interval_ms", cfg.Telecom.Extensions.SweepIntervalMS)
	}

	// 9d. Multi-party mixing rooms (three-way, barge-in, whisper, takeover,
	// whole-chain hangup). Gated off by default — registering the room.*
	// methods and starting per-leg mixing only when enabled. Each leg's
	// mixing goroutines exit on leg end / room close (see internal/room).
	if cfg.Telecom.Rooms.Enabled {
		roomMgr := room.NewManager(mgr, log)
		callHandlers.EnableRooms(roomMgr, registry)
		log.Info("multi-party rooms enabled")
	}

	// 10. SIP UA (optional — bound only if cfg.SIP.Listen is non-empty).
	if cfg.SIP.Listen != "" {
		sipUA, err := flowsip.New(flowsip.Config{
			Listen:      cfg.SIP.Listen,
			AltListen:   cfg.SIP.AltListen,
			ExtIP:       cfg.SIP.ExtIP,
			UserAgent:   serverName,
			Logger:      log,
			ContactPort: envInt("SIP_CONTACT_PORT", 0),
			RTPPortMin:  envInt("RTP_PORT_MIN", 0),
			RTPPortMax:  envInt("RTP_PORT_MAX", 0),
		})
		if err != nil {
			return fmt.Errorf("sip: %w", err)
		}
		sipSignal := flowsip.NewSignal(sipUA, mgr, rpcServer, log)
		sipSignal.SetCaptureEarlyMedia(recCfg.Enabled && recCfg.CaptureEarlyMedia)
		sipSignal.SetOutboundProxy(cfg.SIP.OutboundProxy)
		sipSignal.SetPhoneOutboundProxy(cfg.SIP.PhoneOutboundProxy)
		outTrunk := sipOutboundTrunk(cfg.SIP)
		sipSignal.SetTrunkConfig(outTrunk)
		sipSignal.SetTrunkResolver(catalog)
		sipSignal.SetRejectUnknownPeer(cfg.Telecom.Inbound.RejectUnknownPeer)
		if blocked := parseStringCSV(os.Getenv("SIP_BLOCKED_PEERS")); len(blocked) > 0 {
			sipSignal.SetBlockedPeers(blocked)
			log.Info("sip blocked peers enabled", "count", len(blocked))
		}
		if cfg.Telecom.Inbound.ToNormalization == "e164_strip_prefix" {
			sipSignal.SetToNormalizer(telecom.NormalizeE164)
		}
		sipSignal.SetOutboundResolver(makeOutboundResolver(catalog))
		callHandlers.SetSIPHoldController(sipSignal)
		callHandlers.SetSIPOriginateController(sipSignal)
		if extReg != nil {
			sipUA.SetExtensionRegistry(extReg)
		}
		// Originate is enabled when either the static or any dynamic Trunk has
		// an outbound profile. The Signal-level resolver picks the right one
		// per call; we just need to know whether to register the RPC method.
		if cfg.SIP.AllowOriginate {
			callHandlers.EnableOriginate(32)
			callHandlers.RegisterOriginate(registry)
			if outTrunk.OutboundTrunkReady() {
				log.Info("sip originate enabled", "trunk", outTrunk.Server)
			} else {
				log.Info("sip originate enabled (catalog-routed; no static trunk fallback)")
			}
		}
		defer sipSignal.Shutdown()

		if envBool("SIP_KERNEL_AUTO_LLM", false) && llmClient != nil {
			llmCfg := calls.LoadInboundLLMConfig(dialogPol.Greeting)
			agent := calls.NewInboundLLMAgent(callHandlers, llmHandlers, mgr, llmCfg, log)
			sipSignal.SetOnAccepted(agent.HandleIncoming)
			if envBool("SIP_KERNEL_EXCLUSIVE", true) {
				callHandlers.SetKernelExclusive(true)
			}
			log.Info("sip inbound llm agent enabled",
				"barge_in_min_chars", llmCfg.BargeInMinChars,
				"barge_in_on_start", llmCfg.BargeInOnStart,
				"kernel_exclusive", callHandlers.KernelExclusive(),
			)
		} else if envBool("SIP_KERNEL_AUTO_LLM", false) {
			log.Warn("SIP_KERNEL_AUTO_LLM=true but LLM not configured — inbound calls notify only")
		}

		go func() {
			if err := sipUA.Start(rootCtx); err != nil {
				log.Error("sip ua exited", "err", err)
			}
		}()
		defer func() { _ = sipUA.Stop() }()
		log.Info("sip enabled", "listen", cfg.SIP.Listen)
	}

	// 11. Health broadcaster (15s tick) — runs alongside server
	health := rpc.NewHealthBroadcaster(rpcServer, 15*time.Second, func() rpc.HealthParams {
		return rpc.HealthParams{
			Calls: mgr.Count(),
			Conns: rpcServer.ConnCount(),
		}
	})
	go health.Run(rootCtx)

	// 12. Run — blocks until SIGTERM or rpc.system.shutdown.
	// The signal handler (rootCtx cancellation) triggers the drain sequence
	// below before rpcServer.Start returns.
	go func() {
		<-rootCtx.Done()
		runDrainSequence(mgr, shutdownGrace(), log)
	}()

	if err := rpcServer.Start(rootCtx); err != nil {
		return fmt.Errorf("rpc: %w", err)
	}
	log.Info("flowkit stopped cleanly")
	return nil
}

// sipRegistrarAddr splits cfg.SIP.Listen ("host:port") into the host/port a
// phone should REGISTER to. ExtIP (when a concrete IP) is preferred over a
// 0.0.0.0 / empty bind host so the advertised address is reachable. Returns
// zero values when nothing usable is configured.
func sipRegistrarAddr(sipCfg config.SIPConfig) (host string, port int) {
	if h, p, ok := strings.Cut(sipCfg.Listen, ":"); ok {
		host = h
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	if (host == "" || host == "0.0.0.0") && sipCfg.ExtIP != "" && sipCfg.ExtIP != "auto" {
		host = sipCfg.ExtIP
	}
	return host, port
}

// sipOutboundTrunk returns originate routing config. Outbound-specific fields
// override the general trunk_* block so inbound and outbound can use different
// gateways.
func sipOutboundTrunk(sipCfg config.SIPConfig) flowsip.TrunkConfig {
	server := sipCfg.TrunkOutboundServer
	if server == "" {
		server = sipCfg.TrunkServer
	}
	port := sipCfg.TrunkOutboundPort
	if port <= 0 {
		port = sipCfg.TrunkPort
	}
	did := sipCfg.TrunkOutboundDID
	if did == "" {
		did = sipCfg.TrunkDID
	}
	user := sipCfg.TrunkOutboundUser
	if user == "" {
		user = sipCfg.TrunkUser
	}
	pass := sipCfg.TrunkOutboundPass
	if pass == "" {
		pass = sipCfg.TrunkPass
	}
	realm := sipCfg.TrunkOutboundRealm
	if realm == "" {
		realm = sipCfg.TrunkRealm
	}
	return flowsip.TrunkConfig{
		Server:       server,
		Port:         port,
		User:         user,
		Pass:         pass,
		Realm:        realm,
		DID:          did,
		CalleePrefix: sipCfg.TrunkOutboundCalleePrefix,
	}
}

// shutdownGrace returns the configured drain timeout (env override or default).
func shutdownGrace() time.Duration {
	if v := os.Getenv("SHUTDOWN_GRACE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return shutdownGraceDefault
}

// startDebugServer exposes Go's pprof endpoints on localhost so production
// memory alerts can be investigated without SIGQUIT or public debug exposure.
func startDebugServer(ctx context.Context, log *slog.Logger) {
	addr := strings.TrimSpace(os.Getenv("FLOWKIT_DEBUG_ADDR"))
	if addr == "" {
		addr = "127.0.0.1:6060"
	}
	if strings.EqualFold(addr, "off") || strings.EqualFold(addr, "disabled") {
		log.Info("debug pprof server disabled")
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("debug pprof listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn("debug pprof server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

// runDrainSequence executes the graceful-shutdown phases:
//  1. BeginShutdown → new calls rejected, /health → 503
//  2. Wait for active calls to finish naturally, capped by grace
//  3. Force-hangup any survivors
//
// Called once when rootCtx is cancelled (SIGTERM or system.shutdown).
func runDrainSequence(mgr *session.Manager, grace time.Duration, log *slog.Logger) {
	if mgr.IsDraining() {
		return // idempotent
	}
	log.Info("drain started", "active_calls", mgr.Count(), "grace", grace.String())
	mgr.BeginShutdown()

	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	remaining := mgr.WaitDrain(ctx)
	if remaining > 0 {
		log.Warn("drain timeout — force closing", "remaining", remaining)
	} else {
		log.Info("drain complete — all calls finished gracefully")
	}
	mgr.Shutdown("server_stopping")
}

// buildStore picks the Store implementation based on config. RedisStore is
// stubbed in M1; selecting it returns an error today.
func buildStore(cfg config.StoreConfig) (store.Store, error) {
	switch cfg.Type {
	case "redis":
		return store.NewRedisStore()
	case "", "local":
		return store.NewLocalStore(), nil
	default:
		return nil, fmt.Errorf("unknown store.type %q", cfg.Type)
	}
}

// buildASRFactory returns a factory that constructs a fresh asr.Provider
// for each Call (each ASR session is per-stream).
func buildASRFactory(cfg config.ProviderConfig) (func() (asr.Provider, error), error) {
	switch cfg.Provider {
	case "", "mock":
		return func() (asr.Provider, error) { return asrmock.New(), nil }, nil
	case "aliyun":
		return func() (asr.Provider, error) {
			return asraliyun.New(asraliyun.Config{
				AccessKeyID:          getString(cfg.Config, "access_key_id", os.Getenv("ALIYUN_ACCESS_KEY_ID")),
				AccessKeySecret:      getString(cfg.Config, "access_key_secret", os.Getenv("ALIYUN_ACCESS_KEY_SECRET")),
				AppKey:               getString(cfg.Config, "app_key", os.Getenv("ALIYUN_NLS_APPKEY")),
				Region:               getString(cfg.Config, "region", os.Getenv("ALIYUN_NLS_REGION")),
				MaxSentenceSilenceMs: getInt(cfg.Config, "max_sentence_silence_ms", envInt("ASR_MAX_SENTENCE_SILENCE_MS", 0)),
			})
		}, nil
	case "custom":
		return func() (asr.Provider, error) {
			return asrcustom.New(asrcustom.Config{
				BaseURL:            getString(cfg.Config, "base_url", os.Getenv("ASR_BASE_URL")),
				Mode:               getString(cfg.Config, "mode", os.Getenv("ASR_MODE")),
				Hotwords:           getString(cfg.Config, "hotwords", os.Getenv("ASR_HOTWORDS")),
				ITN:                getBool(cfg.Config, "itn", envBool("ASR_ITN", true)),
				InsecureSkipVerify: getBool(cfg.Config, "insecure_skip_verify", envBool("ASR_INSECURE_SKIP_VERIFY", false)),
				ChunkSize:          parseIntCSV(getString(cfg.Config, "chunk_size", os.Getenv("ASR_CHUNK_SIZE"))),
				EndpointSilenceMs:  endpointSilenceMs(cfg),
				EndpointSilenceRMS: getInt(cfg.Config, "endpoint_silence_rms", envInt("ASR_ENDPOINT_SILENCE_RMS", 350)),
			})
		}, nil
	default:
		return nil, fmt.Errorf("unknown asr.provider %q", cfg.Provider)
	}
}

// endpointSilenceMs is the FunASR trailing-silence flush timeout. With Smart
// Turn detection on, the model owns the endpoint cadence (early release on a
// real end-of-turn), so FunASR's own timer only acts as a long fallback
// (TURN_MAX_SILENCE_MS) for the case the model keeps saying "incomplete".
// With it off, the normal ASR_ENDPOINT_SILENCE_MS applies — behaviour unchanged.
func endpointSilenceMs(cfg config.ProviderConfig) int {
	if turn.Enabled() {
		return envInt("TURN_MAX_SILENCE_MS", 1500)
	}
	return getInt(cfg.Config, "endpoint_silence_ms", envInt("ASR_ENDPOINT_SILENCE_MS", 0))
}

// buildTTSFactory mirrors buildASRFactory for the synthesis side. Real
// providers are wrapped with the model-independent text pipeline (symbol
// cleanup, pronunciation lexicon, digit reading) and audio guard.
func buildTTSFactory(cfg config.ProviderConfig) (func() (tts.Provider, error), error) {
	if cfg.Provider == "" || cfg.Provider == "mock" {
		return func() (tts.Provider, error) { return ttsmock.New(), nil }, nil
	}
	inner, err := buildRawTTSFactory(cfg)
	if err != nil {
		return nil, err
	}
	lex, err := textproc.LoadLexicon(getString(cfg.Config, "lexicon_path", os.Getenv("TTS_LEXICON_PATH")))
	if err != nil {
		return nil, err
	}
	// Model names the voice model behind the provider (e.g. "cosyvoice2") so
	// lexicon entries can target one model; defaults to the provider name.
	model := getString(cfg.Config, "model", os.Getenv("TTS_MODEL"))
	if model == "" {
		model = cfg.Provider
	}
	proc := &textproc.Processor{Lexicon: lex, Model: model}
	guard := textproc.Guard{
		MaxSilenceMs: getInt(cfg.Config, "guard_max_silence_ms", envInt("TTS_GUARD_MAX_SILENCE_MS", 800)),
		BaseMs:       getInt(cfg.Config, "guard_base_ms", envInt("TTS_GUARD_BASE_MS", 3000)),
		PerCharMs:    getInt(cfg.Config, "guard_per_char_ms", envInt("TTS_GUARD_PER_CHAR_MS", 450)),
	}
	slog.Default().Info("tts text pipeline", "provider", cfg.Provider, "model", model,
		"lexicon_entries", lex.Len(), "max_silence_ms", guard.MaxSilenceMs,
		"base_ms", guard.BaseMs, "per_char_ms", guard.PerCharMs)
	return func() (tts.Provider, error) {
		p, err := inner()
		if err != nil {
			return nil, err
		}
		return textproc.Wrap(p, proc, guard), nil
	}, nil
}

func buildRawTTSFactory(cfg config.ProviderConfig) (func() (tts.Provider, error), error) {
	switch cfg.Provider {
	case "aliyun":
		return func() (tts.Provider, error) {
			return ttsaliyun.New(ttsaliyun.Config{
				AccessKeyID:     getString(cfg.Config, "access_key_id", os.Getenv("ALIYUN_ACCESS_KEY_ID")),
				AccessKeySecret: getString(cfg.Config, "access_key_secret", os.Getenv("ALIYUN_ACCESS_KEY_SECRET")),
				AppKey:          getString(cfg.Config, "app_key", os.Getenv("ALIYUN_NLS_APPKEY")),
				Region:          getString(cfg.Config, "region", os.Getenv("ALIYUN_NLS_REGION")),
				DefaultVoice:    getString(cfg.Config, "voice", os.Getenv("TTS_VOICE")),
			})
		}, nil
	case "custom":
		// One shared client across all per-utterance providers so its keep-alive
		// connection pool is reused (each sentence otherwise re-does TCP+TLS).
		ttsClient := ttscustom.NewHTTPClient(0, 0)
		return func() (tts.Provider, error) {
			return ttscustom.New(ttscustom.Config{
				BaseURL:            getString(cfg.Config, "base_url", os.Getenv("TTS_BASE_URL")),
				DefaultVoice:       getString(cfg.Config, "voice", os.Getenv("TTS_VOICE")),
				SourceSampleRate:   getInt(cfg.Config, "source_sample_rate", envInt("TTS_SOURCE_SAMPLE_RATE", 24000)),
				Endpoint:           getString(cfg.Config, "endpoint", os.Getenv("TTS_ENDPOINT")),
				TrimLeadingSilence: getBool(cfg.Config, "trim_leading_silence", envBool("TTS_TRIM_LEADING_SILENCE", true)),
				PrebufferMs:        getInt(cfg.Config, "prebuffer_ms", envInt("TTS_PREBUFFER_MS", 0)),
				LeadPadMs:          getInt(cfg.Config, "lead_pad_ms", envInt("TTS_LEAD_PAD_MS", 0)),
				TrimSustainMs:      getInt(cfg.Config, "trim_sustain_ms", envInt("TTS_TRIM_SUSTAIN_MS", 0)),
				MaxVowelHoldMs:     getInt(cfg.Config, "max_vowel_hold_ms", envInt("TTS_MAX_VOWEL_HOLD_MS", 0)),
				HTTPClient:         ttsClient,
			})
		}, nil
	default:
		return nil, fmt.Errorf("unknown tts.provider %q", cfg.Provider)
	}
}

// getString reads a string from a map[string]any with an env fallback.
func getString(m map[string]any, key, fallback string) string {
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return fallback
}

// getInt reads an int from a map[string]any (handles int/float64/string from
// YAML or env expansion) with a fallback.
func getInt(m map[string]any, key string, fallback int) int {
	switch v := m[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return fallback
}

// getBool reads a bool from a map[string]any (handles bool/string) with a
// fallback.
func getBool(m map[string]any, key string, fallback bool) bool {
	switch v := m[key].(type) {
	case bool:
		return v
	case string:
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return fallback
}

// envInt reads an int from an env var, returning defaultVal when unset/invalid.
func envInt(key string, defaultVal int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key))); err == nil && n > 0 {
		return n
	}
	return defaultVal
}

// parseIntCSV parses "8,8,4" into []int{8,8,4}. Returns nil on empty or any
// malformed element, letting the caller fall back to its own default.
func parseIntCSV(s string) []int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil
		}
		out = append(out, n)
	}
	return out
}

func parseStringCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envBool(key string, defaultVal bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultVal
	}
	v = strings.ToLower(v)
	return v == "1" || v == "true" || v == "yes"
}

// telecomSystemSettings converts the legacy SIP_TRUNK_* config block into the
// input expected by telecom.BuildSystemSnapshot.
func telecomSystemSettings(s config.SIPConfig) telecom.SystemTrunkSettings {
	return telecom.SystemTrunkSettings{
		InboundServer:        s.TrunkServer,
		InboundPort:          s.TrunkPort,
		InboundUser:          s.TrunkUser,
		InboundPass:          s.TrunkPass,
		InboundRealm:         s.TrunkRealm,
		InboundDID:           s.TrunkDID,
		OutboundServer:       s.TrunkOutboundServer,
		OutboundPort:         s.TrunkOutboundPort,
		OutboundUser:         s.TrunkOutboundUser,
		OutboundPass:         s.TrunkOutboundPass,
		OutboundRealm:        s.TrunkOutboundRealm,
		OutboundDID:          s.TrunkOutboundDID,
		OutboundCalleePrefix: s.TrunkOutboundCalleePrefix,
		AllowOriginate:       s.AllowOriginate,
	}
}

// makeOutboundResolver wires Catalog.GetTrunk into the per-trunk outbound
// lookup the SIP signal expects. Empty trunk_id targets system_outbound.
func makeOutboundResolver(catalog *telecom.Catalog) flowsip.OutboundResolverFn {
	return func(trunkID string) (flowsip.TrunkConfig, bool) {
		want := trunkID
		if want == "" {
			want = telecom.SystemOutboundTrunkID
		}
		t := catalog.GetTrunk(want)
		if t == nil || !t.SupportsOutbound() {
			return flowsip.TrunkConfig{}, false
		}
		return flowsip.TrunkConfig{
			Server:       t.Outbound.Server,
			Port:         t.Outbound.Port,
			User:         t.Outbound.User,
			Pass:         t.Outbound.Pass,
			Realm:        t.Outbound.Realm,
			DID:          t.Outbound.DID,
			CalleePrefix: t.Outbound.CalleePrefix,
		}, true
	}
}

// telecomEventSink bridges telecom.EventSink to rpc.Server's broadcast so
// CC clients receive telecom.trunk.* notifications over their WS session.
type telecomEventSink struct {
	server *rpc.Server
	log    *slog.Logger
}

func (s telecomEventSink) Emit(method string, payload map[string]any) {
	n, err := rpc.NewNotification(method, payload)
	if err != nil {
		if s.log != nil {
			s.log.Warn("telecom event marshal failed", "method", method, "err", err)
		}
		return
	}
	s.server.BroadcastNotification(n)
}

// extensionEventSink bridges extension.EventSink to rpc.Server's broadcast so
// CC clients receive extension.registered / extension.unregistered
// notifications over their WS session. It reuses the same broadcast mechanism
// as telecomEventSink.
type extensionEventSink struct {
	server *rpc.Server
	log    *slog.Logger
}

func (s extensionEventSink) Emit(method string, payload map[string]any) {
	n, err := rpc.NewNotification(method, payload)
	if err != nil {
		if s.log != nil {
			s.log.Warn("extension event marshal failed", "method", method, "err", err)
		}
		return
	}
	s.server.BroadcastNotification(n)
}
