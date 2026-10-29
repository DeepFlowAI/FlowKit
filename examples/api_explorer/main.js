/* Flowkit API Explorer — drives every kernel RPC method from one page.
 *
 * Protocol: JSON-RPC 2.0 over WebSocket.
 *
 * The page intentionally exposes one card per API surface so each
 * interaction maps 1:1 to a method name.
 */

const $ = (id) => document.getElementById(id);

const el = {
  url: $('url'),
  apiKey: $('apiKey'),
  connect: $('connect'),
  disconnect: $('disconnect'),
  wsState: $('wsState'),
  serverInfo: $('serverInfo'),

  startCall: $('startCall'),
  callState: $('callState'),
  callIdShow: $('callIdShow'),
  pcState: $('pcState'),
  iceState: $('iceState'),
  remoteAudio: $('remoteAudio'),

  sayText: $('sayText'),
  sayVoice: $('sayVoice'),
  sayBtn: $('sayBtn'),
  playIdShow: $('playIdShow'),

  listenBtn: $('listenBtn'),
  stopListenBtn: $('stopListenBtn'),
  asrState: $('asrState'),
  speechArea: $('speechArea'),

  hangupBtn: $('hangupBtn'),

  agentReadyBtn: $('agentReadyBtn'),
  sipInboundMode: $('sipInboundMode'),
  sipStatus: $('sipStatus'),
  sipCallIdShow: $('sipCallIdShow'),
  sipLegState: $('sipLegState'),
  bridgeState: $('bridgeState'),
  sipDialHint: $('sipDialHint'),
  sipModeBridge: $('sipModeBridge'),
  sipModeNotify: $('sipModeNotify'),
  sipModeNotifyHint: $('sipModeNotifyHint'),
  sipManualBridgeBtn: $('sipManualBridgeBtn'),
  sipManualLlmBtn: $('sipManualLlmBtn'),
  sipManualHangupBtn: $('sipManualHangupBtn'),

  llmInfo: $('llmInfo'),
  llmUser: $('llmUser'),
  llmSendBtn: $('llmSendBtn'),
  llmUseAsrBtn: $('llmUseAsrBtn'),
  llmResetBtn: $('llmResetBtn'),
  llmVoiceMode: $('llmVoiceMode'),
  llmState: $('llmState'),
  llmOutput: $('llmOutput'),
  ttsQueueState: $('ttsQueueState'),

  log: $('log'),
  clearLog: $('clearLog'),
  hideHealth: $('hideHealth'),
};

// --- state -----------------------------------------------------------
let ws = null;
let pc = null;
let callId = null;
let listening = false;
let speechBuffer = []; // {type: 'partial'|'final', text}
let lastAsrFinal = '';
let llmReplyText = '';
let llmEnabled = false;
let llmChatLog = []; // {role:'user'|'assistant', text}
let llmTurnWaiters = new Map(); // turn_id -> {resolve, reject}
let activeLlmTurnId = null;
let llmVoiceTurn = false;
let llmSplitter = null;
let llmTurnBarged = false;
let ttsQueue = [];
let ttsPlaying = false;
let bargeInPending = false; // per user utterance
let nextRpcId = 1;
const pending = new Map();

// WebRTC fast-connect: trickle ICE + optional pre-gather pool (see webrtc notes in README).
const DEFAULT_ICE_SERVERS = [
  { urls: 'stun:stun.miwifi.com:3478' },
  { urls: 'stun:stun.l.google.com:19302' },
];
let clientIceServers = DEFAULT_ICE_SERVERS.slice();
let icePrewarmPC = null;
let pendingLocalICE = [];

// SIP inbound test (Explorer simulates application-layer policy)
let sipBridgeMode = false;
let sipManualBusy = false;
let sipCallId = null;
let pendingSipCallId = null;
let sipLlmActive = false;
let sipListening = false;
let llmVoiceCallId = null;
let ttsActiveCallId = null;
let kernelHost = 'localhost';
let kernelServerAutoLlm = false;
let sipModeInitialized = false;

const SIP_MODE_STORAGE = 'flowkit.explorer.sipInboundMode';

const AGENT_READY_LABEL = 'Agent 就绪 (Start WebRTC)';
const AGENT_ANSWER_LABEL = '接听来电 — 启动 WebRTC 坐席';

function setAgentReadyButton(waitingIncoming) {
  if (!el.agentReadyBtn) return;
  el.agentReadyBtn.textContent = waitingIncoming ? AGENT_ANSWER_LABEL : AGENT_READY_LABEL;
  el.agentReadyBtn.classList.toggle('agent-waiting', !!waitingIncoming);
}

function saveSipInboundMode(mode) {
  try {
    if (mode) localStorage.setItem(SIP_MODE_STORAGE, mode);
  } catch (_) { /* private mode */ }
}

function applyInitialSipMode(helloMode) {
  const fromUrl = new URLSearchParams(location.search).get('sip_mode');
  if (fromUrl && el.sipInboundMode) {
    el.sipInboundMode.value = fromUrl;
    saveSipInboundMode(fromUrl);
    return;
  }
  let saved = null;
  try {
    saved = localStorage.getItem(SIP_MODE_STORAGE);
  } catch (_) { /* ignore */ }
  if (saved && el.sipInboundMode) {
    el.sipInboundMode.value = saved;
    return;
  }
  if (helloMode && el.sipInboundMode) {
    el.sipInboundMode.value = helloMode;
  }
}

function getSipInboundMode() {
  return el.sipInboundMode?.value || 'notify';
}

function setSipLegState(text, kind) {
  if (!el.sipLegState) return;
  el.sipLegState.textContent = text;
  el.sipLegState.className = 'pill' + (kind ? ' ' + kind : '');
}

function kernelHostFromWsUrl(url) {
  try {
    const u = new URL(url.replace(/^ws/i, 'http'));
    return u.hostname || 'localhost';
  } catch {
    return 'localhost';
  }
}

function setBridgeState(text, kind) {
  if (!el.bridgeState) return;
  el.bridgeState.textContent = text;
  el.bridgeState.className = 'pill' + (kind ? ' ' + kind : '');
}

function updateSipModeUI() {
  const mode = getSipInboundMode();
  if (el.sipModeBridge) {
    el.sipModeBridge.style.display = mode === 'bridge' ? 'flex' : 'none';
  }
  if (el.sipModeNotify) {
    el.sipModeNotify.style.display = mode === 'notify' ? 'flex' : 'none';
  }
  if (el.sipModeNotifyHint) {
    el.sipModeNotifyHint.style.display = mode === 'notify' ? 'block' : 'none';
  }
  updateSipManualButtons();
  refreshSipDialHint();
}

function isSipAgentBridged() {
  const sipId = sipCallId || pendingSipCallId;
  if (!sipId || sipLlmActive) return false;
  return sipBridgeMode || !!el.bridgeState?.classList.contains('ok');
}

function updateSipManualButtons() {
  const sipId = sipCallId || pendingSipCallId;
  const mode = getSipInboundMode();
  const agentBridged = isSipAgentBridged();
  if (el.sipManualBridgeBtn) {
    el.sipManualBridgeBtn.disabled = !sipId || mode !== 'notify' || sipManualBusy;
    el.sipManualBridgeBtn.title = !sipId
      ? '等待 SIP 来电（需先 Connect，再拨打 DID）'
      : (sipLlmActive ? 'AI 接待中 — 点此转人工坐席' : (agentBridged ? '已在坐席对讲中' : '开麦克风 + call.bridge，浏览器与手机对讲'));
  }
  if (el.sipManualLlmBtn) {
    el.sipManualLlmBtn.disabled = !sipId || !llmEnabled || (sipLlmActive && !agentBridged) || mode !== 'notify' || sipManualBusy;
    el.sipManualLlmBtn.title = !llmEnabled
      ? '内核需配置 LLM_PROVIDER + LLM_API_KEY'
      : (agentBridged ? '坐席对讲中 — 点此切回 AI 接待' : (sipId ? '对 SIP 腿启动 ASR→LLM→TTS' : '等待 SIP 来电'));
  }
  if (el.sipManualHangupBtn) {
    el.sipManualHangupBtn.disabled = !sipId || sipManualBusy;
  }
}

async function syncActiveSipCalls() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  if (pendingSipCallId || sipCallId) return;
  try {
    const res = await rpc('call.list', {});
    const calls = (res.calls || []).filter(c =>
      c.direction === 'inbound' &&
      c.from &&
      c.call_id !== callId &&
      c.state !== 'ended'
    );
    if (calls.length === 0) return;
    const pick = calls[0];
    pendingSipCallId = pick.call_id;
    el.sipCallIdShow.textContent = pick.call_id;
    if (getSipInboundMode() === 'notify') {
      setSipLegState('notify — 请点下方按钮', 'warn');
      refreshNotifyWaitingHint(pick.from);
    }
    updateSipManualButtons();
    logLine('warn', 'sip.sync', {
      call_id: pick.call_id,
      hint: 'recovered active SIP call (missed call.incoming — Connect 后再拨号可避免)',
    });
  } catch (e) {
    logLine('warn', 'sip.sync', e.message || String(e));
  }
}

function refreshNotifyWaitingHint(from) {
  if (getSipInboundMode() !== 'notify' || !el.sipDialHint) return;
  if (!(pendingSipCallId || sipCallId)) return;
  const who = from ? `（${from}）` : '';
  el.sipDialHint.textContent =
    `notify：来电已接通${who}，内核不会自动处理。请点下方「AI 接待」或「转人工」——不点则手机侧无声无 AI 回复。` +
    (callId ? ' WebRTC 已就绪，可直接 Bridge。' : ' 转人工会自动开麦克风。');
}

function refreshSipDialHint() {
  if (!el.sipDialHint) return;
  const mode = getSipInboundMode();
  const hints = {
    notify: 'notify：来电后只收事件，需手动点「AI 接待」或「转人工」。适合 AI 先接、接不住再转坐席。',
    bridge: 'bridge（人工坐席）：先 Agent 就绪开麦克风，再拨打 DID → 自动 call.bridge 对讲。',
    llm: kernelServerAutoLlm
      ? 'llm：内核 SIP_KERNEL_AUTO_LLM 已启用，拨打 DID 由内核自动 answer + LLM + TTS（无需 Explorer）。'
      : 'llm：直接拨打 DID → Explorer 对 SIP leg 自动 answer + listen + llm.chat + TTS（需内核 LLM_*）。',
  };
  let text = hints[mode] || hints.notify;
  if (kernelServerAutoLlm && mode !== 'llm') {
    text += ' 注意：内核 auto-llm 已启用，会抢先接管入站通话；测 bridge/notify 请将 SIP_KERNEL_AUTO_LLM=false 并重启。';
  }
  el.sipDialHint.textContent = text;
}

function renderSipStatus(info) {
  const sip = info?.sip;
  if (!sip?.enabled) {
    el.sipStatus.textContent = 'disabled — set SIP_LISTEN_ADDR on kernel';
    el.sipDialHint.textContent = '内核未监听 SIP。在服务器 .env 设置 SIP_LISTEN_ADDR=0.0.0.0:5060 并配置 SIP_TRUNK_* 后重启 flowkit。';
    if (el.agentReadyBtn) el.agentReadyBtn.disabled = true;
    return;
  }
  const did = sip.did || '(configure SIP_TRUNK_DID)';
  kernelServerAutoLlm = sip.kernel_auto_llm === true;
  if (!sipModeInitialized) {
    applyInitialSipMode(sip.inbound_mode);
    sipModeInitialized = true;
  }
  const clientMode = getSipInboundMode();
  const extras = [];
  if (kernelServerAutoLlm) extras.push('kernel auto-llm');
  else if (sip.inbound_mode && sip.inbound_mode !== clientMode) {
    extras.push(`hello hint ${sip.inbound_mode}`);
  }
  const extraHint = extras.length ? ` · ${extras.join(' · ')}` : '';
  el.sipStatus.textContent = `listen ${sip.listen || '?'} · DID ${did} · mode ${clientMode}${extraHint}`;
  updateSipModeUI();
  if (el.agentReadyBtn && ws?.readyState === WebSocket.OPEN && getSipInboundMode() === 'bridge') {
    el.agentReadyBtn.disabled = !!callId;
  }
}

function isStaleCallError(msg) {
  const s = String(msg || '').toLowerCase();
  return s.includes('ended') || s.includes('not found') || s.includes('invalid state');
}

async function ensureWebRTCAgentLeg() {
  if (callId && pc && pc.connectionState !== 'closed' && pc.connectionState !== 'failed') {
    return;
  }
  if (pc) {
    teardownCall('refresh_webrtc');
  }
  await startCall();
  if (!callId) {
    throw new Error('WebRTC 坐席建立失败');
  }
}

async function stopSipLlmForHandoff(sipId) {
  if (!sipId) return;
  clearTtsQueue();
  activeLlmTurnId = null;
  llmSplitter = null;
  llmVoiceTurn = false;
  llmVoiceCallId = null;
  bargeInPending = false;
  llmTurnBarged = false;
  sipLlmActive = false;
  sipListening = false;
  try {
    await rpc('call.stop', { call_id: sipId, target: 'all' });
  } catch (e) {
    logLine('warn', 'sip.handoff', { stop: e.message || e });
  }
  try {
    await rpc('llm.reset', { call_id: sipId });
  } catch (_) { /* optional */ }
  logLine('notif', 'sip.handoff', { call_id: sipId, hint: 'LLM/TTS/ASR stopped for bridge' });
}

async function bridgeSipToWebRTC(sipId) {
  if (!sipId) {
    logLine('err', 'call.bridge', 'missing SIP leg');
    return;
  }
  if (sipCallId === sipId && el.bridgeState?.classList.contains('ok')) return;
  setBridgeState('bridging', 'warn');
  setSipLegState('bridging', 'warn');
  try {
    await stopSipLlmForHandoff(sipId);
    if (!callId) {
      await ensureWebRTCAgentLeg();
    }
    await rpc('call.bridge', { call_id: sipId, other_call_id: callId });
    sipCallId = sipId;
    pendingSipCallId = null;
    sipBridgeMode = true;
    el.sipCallIdShow.textContent = sipId;
    setBridgeState('connected', 'ok');
    setSipLegState('bridged', 'ok');
    logLine('notif', 'sip.bridge', { sip: sipId, webrtc: callId });
  } catch (e) {
    const msg = e?.message || String(e);
    if (isStaleCallError(msg)) {
      logLine('warn', 'call.bridge', { retry: true, err: msg });
      try {
        await stopSipLlmForHandoff(sipId);
        await ensureWebRTCAgentLeg();
        await rpc('call.bridge', { call_id: sipId, other_call_id: callId });
        sipCallId = sipId;
        pendingSipCallId = null;
        sipBridgeMode = true;
        el.sipCallIdShow.textContent = sipId;
        setBridgeState('connected', 'ok');
        setSipLegState('bridged', 'ok');
        logLine('notif', 'sip.bridge', { sip: sipId, webrtc: callId, retried: true });
        updateSipManualButtons();
        return;
      } catch (e2) {
        setBridgeState('failed', 'bad');
        setSipLegState('bridge failed', 'bad');
        logLine('err', 'call.bridge', e2.message || e2);
        updateSipManualButtons();
        return;
      }
    }
    setBridgeState('failed', 'bad');
    setSipLegState('bridge failed', 'bad');
    logLine('err', 'call.bridge', msg);
  }
  updateSipManualButtons();
}

async function startSipLlmLeg(sipId) {
  if (!sipId) return;
  if (!llmEnabled) {
    setSipLegState('llm disabled', 'bad');
    logLine('err', 'sip.llm', 'set LLM_PROVIDER + LLM_API_KEY on kernel');
    return;
  }
  sipCallId = sipId;
  pendingSipCallId = null;
  sipLlmActive = true;
  el.sipCallIdShow.textContent = sipId;
  setSipLegState('llm starting', 'warn');
  try {
    await rpc('call.answer', { call_id: sipId });
    await rpc('call.listen', { call_id: sipId });
    sipListening = true;
    setSipLegState('llm listening', 'ok');
    logLine('notif', 'sip.llm', { call_id: sipId, hint: 'speak on phone' });
  } catch (e) {
    sipLlmActive = false;
    setSipLegState('llm failed', 'bad');
    logLine('err', 'sip.llm', e.message || e);
  }
  updateSipManualButtons();
}

async function hangupSipLeg(reason = 'user_hangup') {
  const id = sipCallId || pendingSipCallId;
  if (!id) return;
  try {
    await rpc('call.hangup', { call_id: id, reason });
  } catch (_) { /* logged */ }
  resetSipLeg(reason);
}

function resetSipLeg(reason) {
  const hadWebRTC = !!(callId && (sipBridgeMode || sipCallId));
  if (sipLlmActive) {
    clearTtsQueue();
    activeLlmTurnId = null;
    llmSplitter = null;
    llmVoiceTurn = false;
    llmVoiceCallId = null;
  }
  sipCallId = null;
  pendingSipCallId = null;
  sipLlmActive = false;
  sipListening = false;
  sipBridgeMode = false;
  if (el.sipCallIdShow) el.sipCallIdShow.textContent = '—';
  setBridgeState('idle', '');
  setSipLegState(reason ? `ended (${reason})` : 'idle', '');
  setAgentReadyButton(false);
  if (el.agentReadyBtn && getSipInboundMode() === 'bridge') el.agentReadyBtn.disabled = false;
  updateSipManualButtons();
  if (hadWebRTC) {
    teardownCall(reason || 'sip_ended');
  }
}

function handleSipIncoming(params) {
  if (params.transport !== 'sip') return;
  const incomingId = params.call_id;
  if (!incomingId) return;
  logLine('notif', 'sip.incoming', { from: params.from, call_id: incomingId, mode: getSipInboundMode() });

  // Trunk may retry with parallel legs; keep the first active LLM session.
  if (sipLlmActive && sipCallId && sipCallId !== incomingId) {
    logLine('warn', 'sip.incoming', { ignored: incomingId, active: sipCallId });
    return;
  }

  pendingSipCallId = incomingId;
  el.sipCallIdShow.textContent = incomingId;
  updateSipManualButtons();

  const mode = getSipInboundMode();
  if (mode === 'notify') {
    setSipLegState('notify — 请点下方按钮', 'warn');
    refreshNotifyWaitingHint(params.from);
    return;
  }
  if (mode === 'bridge') {
    if (callId) {
      setAgentReadyButton(false);
      bridgeSipToWebRTC(incomingId).catch(e => logLine('err', 'sip.bridge', e.message));
    } else {
      setSipLegState('waiting agent', 'warn');
      setBridgeState('waiting agent', 'warn');
      setAgentReadyButton(true);
    }
    return;
  }
  if (mode === 'llm') {
    if (kernelServerAutoLlm) {
      sipCallId = incomingId;
      el.sipCallIdShow.textContent = incomingId;
      setSipLegState('kernel auto-llm', 'ok');
      logLine('notif', 'sip.llm', { call_id: incomingId, hint: 'handled by kernel agent' });
      return;
    }
    startSipLlmLeg(incomingId).catch(e => logLine('err', 'sip.llm', e.message));
  }
}

async function agentReadyForSip() {
  if (getSipInboundMode() !== 'bridge') return;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    logLine('err', 'agentReady', '请先点 Connect 连上内核');
    setSipLegState('need connect', 'bad');
    return;
  }
  sipBridgeMode = true;
  setAgentReadyButton(!!pendingSipCallId);
  if (el.llmVoiceMode) el.llmVoiceMode.checked = false;
  if (el.agentReadyBtn) el.agentReadyBtn.disabled = true;
  setSipLegState('starting webrtc', 'warn');
  setBridgeState('starting webrtc', 'warn');
  try {
    await startCall();
    if (!callId) {
      throw new Error('WebRTC 未建立（见上方 Call state 与 Live log）');
    }
    if (pendingSipCallId) {
      await bridgeSipToWebRTC(pendingSipCallId);
      pendingSipCallId = null;
    } else {
      setSipLegState('webrtc ready — dial DID', 'ok');
      setBridgeState('agent ready', 'ok');
    }
    setAgentReadyButton(false);
  } catch (e) {
    sipBridgeMode = false;
    const msg = e?.message || String(e);
    setSipLegState('agent failed', 'bad');
    setBridgeState('failed', 'bad');
    logLine('err', 'agentReady', msg);
    if (el.agentReadyBtn) el.agentReadyBtn.disabled = false;
    setAgentReadyButton(!!pendingSipCallId);
  }
}

async function manualBridgeSip() {
  const sipId = sipCallId || pendingSipCallId;
  if (!sipId) {
    logLine('err', 'sip.manual', '等待 SIP 来电');
    return;
  }
  sipManualBusy = true;
  updateSipManualButtons();
  sipBridgeMode = true;
  setSipLegState('bridging…', 'warn');
  try {
    await bridgeSipToWebRTC(sipId);
    pendingSipCallId = null;
  } catch (e) {
    logLine('err', 'sip.manual.bridge', e.message || e);
  } finally {
    sipManualBusy = false;
    updateSipManualButtons();
  }
}

async function unbridgeSipFromAgent(sipId) {
  if (!sipId) return false;
  try {
    await rpc('call.unbridge', { call_id: sipId });
    sipBridgeMode = false;
    setBridgeState('idle', '');
    logLine('notif', 'sip.unbridge', { call_id: sipId });
    return true;
  } catch (e) {
    logLine('err', 'call.unbridge', e.message || e);
    return false;
  }
}

async function releaseWebRTCAgent(reason = 'handoff') {
  if (!callId) return;
  const id = callId;
  try {
    await rpc('call.hangup', { call_id: id, reason });
  } catch (_) { /* logged by rpc */ }
  teardownCall(reason);
}

async function manualLlmSip() {
  const sipId = sipCallId || pendingSipCallId;
  if (!sipId) return;
  sipManualBusy = true;
  updateSipManualButtons();
  setSipLegState('llm starting…', 'warn');
  logLine('notif', 'sip.manual.llm', { call_id: sipId, bridged: isSipAgentBridged() });
  try {
    if (isSipAgentBridged()) {
      if (!(await unbridgeSipFromAgent(sipId))) {
        setSipLegState('unbridge failed', 'bad');
        return;
      }
      await releaseWebRTCAgent('handoff_to_llm');
    }
    try {
      await rpc('llm.reset', { call_id: sipId });
    } catch (_) { /* optional */ }
    await startSipLlmLeg(sipId);
    if (el.sipDialHint && sipLlmActive) {
      el.sipDialHint.textContent = 'AI 接待中：对着手机说话。接不住可点「转人工」。';
    }
  } finally {
    sipManualBusy = false;
    updateSipManualButtons();
  }
}

function iceServersFromHello(info) {
  const urls = info?.ice_servers;
  if (Array.isArray(urls) && urls.length > 0) {
    return urls.map((u) => ({ urls: u }));
  }
  return DEFAULT_ICE_SERVERS.slice();
}

function prewarmIce() {
  if (icePrewarmPC) {
    try { icePrewarmPC.close(); } catch (_) { /* ignore */ }
  }
  icePrewarmPC = new RTCPeerConnection({
    iceServers: clientIceServers,
    iceCandidatePoolSize: 10,
  });
}

function takePrewarmPC() {
  const pc = icePrewarmPC;
  icePrewarmPC = null;
  return pc;
}

function queueLocalIceCandidate(candidate) {
  if (callId) {
    rpcNotify('webrtc.ice', { call_id: callId, candidate });
  } else {
    pendingLocalICE.push(candidate);
  }
}

function flushPendingLocalICE() {
  if (!callId) return;
  for (const candidate of pendingLocalICE) {
    rpcNotify('webrtc.ice', { call_id: callId, candidate });
  }
  pendingLocalICE = [];
}

function msSince(t0) {
  return `${Math.round(performance.now() - t0)}ms`;
}

function ts() {
  const d = new Date();
  return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}
function logLine(kind, label, payload) {
  if (el.hideHealth?.checked && label === 'system.health') return;
  const line = document.createElement('span');
  line.className = `log-line ${kind}`;
  let arrow = '·';
  if (kind === 'req') arrow = '→';
  else if (kind === 'res') arrow = '←';
  else if (kind === 'err') arrow = '✗';
  else if (kind === 'notif') arrow = '⇠';
  const body = payload == null ? '' : ' ' + JSON.stringify(payload);
  line.textContent = `[${ts()}] ${arrow} ${label}${body}\n`;
  el.log.appendChild(line);
  el.log.scrollTop = el.log.scrollHeight;
}

// --- RPC -------------------------------------------------------------
function rpc(method, params) {
  return new Promise((resolve, reject) => {
    const id = `e-${nextRpcId++}`;
    pending.set(id, { method, resolve, reject });
    const frame = { jsonrpc: '2.0', id, method, params: params || {} };
    logLine('req', method, params);
    ws.send(JSON.stringify(frame));
  });
}
function rpcNotify(method, params) {
  const frame = { jsonrpc: '2.0', method, params: params || {} };
  logLine('req', method + ' (notify)', params);
  ws.send(JSON.stringify(frame));
}

function onWsMessage(ev) {
  let msg;
  try { msg = JSON.parse(ev.data); }
  catch { logLine('err', 'non-JSON frame', ev.data); return; }

  if (msg.id && (msg.result !== undefined || msg.error)) {
    const h = pending.get(msg.id);
    if (!h) { logLine('err', 'unmatched response', msg); return; }
    pending.delete(msg.id);
    if (msg.error) {
      logLine('err', `${h.method} error`, msg.error);
      h.reject(msg.error);
    } else {
      logLine('res', `${h.method} ok`, msg.result);
      h.resolve(msg.result);
    }
    return;
  }
  if (msg.method) {
    onNotification(msg.method, msg.params || {});
  }
}

function onNotification(method, params) {
  logLine('notif', method, params);
  switch (method) {
    case 'webrtc.ice':
      if (pc && params.candidate) {
        pc.addIceCandidate(params.candidate).catch(e => logLine('err', 'addIceCandidate', e.message));
      }
      break;
    case 'call.answered':
      setCallState('answered', 'ok');
      enableActiveCallButtons(true);
      if (el.llmVoiceMode?.checked && !sipBridgeMode) {
        startListen().catch(e => logLine('err', 'auto.listen', e.message));
      }
      break;
    case 'call.play_end':
      el.playIdShow.textContent = `${params.play_id || '?'} (${params.reason || ''}, ${params.played_ms || 0}ms)`;
      if (ttsPlaying && (!ttsActiveCallId || params.call_id === ttsActiveCallId)) {
        ttsPlaying = false;
        ttsActiveCallId = null;
        setTtsQueueState();
        drainTtsQueue();
      }
      break;
    case 'call.speech_start':
      if (params.call_id && params.call_id !== callId && params.call_id !== sipCallId) break;
      bargeInPending = false;
      pushPartial(params.call_id === sipCallId ? '[SIP] ' : '');
      el.asrState.textContent = 'speaking';
      el.asrState.className = 'pill warn';
      break;
    case 'call.speech_partial':
      if (params.call_id && params.call_id !== callId && params.call_id !== sipCallId) break;
      replaceLastPartial((params.call_id === sipCallId ? '[SIP] ' : '') + (params.text || ''));
      if (params.call_id === sipCallId && sipLlmActive && !sipBridgeMode) {
        maybeBargeIn(params.text || '', sipCallId);
      } else {
        maybeBargeIn(params.text || '', callId);
      }
      break;
    case 'call.speech_end':
      if (params.call_id && params.call_id !== callId && params.call_id !== sipCallId) break;
      promoteToFinal((params.call_id === sipCallId ? '[SIP] ' : '') + (params.text || ''));
      lastAsrFinal = params.text || '';
      el.llmUseAsrBtn.disabled = !lastAsrFinal;
      if (params.call_id === sipCallId && sipLlmActive && !sipBridgeMode) {
        el.asrState.textContent = sipListening ? 'sip listening' : 'idle';
        el.asrState.className = sipListening ? 'pill ok' : 'pill';
        if (lastAsrFinal) {
          llmChat(lastAsrFinal, sipCallId).catch(e => logLine('err', 'sip.llm.voice', e.message));
        }
      } else if (params.call_id === sipCallId && sipBridgeMode) {
        el.asrState.textContent = 'bridged';
        el.asrState.className = 'pill ok';
      } else {
        el.asrState.textContent = listening ? 'listening' : 'idle';
        el.asrState.className = listening ? 'pill ok' : 'pill';
        if (el.llmVoiceMode?.checked && lastAsrFinal && callId) {
          llmChat(lastAsrFinal, callId).catch(e => logLine('err', 'llm.voice', e.message));
        }
      }
      break;
    case 'call.error':
      logLine('err', 'call.error', params);
      break;
    case 'llm.token':
      if (params.turn_id === activeLlmTurnId && !sipBridgeMode) {
        if (params.delta && llmSplitter) llmSplitter.feed(params.delta);
      }
      if (!sipBridgeMode) replaceLlmAssistantDraft(params.text || '');
      break;
    case 'llm.done':
      if (params.turn_id === activeLlmTurnId) {
        if (!sipBridgeMode && llmSplitter && !llmTurnBarged) llmSplitter.flush();
        llmSplitter = null;
        llmVoiceTurn = false;
        activeLlmTurnId = null;
      }
      if (!sipBridgeMode) finalizeLlmAssistant(params.text || '');
      setLlmState('done', 'ok');
      {
        const w = llmTurnWaiters.get(params.turn_id);
        if (w) {
          llmTurnWaiters.delete(params.turn_id);
          w.resolve(params.text || '');
        }
      }
      break;
    case 'llm.error':
      if (params.turn_id === activeLlmTurnId) {
        llmSplitter = null;
        llmVoiceTurn = false;
        activeLlmTurnId = null;
        clearTtsQueue();
      }
      setLlmState('failed', 'bad');
      logLine('err', 'llm.error', params);
      {
        const w = llmTurnWaiters.get(params.turn_id);
        if (w) {
          llmTurnWaiters.delete(params.turn_id);
          w.reject(new Error(params.message || 'llm error'));
        }
      }
      break;
    case 'call.hangup':
      if (params.call_id === sipCallId || params.call_id === pendingSipCallId) {
        resetSipLeg(params.reason || 'remote_hangup');
      }
      if (params.call_id === callId) {
        teardownCall('remote_hangup');
      }
      break;
    case 'call.incoming':
      handleSipIncoming(params);
      break;
    case 'call.bridged':
      if (params.other_call_id) {
        setBridgeState('connected', 'ok');
        sipBridgeMode = true;
        if (params.call_id && params.call_id !== callId) {
          sipCallId = params.call_id;
          el.sipCallIdShow.textContent = params.call_id;
        }
        updateSipManualButtons();
      }
      break;
    case 'call.unbridged':
      if (params.call_id === sipCallId || params.call_id === callId) {
        sipBridgeMode = false;
        setBridgeState('idle', '');
        updateSipManualButtons();
      }
      break;
    case 'system.health':
      // hidden by default toggle
      break;
  }
}

// --- speech area helpers --------------------------------------------
function renderSpeech() {
  if (speechBuffer.length === 0) {
    el.speechArea.innerHTML = '<em class="muted">(transcripts appear here)</em>';
    return;
  }
  el.speechArea.innerHTML = '';
  for (const item of speechBuffer) {
    const div = document.createElement('div');
    div.className = item.type;
    div.textContent = item.text || (item.type === 'partial' ? '…' : '');
    el.speechArea.appendChild(div);
  }
}
function pushPartial(text) {
  speechBuffer.push({ type: 'partial', text });
  renderSpeech();
}
function replaceLastPartial(text) {
  const last = speechBuffer[speechBuffer.length - 1];
  if (last && last.type === 'partial') last.text = text;
  else speechBuffer.push({ type: 'partial', text });
  renderSpeech();
}
function promoteToFinal(text) {
  const last = speechBuffer[speechBuffer.length - 1];
  if (last && last.type === 'partial') {
    last.type = 'final';
    last.text = text;
  } else {
    speechBuffer.push({ type: 'final', text });
  }
  renderSpeech();
}

// --- UI state helpers -----------------------------------------------
function setWsState(text, kind) {
  el.wsState.textContent = text;
  el.wsState.className = 'pill' + (kind ? ' ' + kind : '');
}
function setCallState(text, kind) {
  el.callState.textContent = text;
  el.callState.className = 'pill' + (kind ? ' ' + kind : '');
}
function enableActiveCallButtons(on) {
  el.sayBtn.disabled = !on;
  el.listenBtn.disabled = !on;
  el.hangupBtn.disabled = !on;
}

// --- sentence splitter (mirrors internal/llm/splitter.go) ------------
function createSentenceSplitter(maxLen, onSentence) {
  let buf = '';
  const hard = '。！？.!?；;\n';
  const soft = '，,、 ';
  function runeIndexToByteIndex(s, runeIdx) {
    let count = 0;
    for (let i = 0; i < s.length; i++) {
      if (count === runeIdx) return i;
      count++;
    }
    return s.length;
  }
  function findBoundary(s) {
    const runes = [...s];
    for (let i = 0; i < runes.length; i++) {
      if (hard.includes(runes[i])) return runeIndexToByteIndex(s, i + 1);
      if (i + 1 >= maxLen && soft.includes(runes[i])) return runeIndexToByteIndex(s, i + 1);
    }
    return -1;
  }
  return {
    feed(text) {
      buf += text;
      for (;;) {
        const idx = findBoundary(buf);
        if (idx < 0) return;
        const sentence = buf.slice(0, idx).trim();
        buf = buf.slice(idx);
        if (sentence) onSentence(sentence);
      }
    },
    flush() {
      const rest = buf.trim();
      buf = '';
      if (rest) onSentence(rest);
    },
  };
}

function setTtsQueueState() {
  if (!el.ttsQueueState) return;
  if (ttsPlaying) {
    el.ttsQueueState.textContent = 'playing';
    el.ttsQueueState.className = 'pill warn';
  } else if (ttsQueue.length > 0) {
    el.ttsQueueState.textContent = `queued ${ttsQueue.length}`;
    el.ttsQueueState.className = 'pill ok';
  } else {
    el.ttsQueueState.textContent = 'idle';
    el.ttsQueueState.className = 'pill';
  }
}

function enqueueTtsSentence(text, targetCallId) {
  const cid = targetCallId || callId;
  if (!cid || !text.trim()) return;
  if (cid === sipCallId && sipBridgeMode) return;
  ttsQueue.push({ callId: cid, text: text.trim() });
  setTtsQueueState();
  drainTtsQueue();
}

function clearTtsQueue() {
  ttsQueue = [];
  ttsPlaying = false;
  ttsActiveCallId = null;
  setTtsQueueState();
}

async function drainTtsQueue() {
  if (ttsPlaying || ttsQueue.length === 0) return;
  const item = ttsQueue[0];
  if (!item?.callId) {
    ttsQueue.shift();
    return drainTtsQueue();
  }
  ttsPlaying = true;
  ttsActiveCallId = item.callId;
  setTtsQueueState();
  ttsQueue.shift();
  try {
    const res = await rpc('call.say', {
      call_id: item.callId,
      text: item.text,
      voice: el.sayVoice.value || undefined,
    });
    el.playIdShow.textContent = `${item.callId.slice(0, 8)}… ${res.play_id} (playing…)`;
  } catch (_) {
    ttsPlaying = false;
    ttsActiveCallId = null;
    setTtsQueueState();
    drainTtsQueue();
  }
}

async function maybeBargeIn(text, targetCallId) {
  const cid = targetCallId || (el.llmVoiceMode?.checked ? callId : null);
  if (!cid) return;
  const sipLeg = cid === sipCallId;
  if (!sipLeg && !el.llmVoiceMode?.checked) return;
  if (!text.trim() || bargeInPending) return;
  if (!ttsPlaying && ttsQueue.length === 0 && !activeLlmTurnId) return;
  bargeInPending = true;
  llmTurnBarged = true;
  clearTtsQueue();
  try {
    await rpc('call.stop', { call_id: cid, target: 'play' });
    logLine('notif', 'barge-in', { call_id: cid, text: text.slice(0, 20) });
  } catch (e) {
    logLine('err', 'barge-in', e.message || e);
  }
}

function beginLlmVoiceTurn(turnId, voiceCallId) {
  activeLlmTurnId = turnId;
  llmTurnBarged = false;
  llmVoiceCallId = voiceCallId || (el.llmVoiceMode?.checked ? callId : null);
  llmVoiceTurn = !!llmVoiceCallId;
  llmSplitter = llmVoiceTurn
    ? createSentenceSplitter(80, (s) => enqueueTtsSentence(s, llmVoiceCallId))
    : null;
  if (llmVoiceTurn) clearTtsQueue();
}

// --- connect / disconnect -------------------------------------------
function wsOpenWithTimeout(socket, ms) {
  return new Promise((ok, fail) => {
    if (socket.readyState === WebSocket.OPEN) return ok();
    const timer = setTimeout(() => fail(new Error(`ws connect timeout (${ms}ms)`)), ms);
    socket.addEventListener('open', () => { clearTimeout(timer); ok(); }, { once: true });
    socket.addEventListener('error', () => { clearTimeout(timer); fail(new Error('ws connect failed')); }, { once: true });
  });
}

async function connect() {
  if (ws && ws.readyState === WebSocket.OPEN) return;
  setWsState('connecting', 'warn');
  el.connect.disabled = true;
  const url = el.url.value.trim();
  kernelHost = kernelHostFromWsUrl(url);
  if (!url) {
    logLine('err', 'connect', 'Kernel URL is empty');
    setWsState('idle', '');
    el.connect.disabled = false;
    return;
  }

  try {
    // Browsers cannot set an Authorization header on a WebSocket, so the key
    // goes in the query string, which the kernel also accepts.
    const apiKey = el.apiKey ? el.apiKey.value.trim() : '';
    let wsUrl = url;
    if (apiKey) {
      const u = new URL(url);
      u.searchParams.set('api_key', apiKey);
      wsUrl = u.toString();
    }
    ws = new WebSocket(wsUrl);
    ws.onmessage = onWsMessage;
    ws.onclose = () => {
      setWsState('closed', 'bad');
      el.connect.disabled = false;
      el.disconnect.disabled = true;
      el.startCall.disabled = true;
      enableActiveCallButtons(false);
      pending.forEach(h => h.reject(new Error('ws closed')));
      pending.clear();
      ws = null;
    };
    ws.onerror = () => logLine('err', 'ws error', null);

    await wsOpenWithTimeout(ws, 15000);
    setWsState('open', 'warn');

    const info = await rpc('system.hello', {
      sdk: 'flowkit-api-explorer',
      version: '0.1.0',
      capabilities: ['webrtc-trickle'],
    });
    clientIceServers = iceServersFromHello(info);
    prewarmIce();
    el.serverInfo.textContent = JSON.stringify(info, null, 2);
    renderSipStatus(info);
    await syncActiveSipCalls();
    setWsState('ready', 'ok');
    el.disconnect.disabled = false;
    el.startCall.disabled = false;
    if (el.agentReadyBtn && info?.sip?.enabled && getSipInboundMode() === 'bridge') {
      el.agentReadyBtn.disabled = false;
    }
    await refreshLlmStatus();
  } catch (e) {
    const msg = e?.message || String(e);
    logLine('err', 'connect', msg);
    setWsState('failed', 'bad');
    if (ws) {
      try { ws.close(); } catch (_) { /* ignore */ }
      ws = null;
    }
    el.connect.disabled = false;
    el.disconnect.disabled = true;
    el.startCall.disabled = true;
  }
}
function disconnect() {
  if (pc) { pc.close(); pc = null; }
  if (icePrewarmPC) { icePrewarmPC.close(); icePrewarmPC = null; }
  pendingLocalICE = [];
  if (ws) ws.close();
}

// --- start call (trickle ICE — no 5s gather wait) -------------------
async function startCall() {
  el.startCall.disabled = true;
  setCallState('starting', 'warn');
  const t0 = performance.now();

  let mic;
  try {
    mic = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
  } catch (e) {
    logLine('err', 'getUserMedia', e.message);
    setCallState('mic_denied', 'bad');
    el.startCall.disabled = false;
    throw new Error(`麦克风不可用：${e.message}（请用 http://localhost 打开页面，不要用 file://）`);
  }
  logLine('notif', 'startCall', { phase: 'mic', ms: msSince(t0) });

  pc = takePrewarmPC() || new RTCPeerConnection({
    iceServers: clientIceServers,
    iceCandidatePoolSize: 10,
  });
  pc.onconnectionstatechange = () => {
    el.pcState.textContent = pc.connectionState;
    if (pc.connectionState === 'connected') {
      setCallState('connected', 'ok');
      logLine('notif', 'startCall', { phase: 'pc.connected', ms: msSince(t0) });
    }
    if (pc.connectionState === 'failed') setCallState('failed', 'bad');
  };
  pc.oniceconnectionstatechange = () => {
    el.iceState.textContent = pc.iceConnectionState;
  };
  pc.ontrack = (ev) => {
    logLine('notif', 'remote track', { kind: ev.track.kind });
    el.remoteAudio.srcObject = ev.streams[0];
  };
  pc.onicecandidate = (ev) => {
    if (ev.candidate) queueLocalIceCandidate(ev.candidate);
  };

  for (const t of mic.getAudioTracks()) pc.addTrack(t, mic);

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  logLine('notif', 'startCall', { phase: 'offer.local', ms: msSince(t0) });

  let res;
  try {
    res = await rpc('webrtc.offer', { sdp: pc.localDescription.sdp });
  } catch (e) {
    setCallState('offer_failed', 'bad');
    el.startCall.disabled = false;
    throw new Error(`webrtc.offer 失败：${e.message || e}`);
  }
  callId = res.call_id;
  el.callIdShow.textContent = callId;
  flushPendingLocalICE();
  logLine('notif', 'startCall', { phase: 'offer.rpc', ms: msSince(t0) });

  await pc.setRemoteDescription({ type: 'answer', sdp: res.sdp });
  setCallState('negotiating', 'warn');

  try {
    await rpc('call.answer', { call_id: callId });
  } catch (e) {
    setCallState('answer_failed', 'bad');
  }
  enableActiveCallButtons(true);
  prewarmIce(); // next Start call reuses a warm pool
  logLine('notif', 'startCall', { phase: 'answer.rpc', ms: msSince(t0) });
  if (pendingSipCallId && getSipInboundMode() === 'bridge') {
    await bridgeSipToWebRTC(pendingSipCallId);
    pendingSipCallId = null;
  }
  updateSipManualButtons();
}

function setLlmState(text, kind) {
  el.llmState.textContent = text;
  el.llmState.className = 'pill' + (kind ? ' ' + kind : '');
}
function renderLlmChat() {
  if (llmChatLog.length === 0) {
    el.llmOutput.innerHTML = '<em class="muted">(对话记录出现在这里)</em>';
    return;
  }
  el.llmOutput.innerHTML = '';
  for (const item of llmChatLog) {
    const div = document.createElement('div');
    div.className = item.role;
    div.textContent = (item.role === 'user' ? '你: ' : 'AI: ') + (item.text || '…');
    el.llmOutput.appendChild(div);
  }
  el.llmOutput.scrollTop = el.llmOutput.scrollHeight;
}
function pushLlmUser(text) {
  llmChatLog.push({ role: 'user', text });
  llmChatLog.push({ role: 'assistant', text: '' });
  renderLlmChat();
}
function replaceLlmAssistantDraft(text) {
  for (let i = llmChatLog.length - 1; i >= 0; i--) {
    if (llmChatLog[i].role === 'assistant') {
      llmChatLog[i].text = text;
      break;
    }
  }
  renderLlmChat();
}
function finalizeLlmAssistant(text) {
  llmReplyText = text;
  for (let i = llmChatLog.length - 1; i >= 0; i--) {
    if (llmChatLog[i].role === 'assistant') {
      llmChatLog[i].text = text;
      break;
    }
  }
  renderLlmChat();
}
function lastFinalTranscript() {
  for (let i = speechBuffer.length - 1; i >= 0; i--) {
    if (speechBuffer[i].type === 'final' && speechBuffer[i].text) {
      return speechBuffer[i].text;
    }
  }
  return lastAsrFinal;
}

async function refreshLlmStatus() {
  try {
    const st = await rpc('llm.status', {});
    llmEnabled = !!st.enabled;
    if (st.enabled) {
      const prov = st.provider ? `${st.provider}/` : '';
      el.llmInfo.textContent = `${prov}${st.model} (server config)`;
      el.llmSendBtn.disabled = false;
      el.llmResetBtn.disabled = false;
    } else {
      el.llmInfo.textContent = 'disabled — set LLM_PROVIDER + LLM_API_KEY on kernel';
      el.llmSendBtn.disabled = true;
      el.llmResetBtn.disabled = true;
    }
  } catch (e) {
    el.llmInfo.textContent = 'llm.status failed';
    llmEnabled = false;
  }
}

function waitLlmTurn(turnId, timeoutMs = 90000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      llmTurnWaiters.delete(turnId);
      reject(new Error('llm turn timeout'));
    }, timeoutMs);
    llmTurnWaiters.set(turnId, {
      resolve: (text) => { clearTimeout(timer); resolve(text); },
      reject: (err) => { clearTimeout(timer); reject(err); },
    });
  });
}

async function llmChat(userText, voiceCallId) {
  const text = (userText ?? el.llmUser.value).trim();
  if (!text) throw new Error('message is empty');
  if (!llmEnabled) throw new Error('LLM not enabled on kernel');

  pushLlmUser(text);
  el.llmUser.value = '';
  setLlmState('streaming', 'warn');
  llmReplyText = '';

  const params = { text };
  const cid = voiceCallId || callId;
  if (cid) params.call_id = cid;

  const res = await rpc('llm.chat', params);
  beginLlmVoiceTurn(res.turn_id, cid);
  return waitLlmTurn(res.turn_id);
}

async function resetLlmChat() {
  const params = {};
  if (callId) params.call_id = callId;
  await rpc('llm.reset', params);
  llmChatLog = [];
  llmReplyText = '';
  activeLlmTurnId = null;
  llmSplitter = null;
  llmVoiceTurn = false;
  clearTtsQueue();
  renderLlmChat();
  setLlmState('idle', '');
}

function useLastAsrForLlm() {
  const text = lastFinalTranscript();
  if (!text) return;
  el.llmUser.value = text;
}

// --- say (TTS) -------------------------------------------------------
async function sayText(text) {
  if (!callId || !text.trim()) return;
  el.sayBtn.disabled = true;
  try {
    const res = await rpc('call.say', {
      call_id: callId,
      text: text.trim(),
      voice: el.sayVoice.value || undefined,
    });
    el.playIdShow.textContent = res.play_id + ' (playing…)';
  } catch (_) {
    // logged by rpc
  } finally {
    el.sayBtn.disabled = false;
  }
}

async function say() {
  if (!callId) return;
  await sayText(el.sayText.value);
}

// --- listen (ASR) ----------------------------------------------------
async function startListen() {
  if (!callId) return;
  el.listenBtn.disabled = true;
  try {
    await rpc('call.listen', { call_id: callId });
    listening = true;
    el.stopListenBtn.disabled = false;
    el.asrState.textContent = 'listening';
    el.asrState.className = 'pill ok';
  } catch (e) {
    el.listenBtn.disabled = false;
  }
}
async function stopListen() {
  if (!callId) return;
  el.stopListenBtn.disabled = true;
  try {
    await rpc('call.stop', { call_id: callId, target: 'listen' });
  } catch (_) { /* logged */ }
  listening = false;
  el.listenBtn.disabled = false;
  el.asrState.textContent = 'idle';
  el.asrState.className = 'pill';
}

// --- hangup ----------------------------------------------------------
async function hangup() {
  el.hangupBtn.disabled = true;
  if (el.sipManualHangupBtn) el.sipManualHangupBtn.disabled = true;
  const sipId = sipCallId || pendingSipCallId;
  try {
    if (sipId) {
      await hangupSipLeg('user_hangup');
      return;
    }
    if (!callId) return;
    await rpc('call.hangup', { call_id: callId, reason: 'user_hangup' });
    teardownCall('user_hangup');
  } catch (_) {
    el.hangupBtn.disabled = false;
    updateSipManualButtons();
  }
}
function teardownCall(reason) {
  if (pc) { pc.close(); pc = null; }
  callId = null;
  pendingLocalICE = [];
  sipBridgeMode = false;
  el.callIdShow.textContent = '—';
  el.pcState.textContent = '—';
  el.iceState.textContent = '—';
  setCallState('ended (' + reason + ')', '');
  enableActiveCallButtons(false);
  el.startCall.disabled = false;
  listening = false;
  if (!sipCallId) {
    speechBuffer = [];
    lastAsrFinal = '';
  }
  if (!sipLlmActive) {
    activeLlmTurnId = null;
    llmSplitter = null;
    llmVoiceTurn = false;
    llmVoiceCallId = null;
    bargeInPending = false;
    if (!sipCallId) clearTtsQueue();
  }
  renderSpeech();
  el.llmUseAsrBtn.disabled = true;
  if (!sipLlmActive) {
    el.asrState.textContent = 'idle';
    el.asrState.className = 'pill';
  }
  if (el.agentReadyBtn && getSipInboundMode() === 'bridge') el.agentReadyBtn.disabled = false;
  updateSipManualButtons();
}

// --- boot -----------------------------------------------------------
function initFromQuery() {
  const p = new URLSearchParams(location.search);
  const ws = p.get('ws');
  const host = p.get('host');
  if (ws) el.url.value = ws;
  else if (host) el.url.value = `ws://${host.replace(/^https?:\/\//, '').split(':')[0]}:9600/ws`;
  // sip_mode applied in applyInitialSipMode() on first hello
}

function boot() {
  initFromQuery();
  updateSipModeUI();
  if (!window.isSecureContext) {
    logLine('err', 'browser', '页面需通过 http://localhost 或 https 打开；file:// 无法使用麦克风');
    if (el.sipDialHint) {
      el.sipDialHint.textContent = '⚠ 请用 python3 -m http.server 8080 -d examples/api_explorer 启动后访问 http://localhost:8080，不要用 file:// 双击打开。';
    }
  }
  if (!el.connect || !el.url) {
    console.error('api_explorer: missing DOM elements — is main.js loaded?');
    return;
  }
  el.connect.addEventListener('click', () => connect().catch(e => logLine('err', 'connect', e.message)));
  el.disconnect.addEventListener('click', disconnect);
  el.startCall.addEventListener('click', () => startCall().catch(e => logLine('err', 'startCall', e.message)));
  if (el.agentReadyBtn) {
    el.agentReadyBtn.addEventListener('click', () => agentReadyForSip().catch(e => logLine('err', 'agentReady', e.message)));
  }
  if (el.sipInboundMode) {
    el.sipInboundMode.addEventListener('change', () => {
      saveSipInboundMode(getSipInboundMode());
      updateSipModeUI();
    });
  }
  if (el.sipManualBridgeBtn) {
    el.sipManualBridgeBtn.addEventListener('click', () => manualBridgeSip().catch(e => logLine('err', 'sip.manual.bridge', e.message)));
  }
  if (el.sipManualLlmBtn) {
    el.sipManualLlmBtn.addEventListener('click', () => manualLlmSip().catch(e => logLine('err', 'sip.manual.llm', e.message)));
  }
  if (el.sipManualHangupBtn) {
    el.sipManualHangupBtn.addEventListener('click', () => hangupSipLeg().catch(e => logLine('err', 'sip.hangup', e.message)));
  }
  el.sayBtn.addEventListener('click', say);
  el.listenBtn.addEventListener('click', startListen);
  el.stopListenBtn.addEventListener('click', stopListen);
  el.hangupBtn.addEventListener('click', hangup);
  el.llmSendBtn.addEventListener('click', () => llmChat().catch(e => logLine('err', 'llm.chat', e.message)));
  el.llmUseAsrBtn.addEventListener('click', useLastAsrForLlm);
  el.llmResetBtn.addEventListener('click', () => resetLlmChat().catch(e => logLine('err', 'llm.reset', e.message)));
  el.clearLog.addEventListener('click', () => { el.log.innerHTML = ''; });
  logLine('notif', 'ready', { url: el.url.value, hint: 'Click Connect or fix Kernel URL if remote' });
}

boot();
