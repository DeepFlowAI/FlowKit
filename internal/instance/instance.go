// Package instance 提供 flowkit 内核实例的全局标识——
//
// 用途：
//  1. 分布式部署时区分多个 flowkit 副本（k8s pod-1 / pod-2 ...）
//  2. 客户端拿到 session_id 后能直连 owner 实例（跳过 LB 也行）
//  3. 日志/trace 自带 instance_id，方便定位"哪个 pod 出问题"
//
// 设计：
//
//   - INSTANCE_ID 优先级：环境变量 > hostname > 随机短串
//   - INSTANCE_URL：客户配置（如 https://flowkit-pod-1.svc:8080），
//     如果没配就只返回 ID 不返回 URL（客户端走 LB）
//
// 分布式部署的 sticky 路由原理：
//
//	WebRTC 媒体流：天然 sticky（DTLS+SRTP 状态绑定到进程内）
//	v1 API：客户端通过 LB 一致性哈希按 session_id 路由
//	         （nginx: hash $arg_id consistent / k8s: ingress sticky-by-cookie）
//	         或者客户端直接用 instance_url 跳过 LB
package instance

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// HeaderInstanceID 是所有 v1 API 响应都会带的 header。
// 客户排查问题时用这个找到"是哪个 flowkit pod 处理的"。
const HeaderInstanceID = "X-Flowkit-Instance"

// Info 单实例的标识信息——main.go 启动时构造一次，全局共享。
type Info struct {
	// ID 实例唯一 ID，例如：
	//   "flowkit-7c8d9e1f"  (hostname 来源)
	//   "flowkit-a3b4c5d6"  (随机来源)
	//   "pod-east-1-abc"    (用户通过 INSTANCE_ID 显式指定)
	ID string

	// URL 该实例的可直连 URL，例如 "https://flowkit-pod-1.svc.cluster.local:8080"。
	// 空字符串表示未配置（客户端必须走 LB）。
	URL string
}

// New 从环境变量构造 Info——
//
//	INSTANCE_ID   显式指定（推荐 k8s 用 pod name 或 hostname）
//	INSTANCE_URL  本实例的 base URL（用于让客户端直连）
//
// 都没配时：ID 用 hostname；hostname 拿不到就生成一段随机串。
func New() Info {
	id := os.Getenv("INSTANCE_ID")
	if id == "" {
		id = derivedID()
	}
	return Info{
		ID:  id,
		URL: strings.TrimRight(os.Getenv("INSTANCE_URL"), "/"),
	}
}

// ResolvePublicIP picks the address advertised in WebRTC ICE host candidates.
// Priority: explicit configured IP → hostname from INSTANCE_URL (if not loopback).
func ResolvePublicIP(configured, instanceURL string) string {
	if ip := strings.TrimSpace(configured); ip != "" {
		return ip
	}
	if instanceURL == "" {
		return ""
	}
	u, err := url.Parse(instanceURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	switch host {
	case "", "localhost", "127.0.0.1", "0.0.0.0", "::1":
		return ""
	}
	return host
}

// derivedID 从 hostname 派生，否则用随机字节兜底。
func derivedID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		// k8s pod name 通常已经够辨识，直接用
		return h
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "flowkit-" + hex.EncodeToString(b)
}

// HeaderMiddleware 给所有 HTTP 响应自动加 X-Flowkit-Instance header。
// 包在 http.Handler 外面就行：
//
//	http.Handle("/", info.HeaderMiddleware(rootHandler))
func (i Info) HeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderInstanceID, i.ID)
		next.ServeHTTP(w, r)
	})
}
