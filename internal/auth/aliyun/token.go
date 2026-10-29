// Package auth provides shared Aliyun NLS authentication (TokenManager)
// used by both the ASR and TTS adapters.
//
// TokenManager —
//   - Mints an NLS Token from AccessKey ID/Secret via the nls-meta API
//   - Caches the token, auto-refreshes 1 hour before expiry
//   - Thread-safe
//
// Why not the official Aliyun SDK?
//   - Heavy dependency tree
//   - We only need one API (CreateToken); hand-rolling is lighter
//   - Aliyun POP V1 signing matches many other Chinese cloud APIs
//
// Usage:
//
//	tm, err := auth.NewTokenManager(akID, akSecret, "cn-shanghai")
//	if err != nil { panic(err) }
//	token, err := tm.Get()  // auto-refresh
package auth

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TokenManager 缓存并自动刷新 NLS Token。
type TokenManager struct {
	accessKeyID     string
	accessKeySecret string
	region          string // cn-shanghai / cn-beijing / cn-shenzhen
	httpClient      *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// NewTokenManager 创建 Token 管理器。第一次调用 Get() 时才真正请求 Token。
func NewTokenManager(accessKeyID, accessKeySecret, region string) (*TokenManager, error) {
	if accessKeyID == "" || accessKeySecret == "" {
		return nil, fmt.Errorf("AccessKey ID/Secret 不能为空")
	}
	if region == "" {
		region = "cn-shanghai"
	}
	return &TokenManager{
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,
		region:          region,
		httpClient:      &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Get 返回有效的 Token；过期前 1 小时会自动刷新。
func (tm *TokenManager) Get() (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 缓存有效（剩余 > 1 小时）则直接返回
	if tm.token != "" && time.Until(tm.expiresAt) > time.Hour {
		return tm.token, nil
	}

	tok, expiresAt, err := tm.requestToken()
	if err != nil {
		return "", fmt.Errorf("刷新 Token 失败: %w", err)
	}
	tm.token = tok
	tm.expiresAt = expiresAt
	return tok, nil
}

// requestToken 调 nls-meta CreateToken API。
//
// 接口文档：https://help.aliyun.com/document_detail/450255.html
//
// 请求示例：
//
//	GET https://nls-meta.cn-shanghai.aliyuncs.com/?
//	  AccessKeyId=...&Action=CreateToken&Format=JSON&...&Signature=...
//
// 响应：
//
//	{"Token":{"Id":"abcdef...","ExpireTime":1234567890,...},"NlsRequestId":"..."}
func (tm *TokenManager) requestToken() (string, time.Time, error) {
	host := fmt.Sprintf("nls-meta.%s.aliyuncs.com", tm.region)

	params := map[string]string{
		"AccessKeyId":      tm.accessKeyID,
		"Action":           "CreateToken",
		"Format":           "JSON",
		"RegionId":         tm.region,
		"SignatureMethod":  "HMAC-SHA1",
		"SignatureNonce":   uuid.NewString(),
		"SignatureVersion": "1.0",
		"Timestamp":        time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		"Version":          "2019-02-28",
	}
	signature := signParams(http.MethodGet, params, tm.accessKeySecret)
	params["Signature"] = signature

	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}
	endpoint := "https://" + host + "/?" + q.Encode()

	resp, err := tm.httpClient.Get(endpoint)
	if err != nil {
		return "", time.Time{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", time.Time{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var parsed struct {
		Token struct {
			ID         string `json:"Id"`
			ExpireTime int64  `json:"ExpireTime"` // Unix 秒
		} `json:"Token"`
		ErrMsg string `json:"ErrMsg"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", time.Time{}, fmt.Errorf("解析响应: %w（原文 %s）", err, string(body))
	}
	if parsed.Token.ID == "" {
		return "", time.Time{}, fmt.Errorf("接口返回空 Token，原文: %s", string(body))
	}
	return parsed.Token.ID, time.Unix(parsed.Token.ExpireTime, 0), nil
}

// ═══════════════════════ Aliyun POP API V1 签名 ═══════════════════════

// signParams 生成 Aliyun POP API V1 签名（HMAC-SHA1）。
//
// 算法：
//  1. 把 params 按 key 排序，按 popURLEncode 编码后用 = 连接每个键值对，再用 & 拼接 → canonical
//  2. stringToSign = HTTPMethod + "&" + popURLEncode("/") + "&" + popURLEncode(canonical)
//  3. signKey = AccessKeySecret + "&"
//  4. signature = Base64(HMAC-SHA1(signKey, stringToSign))
//
// 这就是著名的"V1 签名"，AWS/Aliyun/腾讯云早期 API 都长这样。
func signParams(httpMethod string, params map[string]string, secret string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var canonical strings.Builder
	for i, k := range keys {
		if i > 0 {
			canonical.WriteString("&")
		}
		canonical.WriteString(popURLEncode(k))
		canonical.WriteString("=")
		canonical.WriteString(popURLEncode(params[k]))
	}

	stringToSign := httpMethod + "&" + popURLEncode("/") + "&" + popURLEncode(canonical.String())

	mac := hmac.New(sha1.New, []byte(secret+"&"))
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// popURLEncode 是 Aliyun POP API 专用的 URL 编码。
//
// 和标准 url.QueryEscape 的差异：
//   - 空格编成 %20（标准是 +）
//   - * 编成 %2A
//   - %7E（即 ~）保持不变
//
// 这是 Aliyun 文档里写明的特殊编码规则。
func popURLEncode(s string) string {
	encoded := url.QueryEscape(s)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}
