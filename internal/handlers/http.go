package handlers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"monitoring-worker-go/internal/config"
	"monitoring-worker-go/internal/model"
)

type oauthTokenCacheEntry struct {
	token     string
	tokenType string
	expiresAt time.Time
}

var oauthTokenCache sync.Map // key: tokenURL+"|"+clientID -> oauthTokenCacheEntry

func RunHTTP(ctx context.Context, job model.Job) map[string]any {
	method := job.Method
	if method == "" {
		method = http.MethodGet
	}
	timeoutSec := capTimeout(job.Timeout)
	clientTimeout := time.Duration(timeoutSec) * time.Second

	verify := truthy(job.SSLVerify, true)
	verifyHost := truthy(job.SSLVerifyHost, true)
	tlsCfg := &tls.Config{InsecureSkipVerify: !(verify && verifyHost), MinVersion: tls.VersionTLS12}
	if err := applyHTTPClientTLS(tlsCfg, job); err != nil {
		return map[string]any{
			"status":             0,
			"status_code":        0,
			"response_error":     truncateResponseError(err.Error()),
			"response_error_num": errNumBadRequest,
		}
	}
	tr := &http.Transport{
		TLSClientConfig:   tlsCfg,
		ForceAttemptHTTP2: true,
		MaxIdleConns:      10,
	}
	defer tr.CloseIdleConnections()
	cfg := config.Get()
	proxyHost := job.ProxyHost
	proxyType := job.ProxyType
	if proxyHost == "" {
		proxyHost = cfg.ProxyHost
		proxyType = cfg.ProxyType
	}
	curlLog := func(format string, args ...any) { cfg.CurlLogf(format, args...) }
	configureTransportProxy(tr, proxyHost, proxyType, curlLog)
	configureTransportTimeouts(tr, timeoutSec)
	maxRedir := job.MaxRedirects
	if maxRedir <= 0 {
		maxRedir = 10
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   clientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedir {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	start := time.Now()
	respMap := map[string]any{"status": 1, "status_code": 0}

	reqURL := job.URL
	if job.CacheBust {
		reqURL = appendCacheBust(reqURL)
	}

	var bodyReader io.Reader
	bodyStr := strings.TrimRight(job.RequestBody, "\r\n")
	if bodyStr != "" {
		bodyReader = strings.NewReader(bodyStr)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = truncateResponseError(err.Error())
		respMap["response_error_num"] = errNumBadRequest
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
	if cfg.CurlDebug {
		cfg.CurlLogf("%s %s proxy_type=%s", method, reqURL, proxyType)
	}
	for _, h := range parseHeaders(job.RequestHeaders) {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) == 2 {
			req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}
	applyBodyContentType(req, job, bodyStr != "")
	if err := applyHTTPAuth(ctx, client, req, job); err != nil {
		respMap["status"] = 0
		respMap["response_error"] = truncateResponseError(err.Error())
		respMap["response_error_num"] = errNumBadRequest
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}

	resp, err := client.Do(req)
	if err != nil {
		num, msg := classifyProbeError(err)
		respMap["status"] = 0
		respMap["status_code"] = 0
		respMap["response_error"] = msg
		respMap["response_error_num"] = num
		respMap["total_time_us"] = time.Since(start).Microseconds()
		cfg.LogHTTPFailure("monitor", method, reqURL, 0, err, nil)
		return respMap
	}
	defer resp.Body.Close()
	if cfg.CurlDebug {
		cfg.CurlLogf("response %s %d %s", reqURL, resp.StatusCode, resp.Proto)
	}

	needBody := job.Type == JobTypeHTTPJSON || job.Type == JobTypeHTTPKeyword || strings.TrimSpace(job.Keyword) != ""
	var respBodyStr string
	var errBody []byte
	if needBody {
		readLimit := int64(2 << 20)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, readLimit))
		respBodyStr = string(body)
		errBody = body
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ = io.ReadAll(io.LimitReader(resp.Body, 800))
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}

	respMap["status_code"] = resp.StatusCode
	respMap["response_error"] = ""
	respMap["response_error_num"] = 0
	respMap["total_time_us"] = time.Since(start).Microseconds()
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		respMap["cert_expire"] = resp.TLS.PeerCertificates[0].NotAfter.Unix()
	}
	// 3xx/4xx stay status=1: server applies monitor.response_code. Only log hard failures.
	if resp.StatusCode < 200 || resp.StatusCode >= 500 {
		cfg.LogHTTPFailure("monitor", method, reqURL, resp.StatusCode, nil, errBody)
		respMap["status"] = 0
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		respMap["response_error_num"] = resp.StatusCode
		respMap["response_error"] = formatHTTPStatusError(resp.StatusCode, errBody)
	}

	if job.Type == JobTypeHTTPJSON {
		applyJSONCheck(respMap, respBodyStr, job)
	}
	if job.Type == JobTypeHTTPKeyword || (job.Type == JobTypeHTTPS && strings.TrimSpace(job.Keyword) != "") {
		applyKeywordCheck(respMap, respBodyStr, job)
	}
	// Optional response header Log: custom error text for alerts (same contract as External).
	if logMsg := formatResponseLogHeader(resp.Header); logMsg != "" {
		respMap["response_error"] = logMsg
	}
	return respMap
}

func applyBodyContentType(req *http.Request, job model.Job, hasBody bool) {
	if !hasBody || req.Header.Get("Content-Type") != "" {
		return
	}
	switch strings.ToLower(strings.TrimSpace(job.HTTPBodyEncoding)) {
	case "", "json":
		req.Header.Set("Content-Type", "application/json")
	case "form":
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	case "xml":
		req.Header.Set("Content-Type", "application/xml")
	case "raw":
		// no auto Content-Type
	default:
		req.Header.Set("Content-Type", "application/json")
	}
}

func applyHTTPAuth(ctx context.Context, client *http.Client, req *http.Request, job model.Job) error {
	method := strings.ToLower(strings.TrimSpace(job.AuthMethod))
	switch method {
	case "", "none":
		return nil
	case "basic":
		req.SetBasicAuth(job.BasicAuthUser, job.BasicAuthPass)
		return nil
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(job.BearerToken))
		return nil
	case "oauth2_cc":
		tok, tokType, err := fetchOAuth2CCToken(ctx, client, job)
		if err != nil {
			return err
		}
		if tokType == "" {
			tokType = "Bearer"
		}
		req.Header.Set("Authorization", tokType+" "+tok)
		return nil
	case "mtls":
		// certificates applied on transport; nothing on request
		return nil
	default:
		return nil
	}
}

func fetchOAuth2CCToken(ctx context.Context, client *http.Client, job model.Job) (string, string, error) {
	cacheKey := job.OAuthTokenURL + "|" + job.OAuthClientID
	if v, ok := oauthTokenCache.Load(cacheKey); ok {
		ent := v.(oauthTokenCacheEntry)
		if time.Now().Before(ent.expiresAt.Add(-30 * time.Second)) {
			return ent.token, ent.tokenType, nil
		}
	}
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	if strings.TrimSpace(job.OAuthScopes) != "" {
		form.Set("scope", strings.TrimSpace(job.OAuthScopes))
	}
	if strings.TrimSpace(job.OAuthAudience) != "" {
		form.Set("audience", strings.TrimSpace(job.OAuthAudience))
	}
	authMethod := strings.ToLower(strings.TrimSpace(job.OAuthAuthMethod))
	if authMethod == "" {
		authMethod = "client_secret_basic"
	}
	if authMethod == "client_secret_post" {
		form.Set("client_id", job.OAuthClientID)
		form.Set("client_secret", job.OAuthClientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.OAuthTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if authMethod != "client_secret_post" {
		req.SetBasicAuth(job.OAuthClientID, job.OAuthClientSecret)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", &httpAuthError{msg: "oauth token HTTP " + strconv.Itoa(resp.StatusCode)}
	}
	var parsed struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", "", err
	}
	if parsed.AccessToken == "" {
		return "", "", &httpAuthError{msg: "oauth token missing access_token"}
	}
	exp := time.Now().Add(time.Hour)
	if parsed.ExpiresIn > 0 {
		exp = time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second)
	}
	oauthTokenCache.Store(cacheKey, oauthTokenCacheEntry{
		token:     parsed.AccessToken,
		tokenType: parsed.TokenType,
		expiresAt: exp,
	})
	return parsed.AccessToken, parsed.TokenType, nil
}

type httpAuthError struct{ msg string }

func (e *httpAuthError) Error() string { return e.msg }

func applyHTTPClientTLS(tlsCfg *tls.Config, job model.Job) error {
	method := strings.ToLower(strings.TrimSpace(job.AuthMethod))
	needClient := method == "mtls" || strings.TrimSpace(job.HTTPTLSCert) != "" || strings.TrimSpace(job.HTTPTLSKey) != ""
	if strings.TrimSpace(job.HTTPTLSCA) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(job.HTTPTLSCA)) {
			return &httpAuthError{msg: "invalid http_tls_ca PEM"}
		}
		tlsCfg.RootCAs = pool
		// custom CA implies we want verification against that pool
		if truthy(job.SSLVerify, true) {
			tlsCfg.InsecureSkipVerify = false
		}
	}
	if !needClient {
		return nil
	}
	certPEM := strings.TrimSpace(job.HTTPTLSCert)
	keyPEM := strings.TrimSpace(job.HTTPTLSKey)
	if certPEM == "" || keyPEM == "" {
		if method == "mtls" {
			return &httpAuthError{msg: "mtls requires http_tls_cert and http_tls_key"}
		}
		return nil
	}
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return err
	}
	tlsCfg.Certificates = []tls.Certificate{cert}
	return nil
}

func appendCacheBust(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		sep := "?"
		if strings.Contains(rawURL, "?") {
			sep = "&"
		}
		return rawURL + sep + "_=" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	q := u.Query()
	q.Set("_", strconv.FormatInt(time.Now().UnixMilli(), 10))
	u.RawQuery = q.Encode()
	return u.String()
}

func applyKeywordCheck(resp map[string]any, body string, job model.Job) {
	kw := job.Keyword
	resp["keyword_valid"] = 0
	if strings.TrimSpace(kw) == "" {
		resp["response_error"] = "Keyword is empty"
		return
	}
	found := strings.Contains(body, kw)
	ok := found
	if job.KeywordInvert {
		ok = !found
	}
	if ok {
		resp["keyword_valid"] = 1
		if v, _ := asRespInt(resp["status"]); v == 1 {
			resp["response_error"] = ""
		}
		return
	}
	if job.KeywordInvert {
		resp["response_error"] = "Keyword found but invert is enabled"
	} else {
		resp["response_error"] = "Keyword not found"
	}
}

func asRespInt(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	default:
		return 0, false
	}
}

func parseProxyURL(host string) (*url.URL, error) {
	h := strings.TrimSpace(host)
	if h == "" {
		return nil, io.EOF
	}
	if !strings.Contains(h, "://") {
		h = "http://" + h
	}
	return url.Parse(h)
}

func truthy(v any, def bool) bool {
	if v == nil {
		return def
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.TrimSpace(strings.ToLower(t))
		if s == "" {
			return def
		}
		return s == "1" || s == "true" || s == "yes"
	case float64:
		return t != 0
	case int:
		return t != 0
	default:
		return def
	}
}

func parseHeaders(raw string) []string {
	if raw == "" {
		return nil
	}
	lines := strings.Split(strings.ReplaceAll(raw, "\r", ""), "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" || !strings.Contains(l, ":") {
			continue
		}
		out = append(out, l)
	}
	return out
}

// formatResponseLogHeader reads optional Log response header from the checked resource.
// Returns "Log: …" for response_error / notifications, or "" if absent.
func formatResponseLogHeader(h http.Header) string {
	if h == nil {
		return ""
	}
	raw := strings.TrimSpace(h.Get("Log"))
	if raw == "" {
		return ""
	}
	raw = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\t' {
			return -1
		}
		return r
	}, raw)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	const maxRunes = 240
	runes := []rune(raw)
	if len(runes) > maxRunes {
		raw = string(runes[:maxRunes-3]) + "..."
	}
	return "Log: " + raw
}

func applyJSONCheck(resp map[string]any, body string, job model.Job) {
	resp["json_valid"] = 0
	if strings.TrimSpace(job.JsonPath) == "" {
		resp["response_error"] = "JSON path is empty"
		return
	}
	bodyForJSON := strings.TrimLeft(body, " \t\n\r")
	bodyForJSON = strings.TrimPrefix(bodyForJSON, "\ufeff")
	if i := strings.IndexAny(bodyForJSON, "{["); i > 0 {
		bodyForJSON = bodyForJSON[i:]
	}
	var decoded any
	if err := json.Unmarshal([]byte(bodyForJSON), &decoded); err != nil {
		preview := strings.TrimSpace(bodyForJSON)
		if len(preview) > 140 {
			preview = preview[:140]
		}
		resp["response_error"] = "Response is not valid JSON: " + preview
		return
	}
	root := jsonRootToMap(decoded)
	actual, found := getByDot(root, job.JsonPath)
	if !found {
		resp["response_error"] = "JSON path not found: " + job.JsonPath
		return
	}
	actualStr := toString(actual)
	if compareJSON(actual, job.JsonExpected, defaultString(job.JsonType, "string")) {
		resp["json_valid"] = 1
		resp["response_error"] = ""
	} else {
		resp["response_error"] = "JSON check failed at \"" + job.JsonPath + "\": expected (" +
			defaultString(job.JsonType, "string") + ") \"" + job.JsonExpected + "\", got \"" + actualStr + "\""
	}
}

func jsonRootToMap(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		return t
	case []any:
		m := make(map[string]any, len(t))
		for i, el := range t {
			m[strconv.Itoa(i)] = el
		}
		return m
	default:
		return map[string]any{}
	}
}

func getByDot(m map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var cur any = m
	for _, p := range parts {
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[p]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			idx, err := strconv.Atoi(p)
			if err != nil || idx < 0 || idx >= len(node) {
				return nil, false
			}
			cur = node[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}

func compareJSON(actual any, expected string, t string) bool {
	switch t {
	case "bool":
		b := strings.EqualFold(expected, "true") || expected == "1"
		ab := false
		switch v := actual.(type) {
		case bool:
			ab = v
		case string:
			ab = strings.EqualFold(v, "true") || v == "1"
		case float64:
			ab = v != 0
		}
		return ab == b
	case "number":
		a, _ := strconv.ParseFloat(toString(actual), 64)
		e, _ := strconv.ParseFloat(expected, 64)
		return math.Abs(a-e) < 0.0000001
	default:
		return toString(actual) == expected
	}
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func capTimeout(v int) int {
	if v < 1 {
		return 15
	}
	if v > 60 {
		return 60
	}
	return v
}

func defaultString(v string, def string) string {
	if v == "" {
		return def
	}
	return v
}
