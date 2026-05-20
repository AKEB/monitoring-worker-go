package handlers

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"monitoring-worker-go/internal/model"
)

func RunHTTP(ctx context.Context, job model.Job) map[string]any {
	method := job.Method
	if method == "" {
		method = http.MethodGet
	}
	timeoutSec := capTimeout(job.Timeout)
	clientTimeout := time.Duration(timeoutSec) * time.Second

	verify := truthy(job.SSLVerify, true)
	verifyHost := truthy(job.SSLVerifyHost, true)
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: !(verify && verifyHost), MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: clientTimeout,
	}
	defer tr.CloseIdleConnections()
	if job.ProxyHost != "" {
		if pu, err := parseProxyURL(job.ProxyHost); err == nil {
			tr.Proxy = http.ProxyURL(pu)
		}
	}
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

	req, err := http.NewRequestWithContext(ctx, method, job.URL, nil)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = err.Error()
		respMap["response_error_num"] = 1
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
	for _, h := range parseHeaders(job.RequestHeaders) {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) == 2 {
			req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = err.Error()
		respMap["response_error_num"] = 2
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	defer resp.Body.Close()

	var bodyStr string
	if job.Type == JobTypeHTTPJSON {
		readLimit := int64(2 << 20)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, readLimit))
		bodyStr = string(body)
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}

	respMap["status_code"] = resp.StatusCode
	respMap["response_error"] = ""
	respMap["response_error_num"] = 0
	respMap["total_time_us"] = time.Since(start).Microseconds()
	if resp.StatusCode < 200 || resp.StatusCode >= 500 {
		respMap["status"] = 0
	}

	if job.Type == JobTypeHTTPJSON {
		applyJSONCheck(respMap, bodyStr, job)
	}
	return respMap
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
