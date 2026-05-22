package handlers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"monitoring-worker-go/internal/config"
	"monitoring-worker-go/internal/model"
)

func RunDocker(ctx context.Context, job model.Job) map[string]any {
	cfg := config.Get()
	start := time.Now()
	timeout := capTimeout(job.Timeout)
	respMap := map[string]any{"status": 1, "status_code": 0}

	portStr := toPort(job.Port)
	scheme := "http"
	if job.TLS {
		scheme = "https"
	}
	host := normalizeDockerHost(job.Host)
	reqURL := scheme + "://" + host + ":" + portStr + "/containers/json?all=true"

	cfg.DockerLogf("job job_id=%d monitor id=%d host=%q port=%s tls=%v timeout_sec=%d",
		job.JobID, job.ID, job.Host, portStr, job.TLS, timeout)
	cfg.DockerLogf("request URL: %s", reqURL)
	cfg.DockerLogf("tls_ca_file present=%v len=%d tls_certificate present=%v tls_key present=%v",
		job.TLSCAFile != "", approxB64Len(job.TLSCAFile),
		job.TLSCertificate != "", job.TLSKey != "")
	proxy := strings.TrimSpace(job.ProxyHost)
	if proxy == "" {
		proxy = strings.TrimSpace(cfg.ProxyHost)
	}
	if proxy != "" {
		cfg.DockerLogf("proxy effective=%q (job override or global PROXY_HOST)", redactProxy(proxy))
	} else {
		cfg.DockerLogf("proxy: none")
	}

	var cleanup []string
	defer func() {
		for _, p := range cleanup {
			_ = os.Remove(p)
		}
	}()

	var tr *http.Transport
	if job.TLS {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if sn := strings.TrimSpace(job.TLSServerName); sn != "" {
			tlsCfg.ServerName = sn
			cfg.DockerLogf("TLS ServerName from tls_server_name: %q (host=%q)", sn, job.Host)
		} else if sn := tlsServerName(job.Host); sn != "" {
			tlsCfg.ServerName = sn
			cfg.DockerLogf("TLS ServerName from host: %q", sn)
		} else if job.TLS && net.ParseIP(strings.Trim(job.Host, "[]")) != nil {
			cfg.DockerLogf("TLS ServerName: not set (host is IP); set tls_server_name if daemon cert is DNS-only")
		}
		if pool, err := x509.SystemCertPool(); err == nil {
			tlsCfg.RootCAs = pool
		} else {
			tlsCfg.RootCAs = x509.NewCertPool()
			cfg.DockerLogf("system cert pool: %v, using empty pool", err)
		}
		caLoaded := false
		if job.TLSCAFile != "" {
			raw, err := decodePEMField(job.TLSCAFile)
			if err != nil {
				cfg.DockerLogf("decode tls_ca_file: %v", err)
				cfg.Infof("[docker-tls] job_id=%d docker_id=%d: tls_ca_file decode failed: %v", job.JobID, job.ID, err)
			} else if len(raw) > 0 {
				if ok := tlsCfg.RootCAs.AppendCertsFromPEM(raw); !ok {
					cfg.DockerLogf("AppendCertsFromPEM(CA): no certs parsed from PEM")
					cfg.Infof("[docker-tls] job_id=%d docker_id=%d: tls_ca_file is not a valid CA PEM", job.JobID, job.ID)
				} else {
					caLoaded = true
					cfg.DockerLogf("loaded CA PEM into pool (%d bytes)", len(raw))
				}
			}
		}
		if !caLoaded {
			cfg.Infof("[docker-tls] job_id=%d docker_id=%d host=%q: no custom CA loaded (tls_ca_file empty or invalid; upload CA in Docker host settings or re-save the host on server)", job.JobID, job.ID, job.Host)
		}
		var certFile, keyFile string
		if job.TLSCertificate != "" {
			raw, err := decodePEMField(job.TLSCertificate)
			if err != nil {
				cfg.DockerLogf("decode tls_certificate: %v", err)
			} else if len(raw) > 0 {
				if f := writeTempPEM("cert", raw); f != "" {
					cleanup = append(cleanup, f)
					certFile = f
				}
			}
		}
		if job.TLSKey != "" {
			raw, err := decodePEMField(job.TLSKey)
			if err != nil {
				cfg.DockerLogf("decode tls_key: %v", err)
			} else if len(raw) > 0 {
				if f := writeTempPEM("key", raw); f != "" {
					cleanup = append(cleanup, f)
					keyFile = f
				}
			}
		}
		if certFile != "" && keyFile != "" {
			c, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				cfg.DockerLogf("LoadX509KeyPair: %v", err)
			} else {
				tlsCfg.Certificates = []tls.Certificate{c}
				cfg.DockerLogf("client certificate+key loaded")
			}
		} else {
			cfg.DockerLogf("client cert/key: certFile=%q keyFile=%q (both needed for mTLS)", certFile, keyFile)
		}
		tr = &http.Transport{TLSClientConfig: tlsCfg}
	} else {
		tr = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
		}
		cfg.DockerLogf("TLS disabled for Docker: InsecureSkipVerify=true (non-TLS job)")
	}
	proxyHost := strings.TrimSpace(job.ProxyHost)
	proxyType := job.ProxyType
	if proxyHost == "" {
		proxyHost = strings.TrimSpace(cfg.ProxyHost)
		proxyType = cfg.ProxyType
	}
	configureTransportProxy(tr, proxyHost, proxyType, cfg.DockerLogf)
	configureTransportTimeouts(tr, timeout)
	defer tr.CloseIdleConnections()

	client := &http.Client{Transport: tr, Timeout: time.Duration(timeout) * time.Second}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = err.Error()
		respMap["total_time_us"] = time.Since(start).Microseconds()
		cfg.DockerLogf("NewRequestWithContext error: %v", err)
		return respMap
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
	req.Header.Set("Accept", "application/json")

	cfg.DockerLogf("HTTP Do start (client.Timeout=%s)", client.Timeout)
	resp, err := client.Do(req)
	if err != nil {
		respMap["status"] = 0
		respMap["status_code"] = 500
		respMap["response_error_num"] = 500
		respMap["response_error"] = err.Error()
		respMap["total_time_us"] = time.Since(start).Microseconds()
		cfg.LogHTTPFailure("docker", http.MethodGet, reqURL, 0, err, nil)
		cfg.DockerLogf("HTTP Do error: %v", err)
		return respMap
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	bodyStr := string(b)
	totalUS := time.Since(start).Microseconds()
	respMap["status_code"] = resp.StatusCode
	respMap["response_error_num"] = 0
	respMap["response_error"] = ""
	respMap["total_time_us"] = totalUS
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		cfg.LogHTTPFailure("docker", http.MethodGet, reqURL, resp.StatusCode, nil, b)
		respMap["status"] = 0
		if respMap["response_error"] == "" {
			respMap["response_error"] = fmt.Sprintf("unexpected HTTP status %d", resp.StatusCode)
		}
	} else {
		states, ok := dockerContainersForState(bodyStr, totalUS)
		if !ok {
			respMap["status"] = 0
			respMap["response_error"] = "invalid Docker Engine containers/json response"
		} else if len(states) == 0 {
			respMap["status"] = 0
			respMap["response_error"] = "no containers in Docker Engine response"
		} else {
			respMap["container_states"] = states
		}
	}

	cfg.DockerLogf("response: status=%q code=%d proto=%s content_length=%d body_len=%d total_time_us=%d",
		resp.Status, resp.StatusCode, resp.Proto, resp.ContentLength, len(bodyStr), respMap["total_time_us"])
	cfg.DockerLogf("response body preview (first 800 chars): %s", truncateForLog(bodyStr, 800))
	if resp.StatusCode >= 400 {
		cfg.DockerLogf("non-success HTTP status: worker maps status=0 only for code>=500; 4xx still status=1 in body map (see response_finish for job outcome)")
	}

	return respMap
}

func tlsServerName(host string) string {
	h := strings.Trim(host, "[]")
	if net.ParseIP(h) != nil {
		return ""
	}
	return h
}

func approxB64Len(s string) int {
	// decoded length upper bound for logging only
	if s == "" {
		return 0
	}
	n := base64.StdEncoding.DecodedLen(len(s))
	return n
}

func redactProxy(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || u.User == nil {
		return s
	}
	u.User = url.UserPassword("***", "***")
	return u.String()
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf(" … (truncated, total %d)", len(s))
}

// normalizeDockerHost strips schemes/extra ports and brackets IPv6 for URL building.
func normalizeDockerHost(host string) string {
	host = strings.TrimSpace(host)
	for _, p := range []string{"tcp://", "https://", "http://"} {
		host = strings.TrimPrefix(host, p)
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if ip := net.ParseIP(host); ip != nil && strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
}

// DockerContainerStatesCount returns how many containers are in a worker response map.
func DockerContainerStatesCount(resp map[string]any) int {
	if resp == nil {
		return 0
	}
	v, ok := resp["container_states"]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case []map[string]any:
		return len(t)
	case []any:
		return len(t)
	default:
		return 0
	}
}

// DockerResponseAcceptedByServer matches State.php updateDockerState() requirements.
func DockerResponseAcceptedByServer(resp map[string]any) bool {
	if resp == nil {
		return false
	}
	switch s := resp["status"].(type) {
	case int:
		if s != 1 {
			return false
		}
	case int64:
		if s != 1 {
			return false
		}
	case float64:
		if int(s) != 1 {
			return false
		}
	default:
		return false
	}
	code, ok := resp["status_code"]
	if !ok {
		return false
	}
	var statusCode int
	switch c := code.(type) {
	case int:
		statusCode = c
	case int64:
		statusCode = int(c)
	case float64:
		statusCode = int(c)
	default:
		return false
	}
	if statusCode < 200 || statusCode >= 300 {
		return false
	}
	return DockerContainerStatesCount(resp) > 0
}

func toPort(p int) string {
	if p <= 0 {
		return "2375"
	}
	return strconv.Itoa(p)
}

func writeTempPEM(suffix string, data []byte) string {
	f, err := os.CreateTemp("", "mw-docker-*_"+suffix+".pem")
	if err != nil {
		return ""
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return ""
	}
	_ = f.Close()
	return f.Name()
}

// dockerContainersForState builds the compact payload stored by monitoring State.php
// (protocol 2.0), matching the legacy body[] parsing on the server.
func dockerContainersForState(body string, totalTimeUS int64) ([]map[string]any, bool) {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return nil, false
	}
	var raw []map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, false
	}
	out := make([]map[string]any, 0, len(raw))
	for _, c := range raw {
		if cs := compactDockerContainer(c, totalTimeUS); cs != nil {
			out = append(out, cs)
		}
	}
	return out, true
}

func compactDockerContainer(c map[string]any, totalTimeUS int64) map[string]any {
	name := dockerContainerName(c)
	if name == "" {
		return nil
	}
	return map[string]any{
		"id":            dockerFieldString(c, "Id"),
		"image":         dockerFieldString(c, "Image"),
		"image_id":      dockerFieldString(c, "ImageID"),
		"name":          name,
		"state":         dockerFieldString(c, "State"),
		"status":        dockerFieldString(c, "Status"),
		"created":       dockerFieldAny(c, "Created"),
		"ports":         dockerFormatPorts(c),
		"total_time_us": totalTimeUS,
	}
}

func dockerContainerName(c map[string]any) string {
	names, ok := c["Names"].([]any)
	if !ok || len(names) == 0 {
		return ""
	}
	s, _ := names[0].(string)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "/") {
		return s[1:]
	}
	return s
}

func dockerFormatPorts(c map[string]any) string {
	portsRaw, ok := c["Ports"].([]any)
	if !ok {
		return ""
	}
	ports := make([]string, 0, len(portsRaw))
	for _, p := range portsRaw {
		pm, ok := p.(map[string]any)
		if !ok || pm == nil {
			continue
		}
		ip := dockerFieldString(pm, "IP")
		if ip == "" {
			continue
		}
		pub := dockerFieldInt(pm, "PublicPort")
		if pub == 0 {
			continue
		}
		portWithHost := ""
		if ip == "::" || ip == "0.0.0.0" {
			portWithHost = strconv.Itoa(pub)
		} else {
			portWithHost = ip + ":" + strconv.Itoa(pub)
		}
		if typ := dockerFieldString(pm, "Type"); typ != "" {
			portWithHost += "/" + typ
		}
		ports = append(ports, portWithHost)
	}
	return strings.Join(ports, ", ")
}

func dockerFieldString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(v)
	}
}

func dockerFieldInt(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	default:
		return 0
	}
}

func dockerFieldAny(m map[string]any, key string) any {
	v, ok := m[key]
	if !ok {
		return 0
	}
	return v
}
