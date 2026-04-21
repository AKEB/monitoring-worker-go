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
	respMap := map[string]any{"status": 1, "status_code": 0, "response_unixtime": time.Now().Unix()}

	portStr := toPort(job.Port)
	scheme := "http"
	if job.TLS {
		scheme = "https"
	}
	reqURL := scheme + "://" + job.Host + ":" + portStr + "/containers/json?all=true"

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
		if job.TLSCAFile != "" {
			raw, err := base64.StdEncoding.DecodeString(job.TLSCAFile)
			if err != nil {
				cfg.DockerLogf("decode tls_ca_file (base64): %v", err)
			} else if len(raw) > 0 {
				if f := writeTempPEM("ca", raw); f != "" {
					cleanup = append(cleanup, f)
					if b, err := os.ReadFile(f); err != nil {
						cfg.DockerLogf("read temp CA file: %v", err)
					} else if ok := tlsCfg.RootCAs.AppendCertsFromPEM(b); !ok {
						cfg.DockerLogf("AppendCertsFromPEM(CA): no certs parsed from PEM")
					} else {
						cfg.DockerLogf("loaded CA PEM into pool (%d bytes)", len(b))
					}
				} else {
					cfg.DockerLogf("failed to create temp file for CA PEM")
				}
			}
		}
		var certFile, keyFile string
		if job.TLSCertificate != "" {
			if raw, err := base64.StdEncoding.DecodeString(job.TLSCertificate); err != nil {
				cfg.DockerLogf("decode tls_certificate (base64): %v", err)
			} else if len(raw) > 0 {
				if f := writeTempPEM("cert", raw); f != "" {
					cleanup = append(cleanup, f)
					certFile = f
				}
			}
		}
		if job.TLSKey != "" {
			if raw, err := base64.StdEncoding.DecodeString(job.TLSKey); err != nil {
				cfg.DockerLogf("decode tls_key (base64): %v", err)
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
	if pu, err := effectiveDockerProxyURL(job, cfg); err == nil && pu != nil {
		tr.Proxy = http.ProxyURL(pu)
		cfg.DockerLogf("HTTP transport ProxyURL set to scheme=%q host=%q", pu.Scheme, pu.Host)
	} else if err != nil && err != io.EOF {
		cfg.DockerLogf("proxy URL parse: %v", err)
	}
	tr.IdleConnTimeout = 90 * time.Second
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
		cfg.DockerLogf("HTTP Do error: %v", err)
		return respMap
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	bodyStr := string(b)
	respMap["status_code"] = resp.StatusCode
	respMap["body"] = decodeDockerBodyForState(bodyStr)
	respMap["response_error_num"] = 0
	respMap["response_error"] = ""
	respMap["total_time_us"] = time.Since(start).Microseconds()
	if resp.StatusCode >= 500 {
		respMap["status"] = 0
	}

	cfg.DockerLogf("response: status=%q code=%d proto=%s content_length=%d body_len=%d total_time_us=%d",
		resp.Status, resp.StatusCode, resp.Proto, resp.ContentLength, len(bodyStr), respMap["total_time_us"])
	cfg.DockerLogf("response body preview (first 800 chars): %s", truncateForLog(bodyStr, 800))
	if resp.StatusCode >= 400 {
		cfg.DockerLogf("non-success HTTP status: worker maps status=0 only for code>=500; 4xx still status=1 in body map (see response_finish for job outcome)")
	}

	return respMap
}

func effectiveDockerProxyURL(job model.Job, cfg *config.Config) (*url.URL, error) {
	ph := strings.TrimSpace(job.ProxyHost)
	if ph == "" {
		ph = strings.TrimSpace(cfg.ProxyHost)
	}
	if ph == "" {
		return nil, io.EOF
	}
	return parseProxyURL(ph)
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

func decodeDockerBodyForState(body string) any {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return body
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(trimmed), &arr); err == nil {
		return arr
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(trimmed), &obj); err == nil {
		return obj
	}
	return body
}
