package handlers

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"monitoring-worker-go/internal/model"
)

func RunSMTP(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	addr := fmt.Sprintf("%s:%d", job.Host, job.Port)

	conn, err := smtpDial(ctx, addr, job)
	if err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	br := bufio.NewReader(conn)
	code, banner, err := smtpReadResponse(br)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if code != 220 {
		resp["status"] = 0
		resp["status_code"] = code
		resp["response_error_num"] = code
		resp["response_error"] = truncateResponseError("SMTP banner: " + banner)
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	if err := smtpSendLine(conn, "EHLO monitoring-worker"); err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	code, ehloResp, err := smtpReadResponse(br)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if code != 250 {
		resp["status"] = 0
		resp["status_code"] = code
		resp["response_error_num"] = code
		resp["response_error"] = truncateResponseError("SMTP EHLO: " + ehloResp)
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	if strings.EqualFold(strings.TrimSpace(job.SMTPSecurity), "starttls") {
		if !strings.Contains(strings.ToUpper(ehloResp), "STARTTLS") {
			resp["status"] = 0
			resp["status_code"] = 500
			resp["response_error_num"] = 500
			resp["response_error"] = "SMTP STARTTLS not offered"
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		if err := smtpSendLine(conn, "STARTTLS"); err != nil {
			num, msg := classifyProbeError(err)
			resp["status"] = 0
			resp["status_code"] = 504
			resp["response_error_num"] = num
			resp["response_error"] = msg
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		code, line, err := smtpReadResponse(br)
		if err != nil {
			resp["status"] = 0
			resp["status_code"] = 500
			resp["response_error_num"] = 500
			resp["response_error"] = truncateResponseError(err.Error())
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		if code != 220 {
			resp["status"] = 0
			resp["status_code"] = code
			resp["response_error_num"] = code
			resp["response_error"] = truncateResponseError("SMTP STARTTLS: " + line)
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		tlsConn := tls.Client(conn, smtpTLSConfig(job))
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			num, msg := classifyProbeError(err)
			resp["status"] = 0
			resp["status_code"] = 504
			resp["response_error_num"] = num
			resp["response_error"] = msg
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		conn = tlsConn
		br = bufio.NewReader(conn)
		if err := smtpSendLine(conn, "EHLO monitoring-worker"); err != nil {
			num, msg := classifyProbeError(err)
			resp["status"] = 0
			resp["status_code"] = 504
			resp["response_error_num"] = num
			resp["response_error"] = msg
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		code, line, err = smtpReadResponse(br)
		if err != nil || code != 250 {
			resp["status"] = 0
			resp["status_code"] = smtpStatusOrDefault(code, 500)
			resp["response_error_num"] = smtpStatusOrDefault(code, 500)
			if err != nil {
				resp["response_error"] = truncateResponseError(err.Error())
			} else {
				resp["response_error"] = truncateResponseError("SMTP EHLO after STARTTLS: " + line)
			}
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
	}

	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func smtpDial(ctx context.Context, addr string, job model.Job) (net.Conn, error) {
	var d net.Dialer
	if strings.EqualFold(strings.TrimSpace(job.SMTPSecurity), "tls") {
		return tls.DialWithDialer(&d, "tcp", addr, smtpTLSConfig(job))
	}
	return d.DialContext(ctx, "tcp", addr)
}

func smtpTLSConfig(job model.Job) *tls.Config {
	verify := truthy(job.SSLVerify, true) && truthy(job.SSLVerifyHost, true)
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: !verify,
		ServerName:         strings.TrimSpace(job.Host),
	}
	if strings.TrimSpace(job.HTTPTLSCA) != "" {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM([]byte(job.HTTPTLSCA)) {
			cfg.RootCAs = pool
			cfg.InsecureSkipVerify = !verify
		}
	}
	return cfg
}

func smtpSendLine(w io.Writer, line string) error {
	_, err := io.WriteString(w, line+"\r\n")
	return err
}

func smtpReadResponse(r *bufio.Reader) (int, string, error) {
	var (
		code int
		all  []string
	)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, strings.Join(all, "\n"), err
		}
		line = strings.TrimRight(line, "\r\n")
		all = append(all, line)
		if len(line) < 3 {
			return 0, strings.Join(all, "\n"), fmt.Errorf("short SMTP response: %q", line)
		}
		if code == 0 {
			fmt.Sscanf(line[:3], "%d", &code)
		}
		if len(line) >= 4 && line[3] == '-' {
			continue
		}
		return code, strings.Join(all, "\n"), nil
	}
}

func RunNTP(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	addr := fmt.Sprintf("%s:%d", job.Host, job.Port)

	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	req := make([]byte, 48)
	req[0] = 0x1B
	txTime := time.Now()
	txBytes := ntpTimeBytes(txTime)
	copy(req[40:48], txBytes[:])
	if _, err := conn.Write(req); err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	reply := make([]byte, 48)
	if _, err := io.ReadFull(conn, reply); err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	serverTime := ntpBytesTime(reply[40:48])
	offsetMs := serverTime.Sub(time.Now()).Milliseconds()
	resp["ntp_offset_ms"] = offsetMs
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	if job.NTPMaxOffsetMs > 0 && int(math.Abs(float64(offsetMs))) > job.NTPMaxOffsetMs {
		resp["status"] = 0
		resp["status_code"] = 409
		resp["response_error_num"] = 409
		resp["response_error"] = fmt.Sprintf("NTP offset too large: %dms", offsetMs)
	}
	return resp
}

func ntpTimeBytes(t time.Time) [8]byte {
	const ntpEpochOffset = 2208988800
	sec := uint64(t.Unix() + ntpEpochOffset)
	frac := uint64(uint64(t.Nanosecond()) * (1 << 32) / 1e9)
	var out [8]byte
	binary.BigEndian.PutUint32(out[0:4], uint32(sec))
	binary.BigEndian.PutUint32(out[4:8], uint32(frac))
	return out
}

func ntpBytesTime(b []byte) time.Time {
	const ntpEpochOffset = 2208988800
	if len(b) < 8 {
		return time.Unix(0, 0)
	}
	sec := int64(binary.BigEndian.Uint32(b[0:4])) - ntpEpochOffset
	frac := binary.BigEndian.Uint32(b[4:8])
	nsec := (int64(frac) * 1e9) >> 32
	return time.Unix(sec, nsec)
}

func RunWebsocket(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	respMap := map[string]any{"status": 1, "status_code": 0}

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
	tr := &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: false}
	defer tr.CloseIdleConnections()
	configureTransportTimeouts(tr, timeoutSec)
	client := &http.Client{
		Transport: tr,
		Timeout:   clientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	reqURL, err := websocketHTTPURL(job.URL)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = truncateResponseError(err.Error())
		respMap["response_error_num"] = errNumBadRequest
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = truncateResponseError(err.Error())
		respMap["response_error_num"] = errNumBadRequest
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	for _, h := range parseHeaders(job.RequestHeaders) {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) == 2 {
			req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", websocketKey())
	if err := applyHTTPAuth(ctx, client, req, job); err != nil {
		respMap["status"] = 0
		respMap["response_error"] = truncateResponseError(err.Error())
		respMap["response_error_num"] = errNumBadRequest
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}

	httpResp, err := client.Do(req)
	if err != nil {
		num, msg := classifyProbeError(err)
		respMap["status"] = 0
		respMap["status_code"] = 0
		respMap["response_error"] = msg
		respMap["response_error_num"] = num
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	defer httpResp.Body.Close()

	respMap["status_code"] = httpResp.StatusCode
	respMap["response_error_num"] = 0
	respMap["response_error"] = ""
	respMap["total_time_us"] = time.Since(start).Microseconds()

	upgradeOK := httpResp.StatusCode == http.StatusSwitchingProtocols &&
		strings.EqualFold(strings.TrimSpace(httpResp.Header.Get("Upgrade")), "websocket")
	if upgradeOK {
		return respMap
	}

	body, _ := io.ReadAll(io.LimitReader(httpResp.Body, 800))
	respMap["response_error_num"] = httpResp.StatusCode
	respMap["response_error"] = formatHTTPStatusError(httpResp.StatusCode, body)
	if httpResp.StatusCode < 400 {
		respMap["status"] = 0
		respMap["response_error"] = "Websocket upgrade failed"
	} else if httpResp.StatusCode >= 500 {
		respMap["status"] = 0
	}
	return respMap
}

func websocketHTTPURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	case "http", "https":
	default:
		return "", fmt.Errorf("unsupported websocket scheme: %s", u.Scheme)
	}
	return u.String(), nil
}

func websocketKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.StdEncoding.EncodeToString(b[:])
}

func smtpStatusOrDefault(code int, def int) int {
	if code > 0 {
		return code
	}
	return def
}
