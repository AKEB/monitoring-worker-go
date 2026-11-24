package handlers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/miekg/dns"

	"monitoring-worker-go/internal/model"
)

func RunDNS(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 0, "status_code": 0}
	name := strings.TrimSpace(job.Host)
	if name == "" {
		resp["status_code"] = 400
		resp["response_error_num"] = 400
		resp["response_error"] = "empty host"
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if !strings.HasSuffix(name, ".") {
		name += "."
	}

	qtype := dnsTypeFromString(job.DNSRecordType)
	mode := strings.ToLower(strings.TrimSpace(job.DNSMatchMode))
	if mode == "" {
		mode = "exact_set"
	}
	expected := splitExpected(job.DNSExpected)

	answers, rcode, err := dnsQuery(ctx, job, name, qtype)
	if err != nil {
		resp["status_code"] = 504
		resp["response_error_num"] = 504
		resp["response_error"] = err.Error()
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	if mode == "nxdomain" {
		if rcode == dns.RcodeNameError {
			resp["status"] = 1
			resp["status_code"] = 200
		} else {
			resp["status_code"] = 409
			resp["response_error_num"] = 409
			resp["response_error"] = fmt.Sprintf("expected NXDOMAIN, got rcode=%s answers=%v", dns.RcodeToString[rcode], answers)
		}
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	if rcode == dns.RcodeNameError {
		resp["status_code"] = 404
		resp["response_error_num"] = 404
		resp["response_error"] = "NXDOMAIN"
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if rcode == dns.RcodeServerFailure {
		resp["status_code"] = 502
		resp["response_error_num"] = 502
		resp["response_error"] = "SERVFAIL"
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if rcode != dns.RcodeSuccess {
		resp["status_code"] = 502
		resp["response_error_num"] = 502
		resp["response_error"] = fmt.Sprintf("DNS rcode=%s", dns.RcodeToString[rcode])
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	ok, detail := matchDNSAnswers(mode, expected, answers, qtype)
	if ok {
		resp["status"] = 1
		resp["status_code"] = 200
		resp["response_error"] = strings.Join(answers, "\n")
	} else {
		resp["status_code"] = 409
		resp["response_error_num"] = 409
		resp["response_error"] = detail
	}
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func dnsTypeFromString(raw string) uint16 {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "AAAA":
		return dns.TypeAAAA
	case "CNAME":
		return dns.TypeCNAME
	case "MX":
		return dns.TypeMX
	case "TXT":
		return dns.TypeTXT
	case "NS":
		return dns.TypeNS
	default:
		return dns.TypeA
	}
}

func splitExpected(raw string) []string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	parts := strings.Split(raw, "\n")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func dnsQuery(ctx context.Context, job model.Job, name string, qtype uint16) ([]string, int, error) {
	msg := new(dns.Msg)
	msg.SetQuestion(name, qtype)
	msg.RecursionDesired = true

	transport := strings.ToLower(strings.TrimSpace(job.DNSTransport))
	if transport == "" {
		transport = "auto"
	}

	switch transport {
	case "doh":
		return dnsQueryDoH(ctx, job, msg)
	case "dot":
		return dnsQueryDoT(ctx, job, msg)
	case "tcp":
		return dnsQueryClassic(ctx, job, msg, "tcp")
	case "udp":
		return dnsQueryClassic(ctx, job, msg, "udp")
	default: // auto
		answers, rcode, err := dnsQueryClassic(ctx, job, msg, "udp")
		if err == nil {
			return answers, rcode, nil
		}
		// Truncation / UDP issues → TCP
		return dnsQueryClassic(ctx, job, msg, "tcp")
	}
}

func resolverAddr(job model.Job, defaultPort int) (string, error) {
	resolver := strings.TrimSpace(job.DNSResolver)
	port := job.Port
	if port <= 0 {
		port = defaultPort
	}
	if resolver == "" {
		cfg, err := dns.ClientConfigFromFile("/etc/resolv.conf")
		if err != nil || len(cfg.Servers) == 0 {
			return "", fmt.Errorf("system resolver unavailable: %v", err)
		}
		return net.JoinHostPort(cfg.Servers[0], fmt.Sprintf("%d", port)), nil
	}
	if host, p, err := net.SplitHostPort(resolver); err == nil {
		if port == defaultPort {
			return net.JoinHostPort(host, p), nil
		}
		return net.JoinHostPort(host, fmt.Sprintf("%d", port)), nil
	}
	return net.JoinHostPort(resolver, fmt.Sprintf("%d", port)), nil
}

func dnsQueryClassic(ctx context.Context, job model.Job, msg *dns.Msg, network string) ([]string, int, error) {
	addr, err := resolverAddr(job, 53)
	if err != nil {
		return nil, 0, err
	}
	client := &dns.Client{Net: network, Timeout: 10 * time.Second}
	if deadline, ok := ctx.Deadline(); ok {
		client.Timeout = time.Until(deadline)
		if client.Timeout <= 0 {
			client.Timeout = time.Second
		}
	}
	in, _, err := client.ExchangeContext(ctx, msg, addr)
	if err != nil {
		return nil, 0, err
	}
	return extractAnswers(in, msg.Question[0].Qtype), in.Rcode, nil
}

func dnsQueryDoT(ctx context.Context, job model.Job, msg *dns.Msg) ([]string, int, error) {
	addr, err := resolverAddr(job, 853)
	if err != nil {
		return nil, 0, err
	}
	host, _, _ := net.SplitHostPort(addr)
	client := &dns.Client{
		Net: "tcp-tls",
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: host,
		},
		Timeout: 10 * time.Second,
	}
	if deadline, ok := ctx.Deadline(); ok {
		client.Timeout = time.Until(deadline)
		if client.Timeout <= 0 {
			client.Timeout = time.Second
		}
	}
	in, _, err := client.ExchangeContext(ctx, msg, addr)
	if err != nil {
		return nil, 0, err
	}
	return extractAnswers(in, msg.Question[0].Qtype), in.Rcode, nil
}

func dnsQueryDoH(ctx context.Context, job model.Job, msg *dns.Msg) ([]string, int, error) {
	url := strings.TrimSpace(job.DNSDoHURL)
	if url == "" {
		url = "https://cloudflare-dns.com/dns-query"
	}
	wire, err := msg.Pack()
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(wire))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/dns-message")
	req.Header.Set("Accept", "application/dns-message")
	client := &http.Client{Timeout: 15 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		// GET fallback with dns= base64url
		q := base64.RawURLEncoding.EncodeToString(wire)
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		req2, err2 := http.NewRequestWithContext(ctx, http.MethodGet, url+sep+"dns="+q, nil)
		if err2 != nil {
			return nil, 0, err
		}
		req2.Header.Set("Accept", "application/dns-message")
		res, err = client.Do(req2)
		if err != nil {
			return nil, 0, err
		}
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, 0, err
	}
	if res.StatusCode >= 400 {
		return nil, 0, fmt.Errorf("DoH HTTP %d", res.StatusCode)
	}
	in := new(dns.Msg)
	if err := in.Unpack(body); err != nil {
		return nil, 0, err
	}
	return extractAnswers(in, msg.Question[0].Qtype), in.Rcode, nil
}

func extractAnswers(msg *dns.Msg, qtype uint16) []string {
	if msg == nil {
		return nil
	}
	out := make([]string, 0, len(msg.Answer))
	for _, rr := range msg.Answer {
		if rr.Header().Rrtype != qtype {
			continue
		}
		switch v := rr.(type) {
		case *dns.A:
			out = append(out, v.A.String())
		case *dns.AAAA:
			out = append(out, v.AAAA.String())
		case *dns.CNAME:
			out = append(out, strings.TrimSuffix(strings.ToLower(v.Target), "."))
		case *dns.NS:
			out = append(out, strings.TrimSuffix(strings.ToLower(v.Ns), "."))
		case *dns.MX:
			out = append(out, fmt.Sprintf("%d %s", v.Preference, strings.TrimSuffix(strings.ToLower(v.Mx), ".")))
		case *dns.TXT:
			out = append(out, strings.Join(v.Txt, ""))
		default:
			out = append(out, strings.TrimSpace(rr.String()))
		}
	}
	return out
}

func matchDNSAnswers(mode string, expected, answers []string, qtype uint16) (bool, string) {
	normAns := normalizeAnswerList(answers, qtype, mode)
	normExp := normalizeExpectedList(expected, qtype, mode)

	switch mode {
	case "exact":
		if len(normExp) != 1 {
			return false, "exact mode requires exactly one expected value"
		}
		if len(normAns) == 1 && normAns[0] == normExp[0] {
			return true, ""
		}
		return false, fmt.Sprintf("expected %q, got %v", normExp[0], normAns)

	case "any":
		if len(normExp) == 0 {
			return false, "any mode requires expected values"
		}
		set := toSet(normAns)
		for _, e := range normExp {
			if set[e] {
				return true, ""
			}
		}
		return false, fmt.Sprintf("none of %v found in %v", normExp, normAns)

	case "all":
		if len(normExp) == 0 {
			return false, "all mode requires expected values"
		}
		set := toSet(normAns)
		missing := []string{}
		for _, e := range normExp {
			if !set[e] {
				missing = append(missing, e)
			}
		}
		if len(missing) == 0 {
			return true, ""
		}
		return false, fmt.Sprintf("missing %v in %v", missing, normAns)

	case "contains":
		if qtype != dns.TypeTXT {
			return false, "contains mode is for TXT"
		}
		if len(normExp) == 0 {
			return false, "contains mode requires expected values"
		}
		joined := strings.ToLower(strings.Join(normAns, "\n"))
		for _, e := range normExp {
			if !strings.Contains(joined, strings.ToLower(e)) {
				return false, fmt.Sprintf("%q not found in TXT answers %v", e, normAns)
			}
		}
		return true, ""

	case "regex":
		if qtype != dns.TypeTXT {
			return false, "regex mode is for TXT"
		}
		if len(normExp) == 0 {
			return false, "regex mode requires a pattern"
		}
		joined := strings.Join(normAns, "\n")
		re, err := regexp.Compile("(?i)" + normExp[0])
		if err != nil {
			return false, "invalid regex: " + err.Error()
		}
		if re.MatchString(joined) {
			return true, ""
		}
		return false, fmt.Sprintf("regex %q did not match %v", normExp[0], normAns)

	case "mx_host":
		hosts := mxHostsOnly(normAns)
		expHosts := mxHostsOnly(normExp)
		if setsEqual(hosts, expHosts) {
			return true, ""
		}
		return false, fmt.Sprintf("expected MX hosts %v, got %v", expHosts, hosts)

	case "mx_priority_host":
		if setsEqual(normAns, normExp) {
			return true, ""
		}
		return false, fmt.Sprintf("expected MX %v, got %v", normExp, normAns)

	default: // exact_set
		ok := setsEqual(normAns, normExp)
		if ok {
			return true, ""
		}
		return false, fmt.Sprintf("expected set %v, got %v", normExp, normAns)
	}
}

func normalizeAnswerList(in []string, qtype uint16, mode string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, normalizeDNSValue(v, qtype, mode))
	}
	return out
}

func normalizeExpectedList(in []string, qtype uint16, mode string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, normalizeDNSValue(v, qtype, mode))
	}
	return out
}

func normalizeDNSValue(v string, qtype uint16, mode string) string {
	v = strings.TrimSpace(v)
	switch qtype {
	case dns.TypeA, dns.TypeAAAA:
		if ip := net.ParseIP(v); ip != nil {
			return ip.String()
		}
		return strings.ToLower(v)
	case dns.TypeCNAME, dns.TypeNS:
		return strings.TrimSuffix(strings.ToLower(v), ".")
	case dns.TypeMX:
		v = strings.TrimSpace(v)
		parts := strings.Fields(v)
		if len(parts) >= 2 {
			host := strings.TrimSuffix(strings.ToLower(parts[len(parts)-1]), ".")
			prio := parts[0]
			if mode == "mx_host" {
				return host
			}
			return prio + " " + host
		}
		return strings.TrimSuffix(strings.ToLower(v), ".")
	case dns.TypeTXT:
		if mode == "contains" || mode == "regex" {
			return v
		}
		return strings.ToLower(v)
	default:
		return strings.ToLower(v)
	}
}

func mxHostsOnly(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		parts := strings.Fields(v)
		if len(parts) >= 2 {
			out = append(out, strings.TrimSuffix(strings.ToLower(parts[len(parts)-1]), "."))
		} else {
			out = append(out, strings.TrimSuffix(strings.ToLower(v), "."))
		}
	}
	return out
}

func toSet(in []string) map[string]bool {
	m := make(map[string]bool, len(in))
	for _, v := range in {
		m[v] = true
	}
	return m
}

func setsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := append([]string{}, a...)
	sb := append([]string{}, b...)
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}
