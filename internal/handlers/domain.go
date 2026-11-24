package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"monitoring-worker-go/internal/model"
)

var (
	whoisDateRes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)paid-till:\s*(.+)`),
		regexp.MustCompile(`(?i)free-date:\s*(.+)`),
		regexp.MustCompile(`(?i)Registry Expiry Date:\s*(.+)`),
		regexp.MustCompile(`(?i)Registrar Registration Expiration Date:\s*(.+)`),
		regexp.MustCompile(`(?i)Expiration Date:\s*(.+)`),
		regexp.MustCompile(`(?i)Expiry Date:\s*(.+)`),
		regexp.MustCompile(`(?i)expire:\s*(.+)`),
		regexp.MustCompile(`(?i)expires:\s*(.+)`),
		regexp.MustCompile(`(?i)Expiration Time:\s*(.+)`),
	}
	whoisDateLayouts = []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.0Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"02-Jan-2006",
		"2006.01.02",
		"02.01.2006",
	}
)

func RunDomain(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 0, "status_code": 0}
	domain := normalizeDomainName(job.Host)
	if domain == "" {
		resp["status_code"] = 400
		resp["response_error_num"] = 400
		resp["response_error"] = "empty domain"
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	expire, src, err := lookupDomainExpiry(ctx, domain)
	if err != nil || expire.IsZero() {
		resp["status_code"] = 502
		resp["response_error_num"] = 502
		if err != nil {
			resp["response_error"] = err.Error()
		} else {
			resp["response_error"] = "domain expiry not found"
		}
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	daysLeft := int(expire.Sub(time.Now()).Hours() / 24)
	resp["status"] = 1
	resp["status_code"] = 200
	resp["domain_expire"] = expire.Unix()
	resp["result"] = daysLeft
	resp["response_error"] = fmt.Sprintf("%d", daysLeft)
	if src != "" {
		resp["response_error_num"] = 0
	}
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func normalizeDomainName(raw string) string {
	d := strings.TrimSpace(strings.ToLower(raw))
	d = strings.TrimPrefix(d, "http://")
	d = strings.TrimPrefix(d, "https://")
	if i := strings.IndexAny(d, "/?#"); i >= 0 {
		d = d[:i]
	}
	d = strings.TrimSuffix(d, ".")
	return d
}

func lookupDomainExpiry(ctx context.Context, domain string) (time.Time, string, error) {
	if t, err := lookupRDAP(ctx, domain); err == nil && !t.IsZero() {
		return t, "rdap", nil
	} else if err != nil {
		// fall through to WHOIS
		_ = err
	}
	t, err := lookupWHOIS(ctx, domain)
	if err != nil {
		return time.Time{}, "", err
	}
	return t, "whois", nil
}

func lookupRDAP(ctx context.Context, domain string) (time.Time, error) {
	urls := rdapURLs(domain)
	var lastErr error
	client := &http.Client{Timeout: 20 * time.Second}
	for _, u := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Accept", "application/rdap+json, application/json")
		res, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		_ = res.Body.Close()
		if res.StatusCode >= 400 {
			lastErr = fmt.Errorf("rdap %s: HTTP %d", u, res.StatusCode)
			continue
		}
		t, err := parseRDAPExpiry(body)
		if err != nil {
			lastErr = err
			continue
		}
		if !t.IsZero() {
			return t, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("rdap: expiry not found")
	}
	return time.Time{}, lastErr
}

func rdapURLs(domain string) []string {
	lower := strings.ToLower(domain)
	urls := []string{}
	if strings.HasSuffix(lower, ".ru") || strings.HasSuffix(lower, ".su") || strings.HasSuffix(lower, ".рф") || strings.HasSuffix(lower, ".xn--p1ai") {
		urls = append(urls, "https://rdap.tcinet.ru/domain/"+domain)
	}
	urls = append(urls,
		"https://rdap.org/domain/"+domain,
		"https://rdap.verisign.com/com/v1/domain/"+domain,
	)
	return urls
}

func parseRDAPExpiry(body []byte) (time.Time, error) {
	var payload struct {
		Events []struct {
			Action string `json:"eventAction"`
			Date   string `json:"eventDate"`
		} `json:"events"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return time.Time{}, err
	}
	for _, ev := range payload.Events {
		action := strings.ToLower(strings.TrimSpace(ev.Action))
		if action == "expiration" || action == "expiry" || action == "expire" {
			if t, err := time.Parse(time.RFC3339, ev.Date); err == nil {
				return t, nil
			}
			if t, ok := parseFlexibleDate(ev.Date); ok {
				return t, nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("rdap: no expiration event")
}

func lookupWHOIS(ctx context.Context, domain string) (time.Time, error) {
	server := whoisServerFor(domain)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(server, "43"))
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := fmt.Fprintf(conn, "%s\r\n", domain); err != nil {
		return time.Time{}, err
	}
	var b strings.Builder
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		b.WriteString(line)
		b.WriteByte('\n')
		// Follow referral for some registries.
		if strings.Contains(strings.ToLower(line), "whois server:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				ref := strings.TrimSpace(parts[1])
				if ref != "" && !strings.EqualFold(ref, server) {
					if t, err := whoisQuery(ctx, ref, domain); err == nil && !t.IsZero() {
						return t, nil
					}
				}
			}
		}
	}
	text := b.String()
	if t, ok := parseWHOISExpiry(text); ok {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("whois: expiry not found via %s", server)
}

func whoisQuery(ctx context.Context, server, domain string) (time.Time, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(server, "43"))
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := fmt.Fprintf(conn, "%s\r\n", domain); err != nil {
		return time.Time{}, err
	}
	var b strings.Builder
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		b.WriteString(sc.Text())
		b.WriteByte('\n')
	}
	if t, ok := parseWHOISExpiry(b.String()); ok {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("whois: expiry not found via %s", server)
}

func whoisServerFor(domain string) string {
	lower := strings.ToLower(domain)
	switch {
	case strings.HasSuffix(lower, ".ru"), strings.HasSuffix(lower, ".su"), strings.HasSuffix(lower, ".рф"), strings.HasSuffix(lower, ".xn--p1ai"):
		return "whois.tcinet.ru"
	case strings.HasSuffix(lower, ".com"), strings.HasSuffix(lower, ".net"):
		return "whois.verisign-grs.com"
	case strings.HasSuffix(lower, ".org"):
		return "whois.pir.org"
	case strings.HasSuffix(lower, ".info"):
		return "whois.afilias.net"
	case strings.HasSuffix(lower, ".io"):
		return "whois.nic.io"
	default:
		// IANA referral
		return "whois.iana.org"
	}
}

func parseWHOISExpiry(text string) (time.Time, bool) {
	for _, re := range whoisDateRes {
		m := re.FindStringSubmatch(text)
		if len(m) < 2 {
			continue
		}
		if t, ok := parseFlexibleDate(strings.TrimSpace(m[1])); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseFlexibleDate(raw string) (time.Time, bool) {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, ".")
	if i := strings.IndexByte(s, '('); i > 0 {
		s = strings.TrimSpace(s[:i])
	}
	for _, layout := range whoisDateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	// Truncate fractional / timezone variants
	if len(s) >= 10 {
		if t, err := time.Parse("2006-01-02", s[:10]); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
