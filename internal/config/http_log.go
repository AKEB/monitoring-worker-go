package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

const httpErrorBodyMax = 800

// LogHTTPFailure writes to stderr when HTTP status is outside 200–299 or the request failed on the wire.
// Works the same for a local binary and Docker (docker logs / compose logs capture stderr).
func (c *Config) LogHTTPFailure(kind, method, requestURL string, httpStatus int, transportErr error, responseBody []byte) {
	if transportErr == nil && httpStatus >= 200 && httpStatus < 300 {
		return
	}
	line := c.formatHTTPFailureLine(kind, method, requestURL, httpStatus, transportErr, responseBody)
	fmt.Fprintln(os.Stderr, line)
}

// LogMonitoringAPIError writes to stderr when monitoring JSON uses status != 0 (API-level error, HTTP may be 200).
func (c *Config) LogMonitoringAPIError(kind, method, requestURL string, httpStatus int, responseBody []byte) {
	if len(responseBody) == 0 {
		return
	}
	var parsed struct {
		Status int    `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &parsed); err != nil || parsed.Status == 0 {
		return
	}
	errText := parsed.Error
	if errText == "" {
		errText = fmt.Sprintf("api status=%d", parsed.Status)
	}
	safeURL := redactURLCredentials(requestURL)
	if c.httpErrorBodyEnabled("server") && len(responseBody) > 0 {
		fmt.Fprintf(os.Stderr, "%s[api-error][%s] %s %s http_status=%d api_status=%d api_error=%q body=%q\n",
			c.logPrefix(), kind, method, safeURL, httpStatus, parsed.Status, errText,
			truncateHTTPLogBody(string(responseBody)),
		)
		return
	}
	fmt.Fprintf(os.Stderr, "%s[api-error][%s] %s %s http_status=%d api_status=%d api_error=%q\n",
		c.logPrefix(), kind, method, safeURL, httpStatus, parsed.Status, errText,
	)
}

func (c *Config) formatHTTPFailureLine(kind, method, requestURL string, httpStatus int, transportErr error, responseBody []byte) string {
	safeURL := redactURLCredentials(requestURL)
	var b strings.Builder
	b.WriteString(c.logPrefix())
	b.WriteString("[http-error][")
	b.WriteString(kind)
	b.WriteString("] ")
	b.WriteString(strings.ToUpper(method))
	b.WriteString(" ")
	b.WriteString(safeURL)
	if transportErr != nil {
		fmt.Fprintf(&b, " transport_err=%q", transportErr.Error())
	}
	if httpStatus != 0 {
		fmt.Fprintf(&b, " http_status=%d", httpStatus)
	} else {
		b.WriteString(" http_status=0")
	}
	if c.httpErrorBodyEnabled(kind) && len(responseBody) > 0 {
		fmt.Fprintf(&b, " body=%q", truncateHTTPLogBody(string(responseBody)))
	}
	return b.String()
}

func (c *Config) httpErrorBodyEnabled(kind string) bool {
	switch kind {
	case "docker":
		return c.DockerDebug
	case "monitor", "exporter":
		return c.CurlDebug
	case "server":
		return c.Debug
	default:
		return c.Debug
	}
}

func redactURLCredentials(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.UserPassword("***", "***")
	return u.String()
}

func truncateHTTPLogBody(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	if len(s) <= httpErrorBodyMax {
		return s
	}
	return s[:httpErrorBodyMax] + fmt.Sprintf(" …(truncated, total %d)", len(s))
}
