package handlers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"syscall"
)

// Probe / transport error classification (response_error_num).
// status_code stays 0 when no HTTP response was received.
const (
	errNumBadRequest    = 1
	errNumTransport     = 2
	errNumTimeout       = 408
	errNumDNS           = 601
	errNumRefused       = 603
	errNumUnreachable   = 604
	errNumReset         = 605
	errNumTLS           = 606
	responseErrorMaxLen = 240
)

var urlErrorPrefixRe = regexp.MustCompile(`^(Get|Post|Head|Put|Delete|Patch|Options|Connect)\s+"[^"]*":\s*`)

// classifyProbeError maps a transport / dial error to response_error_num and a short message.
func classifyProbeError(err error) (num int, msg string) {
	if err == nil {
		return 0, ""
	}

	if isTimeoutErr(err) {
		return errNumTimeout, "timeout: no HTTP response within deadline"
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		detail := strings.TrimSpace(dnsErr.Error())
		if detail == "" {
			detail = "lookup failed"
		}
		return errNumDNS, truncateResponseError("dns: " + detail)
	}

	if isTLSErr(err) {
		return errNumTLS, truncateResponseError("tls: " + cleanProbeErrText(err))
	}

	switch {
	case errors.Is(err, syscall.ECONNREFUSED) || errnoIs(err, syscall.ECONNREFUSED):
		return errNumRefused, "connect: connection refused"
	case errors.Is(err, syscall.ECONNRESET) || errnoIs(err, syscall.ECONNRESET):
		return errNumReset, "connect: connection reset by peer"
	case errors.Is(err, syscall.ECONNABORTED) || errnoIs(err, syscall.ECONNABORTED):
		return errNumReset, "connect: connection aborted"
	case errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) ||
		errnoIs(err, syscall.ENETUNREACH) || errnoIs(err, syscall.EHOSTUNREACH):
		return errNumUnreachable, "connect: network unreachable"
	}

	return errNumTransport, truncateResponseError(cleanProbeErrText(err))
}

func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	var ue *url.Error
	if errors.As(err, &ue) && ue.Timeout() {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "client.timeout exceeded") ||
		strings.Contains(s, "context deadline exceeded")
}

func isTLSErr(err error) bool {
	if err == nil {
		return false
	}
	var (
		unknownAuth x509.UnknownAuthorityError
		hostnameErr x509.HostnameError
		sysRootsErr x509.SystemRootsError
		certInvalid x509.CertificateInvalidError
		recHdr      tls.RecordHeaderError
	)
	if errors.As(err, &unknownAuth) || errors.As(err, &hostnameErr) ||
		errors.As(err, &sysRootsErr) || errors.As(err, &certInvalid) ||
		errors.As(err, &recHdr) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "tls:") ||
		strings.Contains(s, "x509:") ||
		strings.Contains(s, "certificate") ||
		strings.Contains(s, "handshake failure")
}

func errnoIs(err error, want syscall.Errno) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Err != nil {
		if errors.Is(op.Err, want) {
			return true
		}
		var se syscall.Errno
		if errors.As(op.Err, &se) && se == want {
			return true
		}
	}
	var se syscall.Errno
	return errors.As(err, &se) && se == want
}

func cleanProbeErrText(err error) string {
	if err == nil {
		return ""
	}
	s := strings.TrimSpace(err.Error())
	s = urlErrorPrefixRe.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	if s == "" {
		s = "request failed"
	}
	return s
}

func truncateResponseError(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if len(runes) <= responseErrorMaxLen {
		return s
	}
	return string(runes[:responseErrorMaxLen-3]) + "..."
}

// formatHTTPStatusError builds response_error for a real HTTP status (4xx/5xx).
func formatHTTPStatusError(statusCode int, body []byte) string {
	text := http.StatusText(statusCode)
	if text == "" {
		text = "Error"
	}
	msg := fmt.Sprintf("HTTP %d %s", statusCode, text)
	if snippet := sanitizeBodySnippet(body); snippet != "" {
		msg = msg + ": " + snippet
	}
	return truncateResponseError(msg)
}

var htmlTagRe = regexp.MustCompile(`(?i)<[^>]+>`)

func sanitizeBodySnippet(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	s := string(body)
	s = htmlTagRe.ReplaceAllString(s, " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	runes := []rune(s)
	const maxSnippet = 120
	if len(runes) > maxSnippet {
		s = string(runes[:maxSnippet-3]) + "..."
	}
	return s
}
