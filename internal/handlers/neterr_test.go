package handlers

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"syscall"
	"testing"
	"time"
)

func TestClassifyProbeError_Timeout(t *testing.T) {
	num, msg := classifyProbeError(context.DeadlineExceeded)
	if num != errNumTimeout {
		t.Fatalf("num=%d want %d", num, errNumTimeout)
	}
	if msg != "timeout: no HTTP response within deadline" {
		t.Fatalf("msg=%q", msg)
	}

	wrapped := &url.Error{Op: "Get", URL: "https://example.com/", Err: context.DeadlineExceeded}
	num, msg = classifyProbeError(wrapped)
	if num != errNumTimeout {
		t.Fatalf("wrapped num=%d want %d msg=%q", num, errNumTimeout, msg)
	}
}

func TestClassifyProbeError_DNS(t *testing.T) {
	err := &net.DNSError{Err: "no such host", Name: "missing.example", IsNotFound: true}
	num, msg := classifyProbeError(err)
	if num != errNumDNS {
		t.Fatalf("num=%d want %d", num, errNumDNS)
	}
	if len(msg) < 4 || msg[:4] != "dns:" {
		t.Fatalf("msg=%q want dns: prefix", msg)
	}
}

func TestClassifyProbeError_Refused(t *testing.T) {
	err := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	num, msg := classifyProbeError(err)
	if num != errNumRefused {
		t.Fatalf("num=%d want %d msg=%q", num, errNumRefused, msg)
	}
	if msg != "connect: connection refused" {
		t.Fatalf("msg=%q", msg)
	}
}

func TestClassifyProbeError_TLS(t *testing.T) {
	err := x509.UnknownAuthorityError{}
	num, msg := classifyProbeError(fmt.Errorf("Get \"https://x\": %w", err))
	if num != errNumTLS {
		t.Fatalf("num=%d want %d msg=%q", num, errNumTLS, msg)
	}
	if len(msg) < 4 || msg[:4] != "tls:" {
		t.Fatalf("msg=%q want tls: prefix", msg)
	}
}

func TestClassifyProbeError_GenericStripsURLPrefix(t *testing.T) {
	err := errors.New(`Get "https://vpn-at.akeb.ru/": something odd`)
	num, msg := classifyProbeError(err)
	if num != errNumTransport {
		t.Fatalf("num=%d want %d", num, errNumTransport)
	}
	if msg != "something odd" {
		t.Fatalf("msg=%q", msg)
	}
}

func TestFormatHTTPStatusError(t *testing.T) {
	msg := formatHTTPStatusError(502, []byte("<html>upstream timed out</html>"))
	if msg != "HTTP 502 Bad Gateway: upstream timed out" {
		t.Fatalf("msg=%q", msg)
	}
	msg = formatHTTPStatusError(504, nil)
	if msg != "HTTP 504 Gateway Timeout" {
		t.Fatalf("msg=%q", msg)
	}
}

func TestTruncateResponseError(t *testing.T) {
	runes := make([]rune, responseErrorMaxLen+20)
	for i := range runes {
		runes[i] = 'a'
	}
	out := truncateResponseError(string(runes))
	if len([]rune(out)) != responseErrorMaxLen {
		t.Fatalf("len=%d want %d", len([]rune(out)), responseErrorMaxLen)
	}
	if out[len(out)-3:] != "..." {
		t.Fatalf("want ellipsis, got %q", out)
	}
}

func TestTimeoutResponse(t *testing.T) {
	resp := TimeoutResponse(0)
	if resp["response_error"] != "timeout: job deadline exceeded" {
		t.Fatalf("error=%v", resp["response_error"])
	}
	if resp["response_error_num"] != errNumTimeout {
		t.Fatalf("num=%v", resp["response_error_num"])
	}
	if _, ok := resp["total_time_us"]; ok {
		t.Fatal("total_time_us should be omitted when elapsed=0")
	}
	resp = TimeoutResponse(1500 * time.Millisecond)
	us, ok := resp["total_time_us"].(int64)
	if !ok || us != 1500000 {
		t.Fatalf("total_time_us=%v ok=%v", resp["total_time_us"], ok)
	}
}
