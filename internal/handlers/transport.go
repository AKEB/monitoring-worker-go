package handlers

import (
	"net"
	"net/http"
	"time"
)

// configureTransportTimeouts sets dial and response-header deadlines on tr.
// Call after configureTransportProxy so SOCKS DialContext is preserved when set.
func configureTransportTimeouts(tr *http.Transport, timeoutSec int) {
	if tr == nil {
		return
	}
	d := time.Duration(timeoutSec) * time.Second
	dialTimeout := d
	if dialTimeout > 20*time.Second {
		dialTimeout = 20 * time.Second
	}
	if tr.DialContext == nil {
		tr.DialContext = (&net.Dialer{Timeout: dialTimeout}).DialContext
	}
	tr.ResponseHeaderTimeout = d
	if tr.IdleConnTimeout == 0 {
		tr.IdleConnTimeout = 90 * time.Second
	}
}

// TimeoutResponse is sent to the server when a job context deadline is exceeded.
func TimeoutResponse() map[string]any {
	return map[string]any{
		"status":             0,
		"status_code":        0,
		"response_error_num": 408,
		"response_error":     "context deadline exceeded",
	}
}
