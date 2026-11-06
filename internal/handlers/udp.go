package handlers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"monitoring-worker-go/internal/model"
)

// RunUDP probes a UDP port: write an empty datagram, then wait for a reply or ICMP.
// ICMP port unreachable / connection refused → fail; read timeout or any reply → success.
func RunUDP(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	addr := fmt.Sprintf("%s:%d", job.Host, job.Port)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = 504
		resp["response_error"] = err.Error()
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	if _, err := conn.Write([]byte{0}); err != nil {
		if isUDPTimeout(err) {
			resp["status_code"] = 200
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = 504
		resp["response_error"] = err.Error()
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != nil {
		if isUDPTimeout(err) {
			// No ICMP / no reply within timeout → open or filtered.
			resp["status_code"] = 200
		} else {
			resp["status"] = 0
			resp["status_code"] = 504
			resp["response_error_num"] = 504
			resp["response_error"] = err.Error()
		}
	} else {
		resp["status_code"] = 200
	}
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func isUDPTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
