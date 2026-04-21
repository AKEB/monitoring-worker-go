package handlers

import (
	"context"
	"fmt"
	"net"
	"time"

	"monitoring-worker-go/internal/model"
)

func RunTCP(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0, "response_unixtime": time.Now().Unix()}
	addr := fmt.Sprintf("%s:%d", job.Host, job.Port)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = 504
		resp["response_error"] = err.Error()
	} else {
		_ = conn.Close()
		resp["status_code"] = 200
	}
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}
