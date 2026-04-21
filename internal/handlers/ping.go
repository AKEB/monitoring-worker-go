package handlers

import (
	"context"
	"os/exec"
	"time"

	"monitoring-worker-go/internal/model"
)

func RunPing(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	timeout := capTimeout(job.Timeout)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	resp := map[string]any{"status": 1, "status_code": 0, "response_unixtime": time.Now().Unix()}
	cmd := exec.CommandContext(ctx, "ping", "-c", "1", job.Host)
	if err := cmd.Run(); err != nil {
		resp["status"] = 0
		resp["status_code"] = 502
		resp["response_error_num"] = 502
		resp["response_error"] = err.Error()
	} else {
		resp["status_code"] = 200
	}
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}
