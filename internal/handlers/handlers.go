package handlers

import (
	"context"

	"monitoring-worker-go/internal/model"
)

type Runner func(ctx context.Context, job model.Job) map[string]any

const (
	JobTypeHTTPS          = 0
	JobTypeTCP            = 2
	JobTypePing           = 3
	JobTypeDocker         = 4
	JobTypeExporterDisk   = 5
	JobTypeExporterMemory = 6
	JobTypeExporterCPU    = 7
	JobTypeHTTPJSON       = 8
)
