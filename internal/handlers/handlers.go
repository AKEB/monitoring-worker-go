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
	JobTypeUDP            = 18
	JobTypeDomain         = 26
	JobTypeDNS            = 27
	JobTypeHTTPKeyword    = 28
	JobTypeSMTP           = 29
	JobTypeNTP            = 30
	JobTypeWebsocket      = 31
	JobTypeMySQL          = 32
	JobTypePostgres       = 33
	JobTypeRedis          = 34
	JobTypeMongoDB        = 35
	JobTypeSFTP           = 36
	JobTypeSNMP           = 37
	JobTypeRADIUS         = 38
	JobTypeGRPCKeyword    = 39
	JobTypeMQTT           = 40
	JobTypeRabbitMQ       = 41
	JobTypeKafkaProducer  = 42
)
