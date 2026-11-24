package app

import (
	"testing"

	"monitoring-worker-go/internal/handlers"
)

func TestSelectRunner_HTTPFamilyIncludesKeyword(t *testing.T) {
	if got := selectRunner(handlers.JobTypeHTTPKeyword, false); got == nil {
		t.Fatal("http keyword should map to RunHTTP")
	}
	if got := selectRunner(handlers.JobTypeHTTPS, false); got == nil {
		t.Fatal("https should map to RunHTTP")
	}
	if got := selectRunner(handlers.JobTypeHTTPJSON, false); got == nil {
		t.Fatal("http json should map to RunHTTP")
	}
}

func TestSelectRunner_NewProtocolHandlers(t *testing.T) {
	tests := []struct {
		jobType int
		name    string
	}{
		{handlers.JobTypeSMTP, "smtp"},
		{handlers.JobTypeNTP, "ntp"},
		{handlers.JobTypeWebsocket, "websocket"},
		{handlers.JobTypeMySQL, "mysql"},
		{handlers.JobTypePostgres, "postgres"},
		{handlers.JobTypeRedis, "redis"},
		{handlers.JobTypeMongoDB, "mongodb"},
		{handlers.JobTypeSFTP, "sftp"},
		{handlers.JobTypeSNMP, "snmp"},
		{handlers.JobTypeRADIUS, "radius"},
		{handlers.JobTypeGRPCKeyword, "grpc"},
		{handlers.JobTypeMQTT, "mqtt"},
		{handlers.JobTypeRabbitMQ, "rabbitmq"},
		{handlers.JobTypeKafkaProducer, "kafka"},
	}
	for _, tc := range tests {
		if got := selectRunner(tc.jobType, false); got == nil {
			t.Fatalf("%s runner missing", tc.name)
		}
	}
}
