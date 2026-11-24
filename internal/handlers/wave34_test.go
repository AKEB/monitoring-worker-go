package handlers

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"monitoring-worker-go/internal/model"
)

func TestRunSFTP_UsesPasswordAuthFields(t *testing.T) {
	prev := sftpProberFunc
	t.Cleanup(func() { sftpProberFunc = prev })
	sftpProberFunc = func(ctx context.Context, job model.Job) error {
		if job.BasicAuthUser != "alice" || job.BasicAuthPass != "secret" {
			t.Fatalf("unexpected auth fields: %+v", job)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunSFTP(ctx, model.Job{
		Host:          "sftp.local",
		Port:          22,
		BasicAuthUser: "alice",
		BasicAuthPass: "secret",
		Timeout:       1,
	})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunSNMP_ExpectedMatch(t *testing.T) {
	prev := snmpExchange
	t.Cleanup(func() { snmpExchange = prev })
	snmpExchange = func(ctx context.Context, network, addr string, req []byte) ([]byte, error) {
		resp, err := buildSNMPResponseFromRequest(req, "linux")
		if err != nil {
			t.Fatal(err)
		}
		return resp, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunSNMP(ctx, model.Job{
		Host:          "127.0.0.1",
		Port:          161,
		SnmpCommunity: "public",
		SnmpOID:       "1.3.6.1.2.1.1.1.0",
		SnmpExpected:  "linux",
		Timeout:       1,
	})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunRADIUS_AccessAccept(t *testing.T) {
	prev := radiusExchange
	t.Cleanup(func() { radiusExchange = prev })
	radiusExchange = func(ctx context.Context, network, addr string, req []byte) ([]byte, error) {
		if len(req) < 20 || req[0] != 1 {
			t.Fatalf("unexpected radius request: %v", req)
		}
		resp := make([]byte, 20)
		resp[0] = 2
		resp[1] = req[1]
		resp[2] = 0
		resp[3] = 20
		return resp, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunRADIUS(ctx, model.Job{
		Host:          "127.0.0.1",
		Port:          1812,
		BasicAuthUser: "alice",
		BasicAuthPass: "secret",
		RadiusSecret:  "radius-secret",
		Timeout:       1,
	})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunGRPCKeyword_Bufconn(t *testing.T) {
	prev := grpcContextDialer
	t.Cleanup(func() { grpcContextDialer = prev })

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	grpcContextDialer = func(ctx context.Context, target string) (net.Conn, error) {
		return lis.Dial()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunGRPCKeyword(ctx, model.Job{
		URL:         "passthrough:///bufconn",
		Keyword:     "SERVING",
		Timeout:     1,
		RequestBody: "",
	})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunMQTT_KeywordMatch(t *testing.T) {
	prev := mqttClientFactory
	t.Cleanup(func() { mqttClientFactory = prev })
	mqttClientFactory = func(job model.Job) (mqttClient, error) {
		return fakeMQTTClient{message: "hello monitoring"}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunMQTT(ctx, model.Job{
		Host:        "127.0.0.1",
		Port:        1883,
		RequestBody: "alerts/topic",
		Keyword:     "monitoring",
		Timeout:     1,
	})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunRabbitMQ_ConnectSuccess(t *testing.T) {
	prev := rabbitMQDialer
	t.Cleanup(func() { rabbitMQDialer = prev })
	rabbitMQDialer = func(ctx context.Context, job model.Job) (rabbitMQConn, error) {
		return fakeRabbitMQConn{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunRabbitMQ(ctx, model.Job{Host: "127.0.0.1", Port: 5672, Timeout: 1})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunKafkaProducer_WriteFailure(t *testing.T) {
	prev := kafkaProducerFactory
	t.Cleanup(func() { kafkaProducerFactory = prev })
	kafkaProducerFactory = func(job model.Job) (kafkaProducer, error) {
		return fakeKafkaProducer{writeErr: errors.New("broker down")}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunKafkaProducer(ctx, model.Job{
		KafkaBrokers: "k1:9092",
		KafkaTopic:   "alerts",
		KafkaMessage: "hello",
		Timeout:      1,
	})

	if resp["status"] != 0 || resp["status_code"] != 500 {
		t.Fatalf("resp=%v", resp)
	}
}

type fakeMQTTClient struct {
	message string
	err     error
}

func (f fakeMQTTClient) Connect(context.Context) error           { return nil }
func (f fakeMQTTClient) Subscribe(context.Context, string) error { return nil }
func (f fakeMQTTClient) WaitMessage(context.Context) (string, error) {
	return f.message, f.err
}
func (f fakeMQTTClient) Close() error { return nil }

type fakeRabbitMQConn struct{}

func (fakeRabbitMQConn) Close() error { return nil }

type fakeKafkaProducer struct{ writeErr error }

func (f fakeKafkaProducer) Write(context.Context, string, []byte) error { return f.writeErr }
func (f fakeKafkaProducer) Close() error                                { return nil }
