package handlers

import (
	"bytes"
	"context"
	"crypto/md5"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"monitoring-worker-go/internal/model"
)

type mqttClient interface {
	Connect(context.Context) error
	Subscribe(context.Context, string) error
	WaitMessage(context.Context) (string, error)
	Close() error
}

type rabbitMQConn interface{ Close() error }

type kafkaProducer interface {
	Write(context.Context, string, []byte) error
	Close() error
}

var (
	sftpProberFunc    = realSFTPProbe
	snmpExchange      = udpExchange
	radiusExchange    = udpExchange
	grpcContextDialer = func(ctx context.Context, target string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", target)
	}
	mqttClientFactory = func(job model.Job) (mqttClient, error) { return newPahoMQTTClient(job) }
	rabbitMQDialer    = func(ctx context.Context, job model.Job) (rabbitMQConn, error) {
		return dialRabbitMQ(ctx, job)
	}
	kafkaProducerFactory = func(job model.Job) (kafkaProducer, error) { return newKafkaWriter(job) }
)

func RunSFTP(ctx context.Context, job model.Job) map[string]any {
	return runSimpleProbe(ctx, func(context.Context) error { return sftpProberFunc(ctx, job) })
}

func RunSNMP(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	req, reqID, oid, err := buildSNMPGetPacket(job.SnmpCommunity, job.SnmpOID)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	body, err := snmpExchange(ctx, "udp", fmt.Sprintf("%s:%d", job.Host, job.Port), req)
	if err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	got, err := parseSNMPResponse(body, reqID, oid)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	resp["snmp_value"] = got
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	if strings.TrimSpace(job.SnmpExpected) != "" && got != job.SnmpExpected {
		resp["status"] = 0
		resp["status_code"] = 422
		resp["response_error_num"] = 422
		resp["response_error"] = fmt.Sprintf("SNMP value mismatch: expected %q, got %q", job.SnmpExpected, got)
	}
	return resp
}

func RunRADIUS(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	req, id, err := buildRadiusAccessRequest(job)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	body, err := radiusExchange(ctx, "udp", fmt.Sprintf("%s:%d", job.Host, job.Port), req)
	if err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	code, err := parseRadiusResponse(body, id)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if code != 2 {
		resp["status"] = 0
		resp["status_code"] = 401
		resp["response_error_num"] = 401
		resp["response_error"] = fmt.Sprintf("RADIUS rejected with code %d", code)
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func RunGRPCKeyword(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}

	target := strings.TrimSpace(job.URL)
	if target == "" {
		target = fmt.Sprintf("%s:%d", job.Host, job.Port)
	}
	opts := []grpc.DialOption{grpc.WithContextDialer(grpcContextDialer)}
	if job.TLS || strings.HasPrefix(target, "https://") {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !truthy(job.SSLVerify, true)}
		if strings.TrimSpace(job.HTTPTLSCA) != "" {
			pool := x509.NewCertPool()
			if pool.AppendCertsFromPEM([]byte(job.HTTPTLSCA)) {
				tlsCfg.RootCAs = pool
			}
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		num, msg := classifyProbeError(err)
		resp["status"] = 0
		resp["status_code"] = 504
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer conn.Close()

	out, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: strings.TrimSpace(job.RequestBody),
	})
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	statusText := out.Status.String()
	resp["grpc_status"] = statusText
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	if strings.TrimSpace(job.Keyword) != "" && !strings.Contains(statusText, job.Keyword) {
		resp["status"] = 0
		resp["status_code"] = 422
		resp["response_error_num"] = 422
		resp["response_error"] = fmt.Sprintf("gRPC keyword not found in %q", statusText)
	}
	return resp
}

func RunMQTT(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	client, err := mqttClientFactory(job)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer client.Close()

	topic := strings.TrimSpace(job.RequestBody)
	if topic == "" {
		topic = strings.TrimSpace(job.URL)
	}
	if err := client.Connect(ctx); err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	if err := client.Subscribe(ctx, topic); err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	msg, err := client.WaitMessage(ctx)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	resp["mqtt_message"] = msg
	if strings.TrimSpace(job.Keyword) != "" && !strings.Contains(msg, job.Keyword) {
		resp["status"] = 0
		resp["status_code"] = 422
		resp["response_error_num"] = 422
		resp["response_error"] = "Keyword not found"
	}
	return resp
}

func RunRabbitMQ(ctx context.Context, job model.Job) map[string]any {
	return runSimpleProbe(ctx, func(context.Context) error {
		conn, err := rabbitMQDialer(ctx, job)
		if err != nil {
			return err
		}
		defer conn.Close()
		return nil
	})
}

func RunKafkaProducer(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	producer, err := kafkaProducerFactory(job)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer producer.Close()
	if err := producer.Write(ctx, job.KafkaTopic, []byte(job.KafkaMessage)); err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func runSimpleProbe(ctx context.Context, fn func(context.Context) error) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}
	if err := fn(ctx); err != nil {
		num, msg := classifyProbeError(err)
		if num == 0 {
			num = 500
			msg = truncateResponseError(err.Error())
		}
		resp["status"] = 0
		if num >= 400 {
			resp["status_code"] = num
		} else {
			resp["status_code"] = 500
		}
		resp["response_error_num"] = num
		resp["response_error"] = msg
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func realSFTPProbe(ctx context.Context, job model.Job) error {
	cfg := &ssh.ClientConfig{
		User:            job.BasicAuthUser,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(capTimeout(job.Timeout)) * time.Second,
	}
	if strings.TrimSpace(job.BasicAuthPass) != "" {
		cfg.Auth = append(cfg.Auth, ssh.Password(job.BasicAuthPass))
	}
	keyPEM := strings.TrimSpace(job.SftpPrivateKey)
	if keyPEM == "" {
		keyPEM = strings.TrimSpace(job.HTTPTLSKey)
	}
	if keyPEM != "" {
		signer, err := ssh.ParsePrivateKey([]byte(keyPEM))
		if err != nil {
			return err
		}
		cfg.Auth = append(cfg.Auth, ssh.PublicKeys(signer))
	}
	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", job.Host, job.Port), cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	sess, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	if err := sess.RequestSubsystem("sftp"); err != nil {
		return err
	}
	return nil
}

func udpExchange(ctx context.Context, network, addr string, req []byte) ([]byte, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(req); err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), buf[:n]...), nil
}

func buildSNMPGetPacket(community, oid string) ([]byte, int, string, error) {
	if strings.TrimSpace(oid) == "" {
		return nil, 0, "", errors.New("snmp_oid is empty")
	}
	reqID := int(time.Now().UnixNano() & 0x7fffffff)
	oidTLV, err := berOID(oid)
	if err != nil {
		return nil, 0, "", err
	}
	varbind := berSequence(append(oidTLV, berNull()...))
	varbinds := berSequence(varbind)
	pduContent := bytes.Join([][]byte{
		berInteger(reqID),
		berInteger(0),
		berInteger(0),
		varbinds,
	}, nil)
	pdu := berTLV(0xA0, pduContent)
	msg := berSequence(bytes.Join([][]byte{
		berInteger(1), // v2c
		berOctetString([]byte(defaultString(strings.TrimSpace(community), "public"))),
		pdu,
	}, nil))
	return msg, reqID, oid, nil
}

func buildSNMPResponseFromRequest(req []byte, value string) ([]byte, error) {
	community, reqID, oid, _, err := parseSNMPMessage(req, 0xA0)
	if err != nil {
		return nil, err
	}
	oidTLV, err := berOID(oid)
	if err != nil {
		return nil, err
	}
	varbind := berSequence(append(oidTLV, berOctetString([]byte(value))...))
	varbinds := berSequence(varbind)
	pduContent := bytes.Join([][]byte{
		berInteger(reqID),
		berInteger(0),
		berInteger(0),
		varbinds,
	}, nil)
	pdu := berTLV(0xA2, pduContent)
	return berSequence(bytes.Join([][]byte{
		berInteger(1),
		berOctetString([]byte(community)),
		pdu,
	}, nil)), nil
}

func parseSNMPResponse(body []byte, reqID int, oid string) (string, error) {
	_, gotReqID, gotOID, value, err := parseSNMPMessage(body, 0xA2)
	if err != nil {
		return "", err
	}
	if gotReqID != reqID {
		return "", fmt.Errorf("snmp request id mismatch")
	}
	if gotOID != oid {
		return "", fmt.Errorf("snmp oid mismatch")
	}
	return value, nil
}

func parseSNMPMessage(body []byte, wantPDU byte) (community string, reqID int, oid string, value string, err error) {
	tag, content, rest, err := berReadTLV(body)
	if err != nil {
		return "", 0, "", "", err
	}
	if tag != 0x30 || len(rest) != 0 {
		return "", 0, "", "", errors.New("invalid snmp message")
	}
	_, content, err = berReadExpectInteger(content)
	if err != nil {
		return "", 0, "", "", err
	}
	community, content, err = berReadExpectOctetString(content)
	if err != nil {
		return "", 0, "", "", err
	}
	pduTag, pdu, rest, err := berReadTLV(content)
	if err != nil {
		return "", 0, "", "", err
	}
	if pduTag != wantPDU || len(rest) != 0 {
		return "", 0, "", "", errors.New("unexpected snmp pdu")
	}
	reqID, pdu, err = berReadExpectInteger(pdu)
	if err != nil {
		return "", 0, "", "", err
	}
	_, pdu, err = berReadExpectInteger(pdu)
	if err != nil {
		return "", 0, "", "", err
	}
	_, pdu, err = berReadExpectInteger(pdu)
	if err != nil {
		return "", 0, "", "", err
	}
	_, vbContent, rest, err := berReadTLV(pdu)
	if err != nil || len(rest) != 0 {
		return "", 0, "", "", errors.New("invalid snmp varbind list")
	}
	_, vb, rest, err := berReadTLV(vbContent)
	if err != nil || len(rest) != 0 {
		return "", 0, "", "", errors.New("invalid snmp varbind")
	}
	oid, vb, err = berReadExpectOID(vb)
	if err != nil {
		return "", 0, "", "", err
	}
	value, vb, err = berReadValue(vb)
	if err != nil {
		return "", 0, "", "", err
	}
	if len(vb) != 0 {
		return "", 0, "", "", errors.New("extra snmp varbind bytes")
	}
	return community, reqID, oid, value, nil
}

func buildRadiusAccessRequest(job model.Job) ([]byte, byte, error) {
	if strings.TrimSpace(job.RadiusSecret) == "" {
		return nil, 0, errors.New("radius_secret is empty")
	}
	var auth [16]byte
	if _, err := crand.Read(auth[:]); err != nil {
		return nil, 0, err
	}
	var id [1]byte
	if _, err := crand.Read(id[:]); err != nil {
		return nil, 0, err
	}
	attrs := bytes.Join([][]byte{
		radiusAttr(1, []byte(job.BasicAuthUser)),
		radiusAttr(2, radiusEncryptPassword(job.BasicAuthPass, job.RadiusSecret, auth[:])),
	}, nil)
	out := make([]byte, 20+len(attrs))
	out[0] = 1
	out[1] = id[0]
	binary.BigEndian.PutUint16(out[2:4], uint16(len(out)))
	copy(out[4:20], auth[:])
	copy(out[20:], attrs)
	return out, id[0], nil
}

func parseRadiusResponse(body []byte, wantID byte) (byte, error) {
	if len(body) < 20 {
		return 0, errors.New("short radius response")
	}
	if body[1] != wantID {
		return 0, errors.New("radius identifier mismatch")
	}
	return body[0], nil
}

func radiusAttr(t byte, value []byte) []byte {
	out := make([]byte, 2+len(value))
	out[0] = t
	out[1] = byte(len(out))
	copy(out[2:], value)
	return out
}

func radiusEncryptPassword(password, secret string, authenticator []byte) []byte {
	pw := []byte(password)
	if len(pw)%16 != 0 {
		pw = append(pw, bytes.Repeat([]byte{0}, 16-len(pw)%16)...)
	}
	out := make([]byte, len(pw))
	prev := authenticator
	for i := 0; i < len(pw); i += 16 {
		sum := md5.Sum(append([]byte(secret), prev...))
		for j := 0; j < 16; j++ {
			out[i+j] = pw[i+j] ^ sum[j]
		}
		prev = out[i : i+16]
	}
	return out
}

type pahoClient struct {
	client mqtt.Client
	msgCh  chan string
}

func newPahoMQTTClient(job model.Job) (mqttClient, error) {
	broker := strings.TrimSpace(job.URL)
	if broker == "" {
		broker = fmt.Sprintf("tcp://%s:%d", job.Host, job.Port)
	}
	msgCh := make(chan string, 1)
	opts := mqtt.NewClientOptions().AddBroker(broker).
		SetClientID("monitoring-worker-" + strconv.FormatInt(time.Now().UnixNano(), 36)).
		SetUsername(job.BasicAuthUser).
		SetPassword(job.BasicAuthPass).
		SetAutoReconnect(false).
		SetConnectRetry(false)
	client := mqtt.NewClient(opts)
	return &pahoClient{client: client, msgCh: msgCh}, nil
}

func (c *pahoClient) Connect(ctx context.Context) error {
	tok := c.client.Connect()
	if ok := tok.WaitTimeout(time.Until(deadlineOrSoon(ctx))); !ok {
		return context.DeadlineExceeded
	}
	return tok.Error()
}

func (c *pahoClient) Subscribe(ctx context.Context, topic string) error {
	tok := c.client.Subscribe(topic, 0, func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case c.msgCh <- string(msg.Payload()):
		default:
		}
	})
	if ok := tok.WaitTimeout(time.Until(deadlineOrSoon(ctx))); !ok {
		return context.DeadlineExceeded
	}
	return tok.Error()
}

func (c *pahoClient) WaitMessage(ctx context.Context) (string, error) {
	select {
	case msg := <-c.msgCh:
		return msg, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (c *pahoClient) Close() error {
	c.client.Disconnect(250)
	return nil
}

func dialRabbitMQ(ctx context.Context, job model.Job) (rabbitMQConn, error) {
	u := strings.TrimSpace(job.URL)
	if u == "" {
		user := url.QueryEscape(job.BasicAuthUser)
		pass := url.QueryEscape(job.BasicAuthPass)
		if user == "" {
			user = "guest"
		}
		if pass == "" {
			pass = "guest"
		}
		u = fmt.Sprintf("amqp://%s:%s@%s:%d/", user, pass, job.Host, job.Port)
	}
	return amqp.DialConfig(u, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
}

type kafkaWriterWrapper struct{ writer *kafka.Writer }

func newKafkaWriter(job model.Job) (kafkaProducer, error) {
	brokers := strings.Split(job.KafkaBrokers, ",")
	clean := make([]string, 0, len(brokers))
	for _, b := range brokers {
		if s := strings.TrimSpace(b); s != "" {
			clean = append(clean, s)
		}
	}
	if len(clean) == 0 {
		return nil, errors.New("kafka_brokers is empty")
	}
	return &kafkaWriterWrapper{writer: &kafka.Writer{
		Addr:  kafka.TCP(clean...),
		Topic: job.KafkaTopic,
	}}, nil
}

func (k *kafkaWriterWrapper) Write(ctx context.Context, topic string, msg []byte) error {
	return k.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Value: msg})
}

func (k *kafkaWriterWrapper) Close() error { return k.writer.Close() }

func berSequence(content []byte) []byte { return berTLV(0x30, content) }

func berInteger(v int) []byte {
	if v == 0 {
		return []byte{0x02, 0x01, 0x00}
	}
	var tmp [8]byte
	i := len(tmp)
	n := v
	for n > 0 {
		i--
		tmp[i] = byte(n)
		n >>= 8
	}
	content := append([]byte(nil), tmp[i:]...)
	if content[0]&0x80 != 0 {
		content = append([]byte{0x00}, content...)
	}
	return berTLV(0x02, content)
}

func berOctetString(b []byte) []byte { return berTLV(0x04, b) }
func berNull() []byte                { return []byte{0x05, 0x00} }

func berOID(oid string) ([]byte, error) {
	parts := strings.Split(strings.TrimSpace(oid), ".")
	if len(parts) < 2 {
		return nil, errors.New("invalid oid")
	}
	ints := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		ints = append(ints, n)
	}
	content := []byte{byte(ints[0]*40 + ints[1])}
	for _, n := range ints[2:] {
		content = append(content, berBase128(n)...)
	}
	return berTLV(0x06, content), nil
}

func berBase128(n int) []byte {
	if n == 0 {
		return []byte{0}
	}
	var out [10]byte
	i := len(out)
	for n > 0 {
		i--
		out[i] = byte(n & 0x7f)
		n >>= 7
	}
	for j := i; j < len(out)-1; j++ {
		out[j] |= 0x80
	}
	return append([]byte(nil), out[i:]...)
}

func berTLV(tag byte, content []byte) []byte {
	out := []byte{tag}
	out = append(out, berLength(len(content))...)
	out = append(out, content...)
	return out
}

func berLength(n int) []byte {
	if n < 0x80 {
		return []byte{byte(n)}
	}
	var tmp [4]byte
	i := len(tmp)
	for n > 0 {
		i--
		tmp[i] = byte(n)
		n >>= 8
	}
	out := []byte{0x80 | byte(len(tmp)-i)}
	return append(out, tmp[i:]...)
}

func berReadTLV(data []byte) (tag byte, content []byte, rest []byte, err error) {
	if len(data) < 2 {
		return 0, nil, nil, io.ErrUnexpectedEOF
	}
	tag = data[0]
	lb := data[1]
	off := 2
	var l int
	if lb&0x80 == 0 {
		l = int(lb)
	} else {
		n := int(lb & 0x7f)
		if len(data) < 2+n {
			return 0, nil, nil, io.ErrUnexpectedEOF
		}
		for i := 0; i < n; i++ {
			l = (l << 8) | int(data[off+i])
		}
		off += n
	}
	if len(data) < off+l {
		return 0, nil, nil, io.ErrUnexpectedEOF
	}
	return tag, data[off : off+l], data[off+l:], nil
}

func berReadExpectInteger(data []byte) (int, []byte, error) {
	tag, content, rest, err := berReadTLV(data)
	if err != nil {
		return 0, nil, err
	}
	if tag != 0x02 || len(content) == 0 {
		return 0, nil, errors.New("expected integer")
	}
	var v int
	for _, b := range content {
		v = (v << 8) | int(b)
	}
	return v, rest, nil
}

func berReadExpectOctetString(data []byte) (string, []byte, error) {
	tag, content, rest, err := berReadTLV(data)
	if err != nil {
		return "", nil, err
	}
	if tag != 0x04 {
		return "", nil, errors.New("expected octet string")
	}
	return string(content), rest, nil
}

func berReadExpectOID(data []byte) (string, []byte, error) {
	tag, content, rest, err := berReadTLV(data)
	if err != nil {
		return "", nil, err
	}
	if tag != 0x06 || len(content) == 0 {
		return "", nil, errors.New("expected oid")
	}
	out := []int{int(content[0] / 40), int(content[0] % 40)}
	for i := 1; i < len(content); {
		var n int
		for {
			if i >= len(content) {
				return "", nil, io.ErrUnexpectedEOF
			}
			n = (n << 7) | int(content[i]&0x7f)
			cont := content[i]&0x80 != 0
			i++
			if !cont {
				break
			}
		}
		out = append(out, n)
	}
	parts := make([]string, len(out))
	for i, n := range out {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, "."), rest, nil
}

func berReadValue(data []byte) (string, []byte, error) {
	tag, content, rest, err := berReadTLV(data)
	if err != nil {
		return "", nil, err
	}
	switch tag {
	case 0x04:
		return string(content), rest, nil
	case 0x02:
		var v int
		for _, b := range content {
			v = (v << 8) | int(b)
		}
		return strconv.Itoa(v), rest, nil
	default:
		return base64.StdEncoding.EncodeToString(content), rest, nil
	}
}

func deadlineOrSoon(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(5 * time.Second)
}
