package handlers

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"monitoring-worker-go/internal/model"
)

func TestRunSMTP_PlainEHLO(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- serveSMTPPlain(conn)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp := RunSMTP(ctx, model.Job{Host: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port, Timeout: 3})

	if err := <-done; err != nil {
		t.Fatalf("server err=%v", err)
	}
	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunSMTP_STARTTLS(t *testing.T) {
	caPEM, srvTLS := mustSMTPServerTLS(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- serveSMTPStartTLS(conn, srvTLS)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp := RunSMTP(ctx, model.Job{
		Host:         "127.0.0.1",
		Port:         ln.Addr().(*net.TCPAddr).Port,
		Timeout:      3,
		SMTPSecurity: "starttls",
		HTTPTLSCA:    caPEM,
	})

	if err := <-done; err != nil {
		t.Fatalf("server err=%v", err)
	}
	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunSMTP_ImplicitTLS(t *testing.T) {
	caPEM, srvTLS := mustSMTPServerTLS(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, srvTLS)
	defer tlsLn.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := tlsLn.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- serveSMTPPlain(conn)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp := RunSMTP(ctx, model.Job{
		Host:         "127.0.0.1",
		Port:         ln.Addr().(*net.TCPAddr).Port,
		Timeout:      3,
		SMTPSecurity: "tls",
		HTTPTLSCA:    caPEM,
	})

	if err := <-done; err != nil {
		t.Fatalf("server err=%v", err)
	}
	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunNTP_MaxOffsetExceeded(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 48)
		n, remote, err := pc.ReadFrom(buf)
		if err != nil || n < 48 {
			return
		}
		resp := make([]byte, 48)
		resp[0] = 0x24
		copy(resp[24:32], buf[40:48])
		ts := ntpTimeBytes(time.Now().Add(2 * time.Second))
		copy(resp[32:40], ts[:])
		copy(resp[40:48], ts[:])
		_, _ = pc.WriteTo(resp, remote)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp := RunNTP(ctx, model.Job{
		Host:           "127.0.0.1",
		Port:           pc.LocalAddr().(*net.UDPAddr).Port,
		Timeout:        2,
		NTPMaxOffsetMs: 500,
	})
	<-done

	if resp["status"] != 0 {
		t.Fatalf("status=%v want 0", resp["status"])
	}
	if resp["status_code"] == 200 {
		t.Fatalf("resp=%v want drift failure", resp)
	}
}

func TestRunNTP_SkipsOffsetCheckWhenUnset(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 48)
		n, remote, err := pc.ReadFrom(buf)
		if err != nil || n < 48 {
			return
		}
		resp := make([]byte, 48)
		resp[0] = 0x24
		copy(resp[24:32], buf[40:48])
		ts := ntpTimeBytes(time.Now().Add(2 * time.Second))
		copy(resp[32:40], ts[:])
		copy(resp[40:48], ts[:])
		_, _ = pc.WriteTo(resp, remote)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp := RunNTP(ctx, model.Job{
		Host:    "127.0.0.1",
		Port:    pc.LocalAddr().(*net.UDPAddr).Port,
		Timeout: 2,
	})
	<-done

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunWebsocket_UpgradeSuccessWithHeadersAndAuth(t *testing.T) {
	var gotHeader string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Test")
		gotAuth = r.Header.Get("Authorization")
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			t.Fatalf("missing Upgrade header: %q", r.Header.Get("Upgrade"))
		}
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Upgrade", "websocket")
		w.WriteHeader(http.StatusSwitchingProtocols)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp := RunWebsocket(ctx, model.Job{
		URL:            strings.Replace(srv.URL, "http://", "ws://", 1),
		Timeout:        3,
		RequestHeaders: "X-Test: yes",
		AuthMethod:     "basic",
		BasicAuthUser:  "alice",
		BasicAuthPass:  "secret",
	})

	if resp["status"] != 1 || resp["status_code"] != http.StatusSwitchingProtocols {
		t.Fatalf("resp=%v", resp)
	}
	if gotHeader != "yes" {
		t.Fatalf("X-Test=%q", gotHeader)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("Authorization=%q", gotAuth)
	}
}

func serveSMTPPlain(conn net.Conn) error {
	if _, err := fmt.Fprintf(conn, "220 test-smtp ready\r\n"); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "EHLO ") {
		return fmt.Errorf("got %q", line)
	}
	_, err = fmt.Fprintf(conn, "250-test\r\n250 OK\r\n")
	return err
}

func serveSMTPStartTLS(conn net.Conn, cfg *tls.Config) error {
	if _, err := fmt.Fprintf(conn, "220 test-smtp ready\r\n"); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "EHLO ") {
		return fmt.Errorf("got %q", line)
	}
	if _, err := fmt.Fprintf(conn, "250-test\r\n250-STARTTLS\r\n250 OK\r\n"); err != nil {
		return err
	}
	line, err = r.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != "STARTTLS" {
		return fmt.Errorf("got %q", line)
	}
	if _, err := fmt.Fprintf(conn, "220 Ready to start TLS\r\n"); err != nil {
		return err
	}
	tlsConn := tls.Server(conn, cfg)
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	rTLS := bufio.NewReader(tlsConn)
	line, err = rTLS.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "EHLO ") {
		return fmt.Errorf("got %q", line)
	}
	_, err = fmt.Fprintf(tlsConn, "250-test\r\n250 OK\r\n")
	return err
}

func mustSMTPServerTLS(t *testing.T) (string, *tls.Config) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "smtp-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	srvKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	srvCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(srvKey)}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), &tls.Config{
		Certificates: []tls.Certificate{srvCert},
		MinVersion:   tls.VersionTLS12,
	}
}
