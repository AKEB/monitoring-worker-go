package handlers

import (
	"context"
	"net"
	"testing"
	"time"

	"monitoring-worker-go/internal/model"
)

func TestRunUDP_replySuccess(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	addr := pc.LocalAddr().(*net.UDPAddr)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		n, remote, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		_, _ = pc.WriteTo(buf[:n], remote)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp := RunUDP(ctx, model.Job{Host: "127.0.0.1", Port: addr.Port, Timeout: 2})
	<-done

	if resp["status"] != 1 {
		t.Fatalf("status=%v want 1; err=%v", resp["status"], resp["response_error"])
	}
	if resp["status_code"] != 200 {
		t.Fatalf("status_code=%v want 200", resp["status_code"])
	}
}

func TestRunUDP_silentListenerTimeoutSuccess(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	addr := pc.LocalAddr().(*net.UDPAddr)

	// Drain and ignore packets so the client times out without a reply (WireGuard-like).
	go func() {
		buf := make([]byte, 64)
		_ = pc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, _, _ = pc.ReadFrom(buf)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	resp := RunUDP(ctx, model.Job{Host: "127.0.0.1", Port: addr.Port, Timeout: 1})

	if resp["status"] != 1 {
		t.Fatalf("status=%v want 1; err=%v", resp["status"], resp["response_error"])
	}
	if resp["status_code"] != 200 {
		t.Fatalf("status_code=%v want 200", resp["status_code"])
	}
}

func TestRunUDP_closedPortMayFail(t *testing.T) {
	// Bind then close so the port is unused; ICMP unreachable is OS-dependent.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().(*net.UDPAddr)
	_ = pc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	resp := RunUDP(ctx, model.Job{Host: "127.0.0.1", Port: addr.Port, Timeout: 1})

	status, _ := resp["status"].(int)
	if status != 0 && status != 1 {
		t.Fatalf("unexpected status=%v", resp["status"])
	}
	// On platforms that surface ICMP, expect failure; otherwise timeout=success is acceptable.
	if status == 0 && resp["status_code"] != 504 {
		t.Fatalf("fail path status_code=%v want 504", resp["status_code"])
	}
	if status == 1 && resp["status_code"] != 200 {
		t.Fatalf("success path status_code=%v want 200", resp["status_code"])
	}
}

func TestIsUDPTimeout(t *testing.T) {
	if !isUDPTimeout(context.DeadlineExceeded) {
		t.Fatal("context deadline should be timeout")
	}
	if isUDPTimeout(nil) {
		t.Fatal("nil is not timeout")
	}
}
