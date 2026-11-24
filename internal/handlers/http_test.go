package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"monitoring-worker-go/internal/model"
)

func TestRunHTTP_Status502FillsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream timed out"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{URL: srv.URL, Timeout: 5, Type: JobTypeHTTPS})

	if resp["status"] != 0 {
		t.Fatalf("status=%v want 0", resp["status"])
	}
	if resp["status_code"] != http.StatusBadGateway {
		t.Fatalf("status_code=%v", resp["status_code"])
	}
	if resp["response_error_num"] != http.StatusBadGateway {
		t.Fatalf("response_error_num=%v", resp["response_error_num"])
	}
	errMsg, _ := resp["response_error"].(string)
	if errMsg != "HTTP 502 Bad Gateway: upstream timed out" {
		t.Fatalf("response_error=%q", errMsg)
	}
}

func TestRunHTTP_LogHeaderOverridesStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Log", "db down")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("ignored body"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{URL: srv.URL, Timeout: 5, Type: JobTypeHTTPS})

	errMsg, _ := resp["response_error"].(string)
	if errMsg != "Log: db down" {
		t.Fatalf("response_error=%q", errMsg)
	}
}

func TestRunHTTP_TransportTimeoutClassified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{URL: srv.URL, Timeout: 1, Type: JobTypeHTTPS})

	if resp["status"] != 0 {
		t.Fatalf("status=%v want 0", resp["status"])
	}
	if resp["status_code"] != 0 {
		t.Fatalf("status_code=%v want 0 (no HTTP response)", resp["status_code"])
	}
	if resp["response_error_num"] != errNumTimeout {
		t.Fatalf("response_error_num=%v want %d; err=%v", resp["response_error_num"], errNumTimeout, resp["response_error"])
	}
	errMsg, _ := resp["response_error"].(string)
	if errMsg != "timeout: no HTTP response within deadline" {
		t.Fatalf("response_error=%q", errMsg)
	}
}

func TestRunHTTP_ConnectionRefused(t *testing.T) {
	// Port 1 is almost never accepting connections.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{URL: "http://127.0.0.1:1/", Timeout: 2, Type: JobTypeHTTPS})

	if resp["status"] != 0 {
		t.Fatalf("status=%v want 0", resp["status"])
	}
	if resp["status_code"] != 0 {
		t.Fatalf("status_code=%v want 0", resp["status_code"])
	}
	num, _ := resp["response_error_num"].(int)
	if num != errNumRefused && num != errNumTimeout && num != errNumTransport {
		t.Fatalf("response_error_num=%v err=%v", resp["response_error_num"], resp["response_error"])
	}
}

func TestRunHTTPKeyword_BodyContainsKeyword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello monitoring world"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:     srv.URL,
		Timeout: 5,
		Type:    JobTypeHTTPKeyword,
		Keyword: "monitoring",
	})

	if resp["status"] != 1 {
		t.Fatalf("status=%v err=%v", resp["status"], resp["response_error"])
	}
	if resp["keyword_valid"] != 1 {
		t.Fatalf("keyword_valid=%v", resp["keyword_valid"])
	}
	if resp["response_error"] != "" {
		t.Fatalf("response_error=%q want empty", resp["response_error"])
	}
}

func TestRunHTTPKeyword_MissingKeyword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:     srv.URL,
		Timeout: 5,
		Type:    JobTypeHTTPKeyword,
		Keyword: "monitoring",
	})

	if resp["keyword_valid"] != 0 {
		t.Fatalf("keyword_valid=%v want 0", resp["keyword_valid"])
	}
	if resp["response_error"] != "Keyword not found" {
		t.Fatalf("response_error=%q", resp["response_error"])
	}
}

func TestRunHTTPKeyword_InvertRejectsFoundKeyword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello monitoring world"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:           srv.URL,
		Timeout:       5,
		Type:          JobTypeHTTPKeyword,
		Keyword:       "monitoring",
		KeywordInvert: true,
	})

	if resp["keyword_valid"] != 0 {
		t.Fatalf("keyword_valid=%v want 0", resp["keyword_valid"])
	}
	if resp["response_error"] != "Keyword found but invert is enabled" {
		t.Fatalf("response_error=%q", resp["response_error"])
	}
}
