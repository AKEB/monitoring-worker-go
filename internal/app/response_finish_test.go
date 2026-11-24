package app

import (
	"testing"

	"monitoring-worker-go/internal/handlers"
)

func TestResponseIndicatesFailure_HTTPAllows4xx(t *testing.T) {
	resp := map[string]any{"status": 1, "status_code": 401}
	if responseIndicatesFailure(resp, handlers.JobTypeHTTPS) {
		t.Fatal("HTTPS 401 with status=1 must not be local failure (server uses response_code)")
	}
	resp405 := map[string]any{"status": 1, "status_code": 405}
	if responseIndicatesFailure(resp405, handlers.JobTypeHTTPS) {
		t.Fatal("HTTPS 405 with status=1 must not be local failure")
	}
}

func TestResponseIndicatesFailure_HTTP5xxIsFailure(t *testing.T) {
	resp := map[string]any{"status": 0, "status_code": 502}
	if !responseIndicatesFailure(resp, handlers.JobTypeHTTPS) {
		t.Fatal("HTTPS status=0 must be local failure")
	}
}

func TestResponseIndicatesFailure_HTTPJSONInvalid(t *testing.T) {
	resp := map[string]any{"status": 1, "status_code": 200, "json_valid": 0}
	if !responseIndicatesFailure(resp, handlers.JobTypeHTTPJSON) {
		t.Fatal("HTTP JSON with json_valid=0 must be local failure")
	}
}

func TestResponseIndicatesFailure_NonHTTP4xx(t *testing.T) {
	resp := map[string]any{"status": 1, "status_code": 404}
	if !responseIndicatesFailure(resp, handlers.JobTypeDocker) {
		t.Fatal("non-HTTP 4xx should still count as local failure")
	}
}

func TestResponseIndicatesFailure_WebsocketAllows4xx(t *testing.T) {
	resp := map[string]any{"status": 1, "status_code": 401}
	if responseIndicatesFailure(resp, handlers.JobTypeWebsocket) {
		t.Fatal("websocket 4xx with status=1 must not be local failure")
	}
}
