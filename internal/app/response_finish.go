package app

import (
	"encoding/json"
	"strconv"
	"strings"

	"monitoring-worker-go/internal/handlers"
)

// responseIndicatesFailure approximates PHP non-zero child exit: treat payload as failed
// when status/json_valid/status_code match what the PHP scripts send on failure.
func responseIndicatesFailure(resp map[string]any, jobType int) bool {
	if resp == nil {
		return true
	}
	if v, ok := asInt64(resp["status"]); ok && v == 0 {
		return true
	}
	if jobType == handlers.JobTypeHTTPJSON {
		if v, ok := asInt64(resp["json_valid"]); ok && v == 0 {
			return true
		}
	}
	if v, ok := asInt64(resp["status_code"]); ok && v >= 400 {
		return true
	}
	return false
}

func asInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case json.Number:
		i, err := t.Int64()
		return i, err == nil
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}
