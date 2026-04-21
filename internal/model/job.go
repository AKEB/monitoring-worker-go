package model

import (
	"encoding/json"
	"strconv"
	"strings"
)

type Job struct {
	JobID          int    `json:"job_id"`
	ID             int    `json:"id"`
	Type           int    `json:"type"`
	URL            string `json:"url"`
	Method         string `json:"method"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Timeout        int    `json:"timeout"`
	RepeatSeconds  int    `json:"repeat_seconds"`
	UpdateTime     int64  `json:"update_time"`
	StartTime      int64  `json:"start_time,omitempty"`
	RequestHeaders string `json:"request_headers,omitempty"`
	ProxyHost      string `json:"proxy_host,omitempty"`
	ProxyType      string `json:"proxy_type,omitempty"`
	SSLVerify      any    `json:"ssl_verify,omitempty"`
	SSLVerifyHost  any    `json:"ssl_verify_host,omitempty"`
	MaxRedirects   int    `json:"max_redirects,omitempty"`
	JsonPath       string `json:"json_path,omitempty"`
	JsonExpected   string `json:"json_expected_value,omitempty"`
	JsonType       string `json:"json_expected_type,omitempty"`
	TLS            bool   `json:"tls,omitempty"`
	TLSCAFile      string `json:"tls_ca_file,omitempty"`
	TLSServerName  string `json:"tls_server_name,omitempty"`
	TLSCertificate string `json:"tls_certificate,omitempty"`
	TLSKey         string `json:"tls_key,omitempty"`
}

// UnmarshalJSON supports legacy API payloads where numeric fields may come as strings.
func (j *Job) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	j.JobID = rawInt(raw["job_id"])
	j.ID = rawInt(raw["id"])
	j.Type = rawInt(raw["type"])
	j.URL = rawString(raw["url"])
	j.Method = rawString(raw["method"])
	j.Host = rawString(raw["host"])
	j.Port = rawInt(raw["port"])
	j.Timeout = rawInt(raw["timeout"])
	j.RepeatSeconds = rawInt(raw["repeat_seconds"])
	j.UpdateTime = rawInt64(raw["update_time"])
	j.StartTime = rawInt64(raw["start_time"])
	j.RequestHeaders = rawString(raw["request_headers"])
	j.ProxyHost = rawString(raw["proxy_host"])
	j.ProxyType = rawString(raw["proxy_type"])
	j.SSLVerify = rawAny(raw["ssl_verify"])
	j.SSLVerifyHost = rawAny(raw["ssl_verify_host"])
	j.MaxRedirects = rawInt(raw["max_redirects"])
	j.JsonPath = rawString(raw["json_path"])
	j.JsonExpected = rawString(raw["json_expected_value"])
	j.JsonType = rawString(raw["json_expected_type"])
	j.TLS = rawBool(raw["tls"])
	j.TLSCAFile = rawString(raw["tls_ca_file"])
	j.TLSServerName = rawString(raw["tls_server_name"])
	j.TLSCertificate = rawString(raw["tls_certificate"])
	j.TLSKey = rawString(raw["tls_key"])

	return nil
}

func rawAny(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	var out any
	_ = json.Unmarshal(v, &out)
	return out
}

func rawString(v json.RawMessage) string {
	if len(v) == 0 || string(v) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s
	}
	var i int64
	if err := json.Unmarshal(v, &i); err == nil {
		return strconv.FormatInt(i, 10)
	}
	var f float64
	if err := json.Unmarshal(v, &f); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return strings.Trim(string(v), "\"")
}

func rawInt(v json.RawMessage) int {
	if len(v) == 0 || string(v) == "null" {
		return 0
	}
	var i int
	if err := json.Unmarshal(v, &i); err == nil {
		return i
	}
	var i64 int64
	if err := json.Unmarshal(v, &i64); err == nil {
		return int(i64)
	}
	s := rawString(v)
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

func rawInt64(v json.RawMessage) int64 {
	if len(v) == 0 || string(v) == "null" {
		return 0
	}
	var i int64
	if err := json.Unmarshal(v, &i); err == nil {
		return i
	}
	var i32 int
	if err := json.Unmarshal(v, &i32); err == nil {
		return int64(i32)
	}
	s := rawString(v)
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n
}

func rawBool(v json.RawMessage) bool {
	if len(v) == 0 || string(v) == "null" {
		return false
	}
	var b bool
	if err := json.Unmarshal(v, &b); err == nil {
		return b
	}
	s := strings.ToLower(strings.TrimSpace(rawString(v)))
	return s == "1" || s == "true" || s == "yes" || s == "on"
}
