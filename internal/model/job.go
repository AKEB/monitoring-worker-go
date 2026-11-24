package model

import (
	"encoding/json"
	"strconv"
	"strings"
)

type Job struct {
	JobID                 int    `json:"job_id"`
	ID                    int    `json:"id"`
	Type                  int    `json:"type"`
	URL                   string `json:"url"`
	Method                string `json:"method"`
	Host                  string `json:"host"`
	Port                  int    `json:"port"`
	Timeout               int    `json:"timeout"`
	RepeatSeconds         int    `json:"repeat_seconds"`
	UpdateTime            int64  `json:"update_time"`
	DockerUpdateTime      int64  `json:"docker_update_time,omitempty"`
	StartTime             int64  `json:"start_time,omitempty"`
	RequestHeaders        string `json:"request_headers,omitempty"`
	RequestBody           string `json:"request_body,omitempty"`
	HTTPBodyEncoding      string `json:"http_body_encoding,omitempty"`
	AuthMethod            string `json:"auth_method,omitempty"`
	BasicAuthUser         string `json:"basic_auth_user,omitempty"`
	BasicAuthPass         string `json:"basic_auth_pass,omitempty"`
	BearerToken           string `json:"bearer_token,omitempty"`
	OAuthClientID         string `json:"oauth_client_id,omitempty"`
	OAuthClientSecret     string `json:"oauth_client_secret,omitempty"`
	OAuthTokenURL         string `json:"oauth_token_url,omitempty"`
	OAuthScopes           string `json:"oauth_scopes,omitempty"`
	OAuthAudience         string `json:"oauth_audience,omitempty"`
	OAuthAuthMethod       string `json:"oauth_auth_method,omitempty"`
	HTTPTLSCA             string `json:"http_tls_ca,omitempty"`
	HTTPTLSCert           string `json:"http_tls_cert,omitempty"`
	HTTPTLSKey            string `json:"http_tls_key,omitempty"`
	CacheBust             bool   `json:"cache_bust,omitempty"`
	Keyword               string `json:"keyword,omitempty"`
	KeywordInvert         bool   `json:"keyword_invert,omitempty"`
	ProxyHost             string `json:"proxy_host,omitempty"`
	ProxyType             string `json:"proxy_type,omitempty"`
	SSLVerify             any    `json:"ssl_verify,omitempty"`
	SSLVerifyHost         any    `json:"ssl_verify_host,omitempty"`
	MaxRedirects          int    `json:"max_redirects,omitempty"`
	JsonPath              string `json:"json_path,omitempty"`
	JsonExpected          string `json:"json_expected_value,omitempty"`
	JsonType              string `json:"json_expected_type,omitempty"`
	TLS                   bool   `json:"tls,omitempty"`
	TLSCAFile             string `json:"tls_ca_file,omitempty"`
	TLSServerName         string `json:"tls_server_name,omitempty"`
	TLSCertificate        string `json:"tls_certificate,omitempty"`
	TLSKey                string `json:"tls_key,omitempty"`
	DomainExpireAlertDays string `json:"domain_expire_alert_days,omitempty"`
	DNSRecordType         string `json:"dns_record_type,omitempty"`
	DNSResolver           string `json:"dns_resolver,omitempty"`
	DNSTransport          string `json:"dns_transport,omitempty"`
	DNSDoHURL             string `json:"dns_doh_url,omitempty"`
	DNSMatchMode          string `json:"dns_match_mode,omitempty"`
	DNSExpected           string `json:"dns_expected,omitempty"`
	SMTPSecurity          string `json:"smtp_security,omitempty"`
	NTPMaxOffsetMs        int    `json:"ntp_max_offset_ms,omitempty"`
	DbName                string `json:"db_name,omitempty"`
	DbQuery               string `json:"db_query,omitempty"`
	DbExpected            string `json:"db_expected,omitempty"`
	SftpPrivateKey        string `json:"sftp_private_key,omitempty"`
	SftpPath              string `json:"sftp_path,omitempty"`
	SnmpCommunity         string `json:"snmp_community,omitempty"`
	SnmpOID               string `json:"snmp_oid,omitempty"`
	SnmpExpected          string `json:"snmp_expected,omitempty"`
	RadiusSecret          string `json:"radius_secret,omitempty"`
	KafkaBrokers          string `json:"kafka_brokers,omitempty"`
	KafkaTopic            string `json:"kafka_topic,omitempty"`
	KafkaMessage          string `json:"kafka_message,omitempty"`
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
	j.DockerUpdateTime = rawInt64(raw["docker_update_time"])
	j.StartTime = rawInt64(raw["start_time"])
	j.RequestHeaders = rawString(raw["request_headers"])
	j.RequestBody = rawString(raw["request_body"])
	j.HTTPBodyEncoding = rawString(raw["http_body_encoding"])
	j.AuthMethod = rawString(raw["auth_method"])
	j.BasicAuthUser = rawString(raw["basic_auth_user"])
	j.BasicAuthPass = rawString(raw["basic_auth_pass"])
	j.BearerToken = rawString(raw["bearer_token"])
	j.OAuthClientID = rawString(raw["oauth_client_id"])
	j.OAuthClientSecret = rawString(raw["oauth_client_secret"])
	j.OAuthTokenURL = rawString(raw["oauth_token_url"])
	j.OAuthScopes = rawString(raw["oauth_scopes"])
	j.OAuthAudience = rawString(raw["oauth_audience"])
	j.OAuthAuthMethod = rawString(raw["oauth_auth_method"])
	j.HTTPTLSCA = rawString(raw["http_tls_ca"])
	j.HTTPTLSCert = rawString(raw["http_tls_cert"])
	j.HTTPTLSKey = rawString(raw["http_tls_key"])
	j.CacheBust = rawBool(raw["cache_bust"])
	j.Keyword = rawString(raw["keyword"])
	j.KeywordInvert = rawBool(raw["keyword_invert"])
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
	j.DomainExpireAlertDays = rawString(raw["domain_expire_alert_days"])
	j.DNSRecordType = rawString(raw["dns_record_type"])
	j.DNSResolver = rawString(raw["dns_resolver"])
	j.DNSTransport = rawString(raw["dns_transport"])
	j.DNSDoHURL = rawString(raw["dns_doh_url"])
	j.DNSMatchMode = rawString(raw["dns_match_mode"])
	j.DNSExpected = rawString(raw["dns_expected"])
	j.SMTPSecurity = rawString(raw["smtp_security"])
	j.NTPMaxOffsetMs = rawInt(raw["ntp_max_offset_ms"])
	j.DbName = rawString(raw["db_name"])
	j.DbQuery = rawString(raw["db_query"])
	j.DbExpected = rawString(raw["db_expected"])
	j.SftpPrivateKey = rawString(raw["sftp_private_key"])
	j.SftpPath = rawString(raw["sftp_path"])
	j.SnmpCommunity = rawString(raw["snmp_community"])
	j.SnmpOID = rawString(raw["snmp_oid"])
	j.SnmpExpected = rawString(raw["snmp_expected"])
	j.RadiusSecret = rawString(raw["radius_secret"])
	j.KafkaBrokers = rawString(raw["kafka_brokers"])
	j.KafkaTopic = rawString(raw["kafka_topic"])
	j.KafkaMessage = rawString(raw["kafka_message"])

	return nil
}

// MergeDockerTLS keeps cached PEM when the server omits tls_* (docker_tls_sync).
// PEM is preserved across docker_update_time bumps — the server may omit tls_* even when
// dockers.update_time changed (e.g. after a successful state save) while knownRevision matches.
func MergeDockerTLS(prev, incoming Job) Job {
	if prev.ID != 0 && incoming.ID != 0 && prev.ID != incoming.ID {
		return incoming
	}
	if incoming.TLSCAFile == "" && prev.TLSCAFile != "" {
		incoming.TLSCAFile = prev.TLSCAFile
	}
	if incoming.TLSCertificate == "" && prev.TLSCertificate != "" {
		incoming.TLSCertificate = prev.TLSCertificate
	}
	if incoming.TLSKey == "" && prev.TLSKey != "" {
		incoming.TLSKey = prev.TLSKey
	}
	if incoming.TLSServerName == "" && prev.TLSServerName != "" {
		incoming.TLSServerName = prev.TLSServerName
	}
	if incoming.DockerUpdateTime <= 0 && prev.DockerUpdateTime > 0 {
		incoming.DockerUpdateTime = prev.DockerUpdateTime
	}
	return incoming
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
