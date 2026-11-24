package model

import (
	"encoding/json"
	"testing"
)

func TestMergeDockerTLS_keepsPEMWhenRevisionChanges(t *testing.T) {
	prev := Job{
		ID:               7,
		DockerUpdateTime: 100,
		TLSCAFile:        "ca-pem",
		TLSCertificate:   "cert-pem",
		TLSKey:           "key-pem",
	}
	incoming := Job{
		ID:               7,
		DockerUpdateTime: 200,
	}
	out := MergeDockerTLS(prev, incoming)
	if out.TLSCAFile != "ca-pem" || out.TLSCertificate != "cert-pem" || out.TLSKey != "key-pem" {
		t.Fatalf("expected cached PEM after revision bump, got ca=%q cert=%q key=%q", out.TLSCAFile, out.TLSCertificate, out.TLSKey)
	}
	if out.DockerUpdateTime != 200 {
		t.Fatalf("expected incoming docker_update_time, got %d", out.DockerUpdateTime)
	}
}

func TestMergeDockerTLS_keepsPEMWhenRevisionOmitted(t *testing.T) {
	prev := Job{
		ID:               7,
		DockerUpdateTime: 100,
		TLSCAFile:        "ca-pem",
	}
	incoming := Job{
		ID:               7,
		DockerUpdateTime: 0,
	}
	out := MergeDockerTLS(prev, incoming)
	if out.TLSCAFile != "ca-pem" {
		t.Fatalf("expected cached CA, got %q", out.TLSCAFile)
	}
	if out.DockerUpdateTime != 100 {
		t.Fatalf("expected prev docker_update_time when incoming omitted, got %d", out.DockerUpdateTime)
	}
}

func TestMergeDockerTLS_differentDockerID(t *testing.T) {
	prev := Job{ID: 1, TLSCAFile: "ca"}
	incoming := Job{ID: 2}
	out := MergeDockerTLS(prev, incoming)
	if out.TLSCAFile != "" {
		t.Fatalf("expected no merge across docker ids, got %q", out.TLSCAFile)
	}
}

func TestMergeDockerTLS_incomingPEMNotOverwritten(t *testing.T) {
	prev := Job{ID: 7, DockerUpdateTime: 100, TLSCAFile: "old-ca"}
	incoming := Job{ID: 7, DockerUpdateTime: 200, TLSCAFile: "new-ca"}
	out := MergeDockerTLS(prev, incoming)
	if out.TLSCAFile != "new-ca" {
		t.Fatalf("expected server PEM, got %q", out.TLSCAFile)
	}
}

func TestJobUnmarshalJSON_NewMonitorFields(t *testing.T) {
	var job Job
	err := json.Unmarshal([]byte(`{
		"db_name":"app",
		"db_query":"select 1",
		"db_expected":"1",
		"sftp_private_key":"pem",
		"sftp_path":"/upload",
		"snmp_community":"public",
		"snmp_oid":"1.3.6.1.2.1.1.1.0",
		"snmp_expected":"linux",
		"radius_secret":"radius-secret",
		"kafka_brokers":"k1:9092,k2:9092",
		"kafka_topic":"alerts",
		"kafka_message":"hello"
	}`), &job)
	if err != nil {
		t.Fatal(err)
	}
	if job.DbName != "app" || job.DbQuery != "select 1" || job.DbExpected != "1" {
		t.Fatalf("db fields not unmarshaled: %+v", job)
	}
	if job.SftpPrivateKey != "pem" || job.SftpPath != "/upload" {
		t.Fatalf("sftp fields not unmarshaled: %+v", job)
	}
	if job.SnmpCommunity != "public" || job.SnmpOID == "" || job.SnmpExpected != "linux" {
		t.Fatalf("snmp fields not unmarshaled: %+v", job)
	}
	if job.RadiusSecret != "radius-secret" {
		t.Fatalf("radius field not unmarshaled: %+v", job)
	}
	if job.KafkaBrokers == "" || job.KafkaTopic != "alerts" || job.KafkaMessage != "hello" {
		t.Fatalf("kafka fields not unmarshaled: %+v", job)
	}
}
