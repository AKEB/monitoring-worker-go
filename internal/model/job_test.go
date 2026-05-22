package model

import "testing"

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
