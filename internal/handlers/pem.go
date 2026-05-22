package handlers

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"strings"
)

// decodePEMField accepts base64-encoded PEM (monitoring server) or raw PEM text.
func decodePEMField(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if strings.Contains(s, "-----BEGIN") {
		return []byte(s), nil
	}
	return base64.StdEncoding.DecodeString(s)
}

// loadMTLSKeyPair loads client certificate+key from PEM (no temp files; required for scratch images without /tmp).
func loadMTLSKeyPair(certField, keyField string) (tls.Certificate, error) {
	certPEM, err := decodePEMField(certField)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls_certificate: %w", err)
	}
	keyPEM, err := decodePEMField(keyField)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls_key: %w", err)
	}
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		return tls.Certificate{}, fmt.Errorf("tls_certificate or tls_key empty after decode")
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}
