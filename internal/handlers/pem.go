package handlers

import (
	"encoding/base64"
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
