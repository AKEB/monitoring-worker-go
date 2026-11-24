package handlers

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"monitoring-worker-go/internal/model"
)

func TestRunHTTP_POSTBodyReceived(t *testing.T) {
	var gotBody string
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:              srv.URL,
		Method:           http.MethodPost,
		Timeout:          5,
		Type:             JobTypeHTTPS,
		RequestBody:      `{"hello":"world"}`,
		HTTPBodyEncoding: "json",
	})
	if resp["status"] != 1 {
		t.Fatalf("status=%v err=%v", resp["status"], resp["response_error"])
	}
	if gotBody != `{"hello":"world"}` {
		t.Fatalf("body=%q", gotBody)
	}
	if !strings.Contains(gotCT, "application/json") {
		t.Fatalf("Content-Type=%q", gotCT)
	}
}

func TestRunHTTP_FormEncodingContentType(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = RunHTTP(ctx, model.Job{
		URL:              srv.URL,
		Method:           http.MethodPost,
		Timeout:          5,
		Type:             JobTypeHTTPS,
		RequestBody:      "a=1&b=2",
		HTTPBodyEncoding: "form",
	})
	if !strings.Contains(gotCT, "application/x-www-form-urlencoded") {
		t.Fatalf("Content-Type=%q", gotCT)
	}
}

func TestRunHTTP_CustomContentTypePreserved(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = RunHTTP(ctx, model.Job{
		URL:              srv.URL,
		Method:           http.MethodPost,
		Timeout:          5,
		Type:             JobTypeHTTPS,
		RequestBody:      `{"x":1}`,
		HTTPBodyEncoding: "json",
		RequestHeaders:   "Content-Type: application/vnd.custom+json",
	})
	if gotCT != "application/vnd.custom+json" {
		t.Fatalf("Content-Type=%q", gotCT)
	}
}

func TestRunHTTP_BasicAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = RunHTTP(ctx, model.Job{
		URL:           srv.URL,
		Timeout:       5,
		Type:          JobTypeHTTPS,
		AuthMethod:    "basic",
		BasicAuthUser: "alice",
		BasicAuthPass: "s3cret",
	})
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("Authorization=%q", gotAuth)
	}
	u, p, ok := parseBasic(gotAuth)
	if !ok || u != "alice" || p != "s3cret" {
		t.Fatalf("parsed basic user=%q pass=%q ok=%v", u, p, ok)
	}
}

func TestRunHTTP_BearerAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = RunHTTP(ctx, model.Job{
		URL:         srv.URL,
		Timeout:     5,
		Type:        JobTypeHTTPS,
		AuthMethod:  "bearer",
		BearerToken: "tok-123",
	})
	if gotAuth != "Bearer tok-123" {
		t.Fatalf("Authorization=%q", gotAuth)
	}
}

func TestRunHTTP_AuthMethodOverridesHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = RunHTTP(ctx, model.Job{
		URL:            srv.URL,
		Timeout:        5,
		Type:           JobTypeHTTPS,
		RequestHeaders: "Authorization: Bearer from-header",
		AuthMethod:     "bearer",
		BearerToken:    "from-auth-method",
	})
	if gotAuth != "Bearer from-auth-method" {
		t.Fatalf("Authorization=%q want auth_method to win", gotAuth)
	}
}

func TestRunHTTP_OAuth2ClientCredentials(t *testing.T) {
	tokenHits := 0
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"oauth-access","token_type":"Bearer","expires_in":3600}`))
	}))
	defer tokenSrv.Close()

	var gotAuth string
	resSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer resSrv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:              resSrv.URL,
		Timeout:          5,
		Type:             JobTypeHTTPS,
		AuthMethod:       "oauth2_cc",
		OAuthClientID:    "cid",
		OAuthClientSecret: "csecret",
		OAuthTokenURL:    tokenSrv.URL,
		OAuthAuthMethod:  "client_secret_post",
	})
	if resp["status"] != 1 {
		t.Fatalf("status=%v err=%v", resp["status"], resp["response_error"])
	}
	if tokenHits < 1 {
		t.Fatal("token endpoint not called")
	}
	if gotAuth != "Bearer oauth-access" {
		t.Fatalf("Authorization=%q", gotAuth)
	}
}

func TestRunHTTP_CacheBustAddsQuery(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = RunHTTP(ctx, model.Job{
		URL:       srv.URL + "/path?x=1",
		Timeout:   5,
		Type:      JobTypeHTTPS,
		CacheBust: true,
	})
	if !strings.Contains(gotURL, "x=1") || !strings.Contains(gotURL, "_=") {
		t.Fatalf("url=%q", gotURL)
	}
}

func TestRunHTTP_JSONWithPOSTBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:              srv.URL,
		Method:           http.MethodPost,
		Timeout:          5,
		Type:             JobTypeHTTPJSON,
		RequestBody:      `{"q":1}`,
		HTTPBodyEncoding: "json",
		JsonPath:         "ok",
		JsonExpected:     "true",
		JsonType:         "bool",
	})
	if resp["json_valid"] != 1 {
		t.Fatalf("json_valid=%v err=%v", resp["json_valid"], resp["response_error"])
	}
}

func TestRunHTTP_MTLSClientCert(t *testing.T) {
	caCert, caKey := mustTestCA(t)
	serverCert, serverKey := mustIssue(t, caCert, caKey, false)
	clientCert, clientKey := mustIssue(t, caCert, caKey, true)

	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Raw})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw})

	tlsCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)}),
	)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := RunHTTP(ctx, model.Job{
		URL:           srv.URL,
		Timeout:       5,
		Type:          JobTypeHTTPS,
		AuthMethod:    "mtls",
		HTTPTLSCA:    string(caPEM),
		HTTPTLSCert:  string(clientCertPEM),
		HTTPTLSKey:   string(clientKeyPEM),
		SSLVerify:     false,
		SSLVerifyHost: false,
	})
	if resp["status"] != 1 {
		t.Fatalf("status=%v err=%v", resp["status"], resp["response_error"])
	}
}

func parseBasic(h string) (user, pass string, ok bool) {
	const p = "Basic "
	if !strings.HasPrefix(h, p) {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(h, p))
	if err != nil {
		return "", "", false
	}
	parts := strings.SplitN(string(raw), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func mustTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func mustIssue(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, client bool) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	usage := x509.KeyUsageDigitalSignature
	ext := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	if client {
		ext = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     usage,
		ExtKeyUsage:  ext,
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}
