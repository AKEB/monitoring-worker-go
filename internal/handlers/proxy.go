package handlers

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/proxy"
)

// PHP cURL proxy type constants (ext-curl).
const (
	proxyTypeHTTP             = 0
	proxyTypeHTTP10           = 1
	proxyTypeHTTPS            = 2
	proxyTypeSOCKS4           = 4
	proxyTypeSOCKS5           = 5
	proxyTypeSOCKS4A          = 6
	proxyTypeSOCKS5Hostname   = 7
)

func parseProxyTypeInt(proxyType string) int {
	s := strings.TrimSpace(proxyType)
	if s == "" {
		return proxyTypeHTTP
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return proxyTypeHTTP
	}
	return n
}

func configureTransportProxy(tr *http.Transport, proxyHost, proxyType string, logf func(string, ...any)) {
	host := strings.TrimSpace(proxyHost)
	if host == "" {
		return
	}
	u, err := parseProxyURL(host)
	if err != nil {
		if logf != nil {
			logf("proxy URL parse error: %v", err)
		}
		return
	}
	kind := parseProxyTypeInt(proxyType)
	switch kind {
	case proxyTypeSOCKS5, proxyTypeSOCKS5Hostname:
		applySOCKS5Proxy(tr, u, logf)
	case proxyTypeSOCKS4, proxyTypeSOCKS4A:
		if logf != nil {
			logf("proxy type %d: using SOCKS5 dialer (best effort for SOCKS4/4a)", kind)
		}
		applySOCKS5Proxy(tr, u, logf)
	default:
		tr.Proxy = http.ProxyURL(u)
		if logf != nil {
			logf("proxy HTTP type=%d via %s", kind, u.Host)
		}
	}
}

func applySOCKS5Proxy(tr *http.Transport, u *url.URL, logf func(string, ...any)) {
	var auth *proxy.Auth
	if u.User != nil {
		pass, _ := u.User.Password()
		auth = &proxy.Auth{
			User:     u.User.Username(),
			Password: pass,
		}
	}
	dialer, err := proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
	if err != nil {
		if logf != nil {
			logf("SOCKS5 proxy setup error: %v", err)
		}
		return
	}
	tr.Proxy = nil
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if cd, ok := dialer.(proxy.ContextDialer); ok {
			return cd.DialContext(ctx, network, addr)
		}
		return dialer.Dial(network, addr)
	}
	if logf != nil {
		logf("proxy SOCKS5 via %s", u.Host)
	}
}
