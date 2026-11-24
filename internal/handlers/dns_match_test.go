package handlers

import (
	"testing"

	"github.com/miekg/dns"
)

func TestMatchDNSExactSet(t *testing.T) {
	ok, _ := matchDNSAnswers("exact_set", []string{"1.2.3.4", "5.6.7.8"}, []string{"5.6.7.8", "1.2.3.4"}, dns.TypeA)
	if !ok {
		t.Fatal("exact_set should match regardless of order")
	}
	ok, _ = matchDNSAnswers("exact_set", []string{"1.2.3.4"}, []string{"1.2.3.4", "5.6.7.8"}, dns.TypeA)
	if ok {
		t.Fatal("exact_set should fail on extra answers")
	}
}

func TestMatchDNSAnyAll(t *testing.T) {
	ok, _ := matchDNSAnswers("any", []string{"1.2.3.4", "9.9.9.9"}, []string{"5.6.7.8", "1.2.3.4"}, dns.TypeA)
	if !ok {
		t.Fatal("any should pass when one expected is present")
	}
	ok, _ = matchDNSAnswers("all", []string{"1.2.3.4", "5.6.7.8"}, []string{"5.6.7.8", "1.2.3.4", "8.8.8.8"}, dns.TypeA)
	if !ok {
		t.Fatal("all should pass when all expected present")
	}
	ok, _ = matchDNSAnswers("all", []string{"1.2.3.4", "9.9.9.9"}, []string{"1.2.3.4"}, dns.TypeA)
	if ok {
		t.Fatal("all should fail when one missing")
	}
}

func TestMatchDNSContainsRegex(t *testing.T) {
	ok, _ := matchDNSAnswers("contains", []string{"v=spf1"}, []string{"v=spf1 include:_spf.google.com ~all"}, dns.TypeTXT)
	if !ok {
		t.Fatal("contains should match substring")
	}
	ok, _ = matchDNSAnswers("regex", []string{`v=spf1.*~all`}, []string{"v=spf1 include:example.com ~all"}, dns.TypeTXT)
	if !ok {
		t.Fatal("regex should match")
	}
}

func TestMatchDNSMX(t *testing.T) {
	ok, _ := matchDNSAnswers("mx_host", []string{"mail.example.com"}, []string{"10 mail.example.com."}, dns.TypeMX)
	if !ok {
		t.Fatal("mx_host should ignore priority and trailing dot")
	}
	ok, _ = matchDNSAnswers("mx_priority_host", []string{"10 mail.example.com"}, []string{"10 mail.example.com"}, dns.TypeMX)
	if !ok {
		t.Fatal("mx_priority_host should match")
	}
}

func TestNormalizeDomainName(t *testing.T) {
	if got := normalizeDomainName("Example.COM."); got != "example.com" {
		t.Fatalf("got %q", got)
	}
	if got := normalizeDomainName("https://Example.COM/path"); got != "example.com" {
		t.Fatalf("got %q", got)
	}
}

func TestParseWHOISExpiryRU(t *testing.T) {
	text := "domain:        EXAMPLE.RU\npaid-till:     2027-05-01T21:00:00Z\n"
	tm, ok := parseWHOISExpiry(text)
	if !ok {
		t.Fatal("expected parse")
	}
	if tm.Year() != 2027 || tm.Month() != 5 {
		t.Fatalf("unexpected time %v", tm)
	}
}
