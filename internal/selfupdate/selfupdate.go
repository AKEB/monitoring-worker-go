package selfupdate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const githubAllowPrefix = "https://github.com/AKEB/monitoring-worker-go/releases/download/"

// Command is the update payload from monitoring get/ response data.update.
type Command struct {
	Version string
	URL     string
	Sha256  string
	Arch    string
}

// Apply downloads, verifies, and replaces the running binary. On success the
// caller should exit so systemd (Restart=always) starts the new binary.
func Apply(cmd Command, requireGitHub bool) error {
	if strings.TrimSpace(cmd.URL) == "" {
		return fmt.Errorf("update url empty")
	}
	if strings.TrimSpace(cmd.Sha256) == "" {
		return fmt.Errorf("update sha256 empty")
	}
	wantArch := strings.TrimSpace(cmd.Arch)
	if wantArch == "" {
		wantArch = runtime.GOARCH
	}
	if wantArch != runtime.GOARCH {
		return fmt.Errorf("update arch %q != runtime %q", wantArch, runtime.GOARCH)
	}
	if err := validateURL(cmd.URL, requireGitHub, wantArch); err != nil {
		return err
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return fmt.Errorf("eval symlinks: %w", err)
	}
	dir := filepath.Dir(exe)
	tmp := filepath.Join(dir, ".monitoring-worker.update."+fmt.Sprint(os.Getpid()))
	defer func() { _ = os.Remove(tmp) }()

	if err := downloadFile(cmd.URL, tmp); err != nil {
		return err
	}
	sum, err := fileSHA256(tmp)
	if err != nil {
		return err
	}
	want := strings.ToLower(strings.TrimSpace(cmd.Sha256))
	if sum != want {
		return fmt.Errorf("sha256 mismatch: got %s want %s", sum, want)
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}
	backup := exe + ".bak"
	_ = os.Remove(backup)
	if err := os.Rename(exe, backup); err != nil {
		return fmt.Errorf("backup current binary: %w", err)
	}
	if err := os.Rename(tmp, exe); err != nil {
		_ = os.Rename(backup, exe)
		return fmt.Errorf("install new binary: %w", err)
	}
	_ = os.Remove(backup)
	return nil
}

func validateURL(raw string, requireGitHub bool, arch string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("bad update url: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("update url must be https")
	}
	if requireGitHub {
		if !strings.HasPrefix(raw, githubAllowPrefix) {
			return fmt.Errorf("update url not in GitHub allowlist")
		}
	}
	base := filepath.Base(u.Path)
	wantName := "monitoring-worker-linux-" + arch
	if base != wantName {
		return fmt.Errorf("update filename %q != %q", base, wantName)
	}
	return nil
}

func downloadFile(rawURL, dest string) error {
	client := &http.Client{Timeout: 5 * time.Minute}
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "monitoring-worker-selfupdate")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download HTTP %d", resp.StatusCode)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, io.LimitReader(resp.Body, 256<<20)); err != nil {
		return fmt.Errorf("write download: %w", err)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
