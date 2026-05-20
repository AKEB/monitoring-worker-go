package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"monitoring-worker-go/internal/buildinfo"
)

type Config struct {
	WorkerID            int
	WorkerThreads       int
	JobsGetTimeout      int
	LoopTimeoutUS       int
	ResponseSendTimeout int
	LogsWriteTimeout    int
	Timezone            string
	ServerHost          string
	ServerTLS           bool
	ProxyHost           string
	ProxyType           string
	WorkerKeyHash       string
	WorkerVersion       string
	ProtocolVersion     string
	Debug               bool
	CurlDebug           bool
	DockerDebug         bool
	// ImmediateErrorRetry mirrors PHP: after failure/timeout set update_time so the job is eligible again on the next loop tick (can spam DEBUG logs).
	// Set IMMEDIATE_ERROR_RETRY=false to wait full repeat_seconds before the next run (calmer; not byte-for-byte PHP).
	ImmediateErrorRetry bool
}

var (
	once sync.Once
	cfg  *Config
)

func Get() *Config {
	once.Do(func() {
		_ = loadDotEnvIfExists()
		cfg = &Config{
			WorkerID:            0,
			WorkerThreads:       getInt("WORKER_THREADS", 10),
			JobsGetTimeout:      getInt("JOBS_GET_TIMEOUT", 30),
			LoopTimeoutUS:       getInt("LOOP_TIMEOUT", 200000),
			ResponseSendTimeout: getInt("RESPONSE_SEND_TIMEOUT", 5),
			LogsWriteTimeout:    getInt("LOGS_WRITE_TIMEOUT", 10),
			Timezone:            getString("TZ", "UTC"),
			ServerHost:          normalizeHost(getString("SERVER_HOST", "")),
			ProxyHost:           getString("PROXY_HOST", ""),
			ProxyType:           getString("PROXY_TYPE", ""),
			WorkerKeyHash:       getString("WORKER_KEY_HASH", ""),
			WorkerVersion:       resolveWorkerVersion(),
			ProtocolVersion:     getString("PROTOCOL_VERSION", "2.0"),
			Debug:               getBool("DEBUG", false),
			CurlDebug:           getBool("CURL_DEBUG", false),
			DockerDebug:         getBool("DOCKER_DEBUG", false),
			ImmediateErrorRetry: getBool("IMMEDIATE_ERROR_RETRY", true),
		}
		cfg.ServerTLS = strings.HasPrefix(strings.ToLower(cfg.ServerHost), "https://")
		_ = os.Setenv("TZ", cfg.Timezone)
		if loc, err := time.LoadLocation(cfg.Timezone); err == nil {
			time.Local = loc
		} else {
			time.Local = time.UTC
		}
		if cfg.Debug {
			cfg.Logf("worker_version sent to server: %q", cfg.WorkerVersion)
		}
	})
	return cfg
}

func (c *Config) logPrefix() string {
	if c.WorkerID > 0 {
		return fmt.Sprintf("[%s] worker=%d PID=%d ", time.Now().Format("2006-01-02 15:04:05"), c.WorkerID, os.Getpid())
	}
	return fmt.Sprintf("[%s] PID=%d ", time.Now().Format("2006-01-02 15:04:05"), os.Getpid())
}

func (c *Config) Logf(format string, args ...any) {
	if !c.Debug {
		return
	}
	fmt.Printf("%s%s\n", c.logPrefix(), fmt.Sprintf(format, args...))
}

// CurlLogf — диагностика HTTP-запросов при CURL_DEBUG=true.
func (c *Config) CurlLogf(format string, args ...any) {
	if !c.CurlDebug {
		return
	}
	fmt.Printf("%s[curl] %s\n", c.logPrefix(), fmt.Sprintf(format, args...))
}

// DockerLogf пишет диагностику Docker Engine API при DOCKER_DEBUG=true (аналог PHP DOCKER_DEBUG).
func (c *Config) DockerLogf(format string, args ...any) {
	if !c.DockerDebug {
		return
	}
	fmt.Printf("[%s] PID=%d [docker] %s\n", time.Now().Format("2006-01-02 15:04:05"), os.Getpid(), fmt.Sprintf(format, args...))
}

func loadDotEnvIfExists() error {
	paths := []string{
		filepath.Join(getwdSafe(), ".env"),
		filepath.Join(filepath.Dir(getExecutableSafe()), ".env"),
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			line := strings.TrimSpace(s.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}
			k := strings.TrimSpace(parts[0])
			v := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
			if _, exists := os.LookupEnv(k); !exists {
				_ = os.Setenv(k, v)
			}
		}
		return s.Err()
	}
	return nil
}

func getwdSafe() string {
	wd, _ := os.Getwd()
	return wd
}

func getExecutableSafe() string {
	exe, _ := os.Executable()
	return exe
}

func normalizeHost(v string) string {
	if v == "" {
		return v
	}
	if strings.HasSuffix(v, "/") {
		return v
	}
	return v + "/"
}

// resolveWorkerVersion: как в PHP-воркере — в API уходит worker_version из WORKER_VERSION;
// в Docker PHP-образ дополнительно задаёт ENV при сборке. Для Go: если env пустой — берём
// версию из -ldflags (buildinfo.Version), иначе строка "local" как в src/version.php репозитория.
func resolveWorkerVersion() string {
	if v := strings.TrimSpace(getString("WORKER_VERSION", "")); v != "" {
		return v
	}
	if v := strings.TrimSpace(buildinfo.Version); v != "" {
		return v
	}
	return "local"
}

func getString(k, def string) string {
	v, ok := os.LookupEnv(k)
	if !ok {
		return def
	}
	return v
}

func getInt(k string, def int) int {
	v := getString(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getBool(k string, def bool) bool {
	v := strings.ToLower(getString(k, ""))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
