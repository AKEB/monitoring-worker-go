package handlers

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"monitoring-worker-go/internal/config"
	"monitoring-worker-go/internal/model"
)

var diskLineRe = regexp.MustCompile(`^node_filesystem_(size|avail)_bytes\{device="([^"]+)",device_error="[^"]*",fstype="([^"]+)",mountpoint="([^"]+)"\}$`)

func RunExporter(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	timeout := capTimeout(job.Timeout)
	respMap := map[string]any{"status": 1, "status_code": 0}
	url := "http://" + job.Host + ":" + strconv.Itoa(job.Port) + "/metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		respMap["status"] = 0
		respMap["response_error"] = err.Error()
		respMap["result"] = "0"
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	tr := &http.Transport{IdleConnTimeout: 90 * time.Second}
	defer tr.CloseIdleConnections()
	cfg := config.Get()
	proxyHost := job.ProxyHost
	proxyType := job.ProxyType
	if proxyHost == "" {
		proxyHost = cfg.ProxyHost
		proxyType = cfg.ProxyType
	}
	configureTransportProxy(tr, proxyHost, proxyType, cfg.CurlLogf)
	client := &http.Client{Transport: tr, Timeout: time.Duration(timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		respMap["status"] = 0
		respMap["status_code"] = 500
		respMap["response_error_num"] = 500
		respMap["response_error"] = err.Error()
		respMap["result"] = "0"
		respMap["total_time_us"] = time.Since(start).Microseconds()
		return respMap
	}
	defer resp.Body.Close()
	metrics := parseMetricsLines(resp.Body)
	respMap["status_code"] = resp.StatusCode
	respMap["response_error_num"] = 0
	respMap["response_error"] = ""
	respMap["result"] = computeExporterMetric(job.Type, metrics)
	respMap["total_time_us"] = time.Since(start).Microseconds()
	return respMap
}

func parseMetricsLines(r io.Reader) map[string]float64 {
	out := make(map[string]float64)
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		out[parts[0]] = v
	}
	return out
}

func computeExporterMetric(kind int, m map[string]float64) string {
	switch kind {
	case JobTypeExporterMemory:
		total := m["node_memory_MemTotal_bytes"]
		avail := m["node_memory_MemAvailable_bytes"]
		if total == 0 {
			return "0"
		}
		return formatFloatRound2(100 - ((avail * 100) / total))
	case JobTypeExporterCPU:
		var totalTime, idleTime float64
		for k, v := range m {
			if strings.HasPrefix(k, "node_cpu_seconds_total") {
				totalTime += v
			}
			if strings.Contains(k, `mode="idle"`) {
				idleTime += v
			}
		}
		if totalTime == 0 {
			return "0"
		}
		return formatFloatRound2((1 - (idleTime / totalTime)) * 100)
	case JobTypeExporterDisk:
		excludedFSType := map[string]bool{
			"autofs": true, "binfmt_misc": true, "cgroup": true, "configfs": true, "debugfs": true,
			"devpts": true, "devtmpfs": true, "fuse.gvfsd-fuse": true, "fusectl": true, "hugetlbfs": true,
			"mqueue": true, "overlay": true, "proc": true, "pstore": true, "rpc_pipefs": true,
			"securityfs": true, "sysfs": true, "tmpfs": true, "tracefs": true, "efivarfs": true, "squashfs": true,
		}
		prefixes := []string{"/dev", "/sys", "/proc", "/run", "/var/lib/docker/overlay2"}
		var totalSize, totalAvail float64
		for k, v := range m {
			mm := diskLineRe.FindStringSubmatch(k)
			if len(mm) != 5 {
				continue
			}
			metricKind, fstype, mountpoint := mm[1], mm[3], mm[4]
			if excludedFSType[fstype] {
				continue
			}
			excludedMount := false
			for _, pref := range prefixes {
				if strings.HasPrefix(mountpoint, pref) {
					excludedMount = true
					break
				}
			}
			if excludedMount {
				continue
			}
			if metricKind == "size" {
				totalSize += v
			} else {
				totalAvail += v
			}
		}
		if totalSize == 0 {
			return "0"
		}
		return formatFloatRound2(100 - ((totalAvail * 100) / totalSize))
	default:
		return "0"
	}
}

func formatFloatRound2(v float64) string {
	return strconv.FormatFloat(float64(int(v*100+0.5))/100, 'f', -1, 64)
}
