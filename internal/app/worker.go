package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"monitoring-worker-go/internal/config"
	"monitoring-worker-go/internal/handlers"
	"monitoring-worker-go/internal/model"
)

type trackedJob struct {
	status   string
	state    string
	job      model.Job
	response map[string]any
	running  bool
	cancel   context.CancelFunc
	done     chan struct{}
	started  int64
}

type Worker struct {
	cfg *config.Config
	mu  sync.Mutex

	jobs    map[int]*trackedJob
	dockers map[int]*trackedJob

	jobSyncLast         int64
	sendStateTime       int64
	sendDockerStateTime int64
	sendStateJobs       []*trackedJob
	sendDockerStateJobs []*trackedJob
	restartFromServer   bool

	sem chan struct{}

	http *http.Client
}

func NewWorker(cfg *config.Config) *Worker {
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !cfg.ServerTLS},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		DialContext:           (&net.Dialer{Timeout: 20 * time.Second}).DialContext,
		ResponseHeaderTimeout: 10 * time.Second,
	}
	wt := max(1, cfg.WorkerThreads)
	return &Worker{
		cfg:     cfg,
		jobs:    make(map[int]*trackedJob),
		dockers: make(map[int]*trackedJob),
		sem:     make(chan struct{}, wt),
		http:    &http.Client{Transport: tr, Timeout: 30 * time.Second},
	}
}

func (w *Worker) Init() {
	w.cfg.Logf("Starting init function")
	w.getJobs()
	w.cfg.Logf("Finished init function")
}

func (w *Worker) Loop() {
	w.cfg.Logf("Starting loop function")
	w.sendStateTime = 0
	w.sendDockerStateTime = 0
	w.sendStateJobs = nil
	w.sendDockerStateJobs = nil
	var logTime int64
	loop := time.NewTicker(time.Duration(max(1, w.cfg.LoopTimeoutUS)) * time.Microsecond)
	defer loop.Stop()

	for {
		w.mu.Lock()
		restart := w.restartFromServer
		w.mu.Unlock()
		if restart {
			break
		}
		now := time.Now().Unix()
		if w.jobSyncLast < now-int64(w.cfg.JobsGetTimeout) {
			w.getJobs()
			w.jobSyncLast = now
		}

		w.tickAll(now)

		now = time.Now().Unix()
		if w.sendStateTime < now-int64(w.cfg.ResponseSendTimeout) && len(w.sendStateJobs) > 0 {
			w.sendJobsState()
		}
		if w.sendDockerStateTime < now-int64(w.cfg.ResponseSendTimeout) && len(w.sendDockerStateJobs) > 0 {
			w.sendDockersState()
		}

		if logTime < now-int64(w.cfg.LogsWriteTimeout) {
			logTime = now
			w.cfg.Logf("Loop: %d", w.cfg.LogsWriteTimeout)
			w.mu.Lock()
			for id, t := range w.jobs {
				w.cfg.Logf("Job ID: %d Status: %s State: %s StartTime: %s UpdateTime: %s",
					id, t.status, t.state, formatUnix(t.job.StartTime), formatUnix(t.job.UpdateTime))
			}
			for id, t := range w.dockers {
				w.cfg.Logf("Dockers Job ID: %d Status: %s State: %s StartTime: %s UpdateTime: %s",
					id, t.status, t.state, formatUnix(t.job.StartTime), formatUnix(t.job.UpdateTime))
			}
			w.mu.Unlock()
		}

		<-loop.C
	}
}

func formatUnix(v int64) string {
	if v <= 0 {
		return "0000-00-00 00:00:00"
	}
	return time.Unix(v, 0).In(time.Local).Format("2006-01-02 15:04:05")
}

func (w *Worker) countRunningLocked() int {
	n := 0
	for _, t := range w.jobs {
		if t.running {
			n++
		}
	}
	for _, t := range w.dockers {
		if t.running {
			n++
		}
	}
	return n
}

func (w *Worker) tickAll(now int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	running := w.countRunningLocked()
	w.tickMap(w.jobs, now, running, false)
	running = w.countRunningLocked()
	w.tickMap(w.dockers, now, running, true)
}

func (w *Worker) tickMap(m map[int]*trackedJob, now int64, running int, docker bool) {
	for id, t := range m {
		j := &t.job
		if j.RepeatSeconds <= 0 {
			j.RepeatSeconds = 60
		}
		j.RepeatSeconds = min(max(60, j.RepeatSeconds), 600)
		if j.Timeout <= 0 {
			j.Timeout = 15
		}
		j.Timeout = min(max(15, j.Timeout), 60)

		if t.running {
			if now > t.started+int64(j.Timeout) {
				if t.cancel != nil {
					t.cancel()
				}
				waitDone(t.done, 3*time.Second, func() {
					w.cfg.Logf("job_id %d: handler did not exit after cancel, continuing", id)
				})
				t.running = false
				t.cancel = nil
				if t.response == nil {
					t.response = handlers.TimeoutResponse()
				}
				w.bumpScheduleAfterFailureOrTimeout(j, now)
				t.state = "timeout"
				w.enqueueSend(t, docker)
				continue
			}
			select {
			case <-t.done:
				t.running = false
				if t.cancel != nil {
					t.cancel()
					t.cancel = nil
				}
				if responseIndicatesFailure(t.response, j.Type) {
					w.cfg.Logf("Finish error job_id: %d", id)
					w.bumpScheduleAfterFailureOrTimeout(j, now)
					t.state = "finished with error"
				} else {
					w.cfg.Logf("Finish job_id: %d", id)
					j.UpdateTime = t.started
					t.state = "finished"
				}
				if docker && !handlers.DockerResponseAcceptedByServer(t.response) {
					w.logDockerPollFailure(id, j, t.response)
				}
				w.enqueueSend(t, docker)
			default:
				t.state = "running"
			}
			continue
		}

		t.state = ""
		if t.status == "deleted" || j.JobID == 0 {
			delete(m, id)
			continue
		}

		if j.UpdateTime+int64(j.RepeatSeconds) > now {
			t.state = "waiting"
			continue
		}
		if w.cfg.WorkerThreads <= running {
			t.state = ""
			continue
		}

		var run handlers.Runner
		switch {
		case !docker && (j.Type == handlers.JobTypeHTTPS || j.Type == handlers.JobTypeHTTPJSON):
			run = handlers.RunHTTP
		case !docker && j.Type == handlers.JobTypeTCP:
			run = handlers.RunTCP
		case !docker && j.Type == handlers.JobTypePing:
			run = handlers.RunPing
		case !docker && (j.Type == handlers.JobTypeExporterDisk || j.Type == handlers.JobTypeExporterMemory || j.Type == handlers.JobTypeExporterCPU):
			run = handlers.RunExporter
		case docker:
			run = handlers.RunDocker
		default:
			j.UpdateTime = now + 3600
			continue
		}

		w.cfg.Logf("Start job_id: %d", id)
		t.done = make(chan struct{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(j.Timeout)*time.Second)
		t.cancel = cancel
		t.running = true
		t.state = "starting"
		t.started = now
		j.StartTime = now
		jobCopy := *j
		w.applyProxy(&jobCopy)
		running++

		go w.runJob(id, docker, t, ctx, jobCopy, run)
	}
}

func (w *Worker) runJob(id int, docker bool, t *trackedJob, ctx context.Context, jobCopy model.Job, run handlers.Runner) {
	_, _ = id, docker

	w.sem <- struct{}{}
	defer func() { <-w.sem }()
	defer close(t.done)

	resp := run(ctx, jobCopy)
	if ctx.Err() == context.DeadlineExceeded {
		if resp == nil {
			t.response = handlers.TimeoutResponse()
		} else {
			t.response = resp
		}
	} else {
		t.response = resp
	}
}

func waitDone(done <-chan struct{}, maxWait time.Duration, onTimeout func()) {
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(maxWait):
		if onTimeout != nil {
			onTimeout()
		}
	}
}

func (w *Worker) bumpScheduleAfterFailureOrTimeout(j *model.Job, now int64) {
	if w.cfg.ImmediateErrorRetry {
		j.UpdateTime = now - int64(j.RepeatSeconds)
		return
	}
	j.UpdateTime = now
}

func (w *Worker) enqueueSend(t *trackedJob, docker bool) {
	if t == nil || t.job.JobID == 0 {
		return
	}
	if docker {
		w.sendDockerStateJobs = upsertSendQueueByJobID(w.sendDockerStateJobs, t)
	} else {
		w.sendStateJobs = upsertSendQueueByJobID(w.sendStateJobs, t)
	}
}

// upsertSendQueueByJobID keeps at most one pending send per job_id (latest result wins).
// Without this, fast-failing jobs enqueue once per loop tick and one POST logs hundreds of duplicates.
func upsertSendQueueByJobID(q []*trackedJob, t *trackedJob) []*trackedJob {
	for i, x := range q {
		if x != nil && x.job.JobID == t.job.JobID {
			q[i] = t
			return q
		}
	}
	return append(q, t)
}

func (w *Worker) applyProxy(j *model.Job) {
	if j.ProxyHost == "" && w.cfg.ProxyHost != "" {
		j.ProxyHost = w.cfg.ProxyHost
		j.ProxyType = w.cfg.ProxyType
	}
}

func (w *Worker) dockerTLSSyncLocked() map[string]int64 {
	out := make(map[string]int64)
	for _, t := range w.dockers {
		if t == nil || t.job.ID <= 0 || t.job.DockerUpdateTime <= 0 {
			continue
		}
		out[strconv.Itoa(t.job.ID)] = t.job.DockerUpdateTime
	}
	return out
}

func (w *Worker) getJobs() {
	w.cfg.Logf("Starting getJobs function")
	w.mu.Lock()
	tlsSync := w.dockerTLSSyncLocked()
	w.mu.Unlock()
	payload := map[string]any{
		"worker_key_hash":  w.cfg.WorkerKeyHash,
		"protocol_version": w.cfg.ProtocolVersion,
		"worker_version":   w.cfg.WorkerVersion,
	}
	if len(tlsSync) > 0 {
		payload["docker_tls_sync"] = tlsSync
	}
	getURL := w.cfg.ServerHost + "api/monitoring/get/"
	body, code, err := w.postMonitoring(getURL, payload)
	if err != nil || code != 200 || len(body) == 0 {
		w.cfg.Logf("getJobs HTTP err=%v code=%d", err, code)
		time.Sleep(10 * time.Second)
		return
	}
	w.cfg.LogMonitoringAPIError("server", http.MethodPost, getURL, code, body)
	var parsed getJobsJSON
	if err := json.Unmarshal(body, &parsed); err != nil {
		w.cfg.Logf("getJobs JSON: %v", err)
		time.Sleep(10 * time.Second)
		return
	}
	if parsed.Error != "" {
		w.cfg.Logf("getJobs error field: %s", parsed.Error)
		time.Sleep(10 * time.Second)
		return
	}
	if parsed.Status != 0 {
		w.cfg.Logf("getJobs status=%d", parsed.Status)
		time.Sleep(10 * time.Second)
		return
	}

	jobs := parsed.Jobs
	if jobs == nil {
		jobs = []model.Job{}
	}
	dockers := parsed.Dockers
	if dockers == nil {
		dockers = []model.Job{}
	}

	w.mu.Lock()
	if len(parsed.Data) > 0 && string(parsed.Data) != "null" {
		var cfgObj map[string]any
		if err := json.Unmarshal(parsed.Data, &cfgObj); err == nil {
			w.syncConfigLocked(cfgObj)
		}
	}
	w.syncJobsLocked(jobs)
	w.syncDockersLocked(dockers)
	w.jobSyncLast = time.Now().Unix()
	w.mu.Unlock()

	w.cfg.Infof("getJobs: %d jobs, %d dockers", len(jobs), len(dockers))
	w.cfg.Logf("Finished getJobs function")
}

func (w *Worker) syncConfigLocked(cfgObj map[string]any) {
	w.restartFromServer = false
	if v, ok := cfgObj["worker_id"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			w.cfg.WorkerID = n
		}
	}
	if v, ok := cfgObj["worker_threads"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			w.cfg.WorkerThreads = n
			w.sem = make(chan struct{}, max(1, w.cfg.WorkerThreads))
		}
	}
	if v, ok := cfgObj["jobs_get_timeout"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			w.cfg.JobsGetTimeout = n
		}
	}
	if v, ok := cfgObj["loop_timeout"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			w.cfg.LoopTimeoutUS = n
		}
	}
	if v, ok := cfgObj["response_send_timeout"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			w.cfg.ResponseSendTimeout = n
		}
	}
	if v, ok := cfgObj["logs_write_timeout"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			w.cfg.LogsWriteTimeout = n
		}
	}
	if r, ok := cfgObj["restart"]; ok {
		if fmt.Sprint(r) == "true" {
			w.restartFromServer = true
		}
	}
}

func toInt(v any) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case json.Number:
		i, err := t.Int64()
		return int(i), err == nil
	case string:
		var n int
		_, err := fmt.Sscanf(strings.TrimSpace(t), "%d", &n)
		return n, err == nil && n > 0
	default:
		return 0, false
	}
}

func (w *Worker) syncJobsLocked(incoming []model.Job) {
	byID := make(map[int]model.Job, len(incoming))
	newIDs := make([]int, 0)
	for _, j := range incoming {
		byID[j.JobID] = j
		if _, ok := w.jobs[j.JobID]; !ok {
			newIDs = append(newIDs, j.JobID)
		}
	}
	for id, t := range w.jobs {
		if _, ok := byID[id]; !ok {
			t.status = "deleted"
			t.job = model.Job{}
			t.response = nil
			continue
		}
		t.status = "updated"
		nj := byID[id]
		if t.job.UpdateTime > nj.UpdateTime {
			nj.UpdateTime = t.job.UpdateTime
		}
		if t.job.StartTime > 0 {
			nj.StartTime = t.job.StartTime
		}
		t.job = nj
	}
	for _, id := range newIDs {
		w.jobs[id] = &trackedJob{status: "created", job: byID[id]}
	}
}

func (w *Worker) syncDockersLocked(incoming []model.Job) {
	byID := make(map[int]model.Job, len(incoming))
	newIDs := make([]int, 0)
	for _, j := range incoming {
		byID[j.JobID] = j
		if _, ok := w.dockers[j.JobID]; !ok {
			newIDs = append(newIDs, j.JobID)
		}
	}
	for id, t := range w.dockers {
		if _, ok := byID[id]; !ok {
			t.status = "deleted"
			t.job = model.Job{}
			t.response = nil
			continue
		}
		t.status = "updated"
		nj := model.MergeDockerTLS(t.job, byID[id])
		if t.job.UpdateTime > nj.UpdateTime {
			nj.UpdateTime = t.job.UpdateTime
		}
		if t.job.StartTime > 0 {
			nj.StartTime = t.job.StartTime
		}
		t.job = nj
	}
	for _, id := range newIDs {
		w.dockers[id] = &trackedJob{status: "created", job: byID[id]}
	}
}

func (w *Worker) sendJobsState() {
	w.mu.Lock()
	if len(w.sendStateJobs) == 0 {
		w.sendStateTime = time.Now().Unix()
		w.sendStateJobs = nil
		w.mu.Unlock()
		return
	}
	jobsCopy := w.sendStateJobs
	w.sendStateJobs = nil
	w.mu.Unlock()

	w.cfg.Logf("Starting sendJobsState function")
	params := map[string]any{
		"worker_key_hash":  w.cfg.WorkerKeyHash,
		"protocol_version": w.cfg.ProtocolVersion,
		"worker_version":   w.cfg.WorkerVersion,
	}
	jobsOut := make([]any, 0, len(jobsCopy))
	for _, t := range jobsCopy {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		w.cfg.Logf("Sending job %d monitor %d", t.job.JobID, t.job.ID)
		jobsOut = append(jobsOut, map[string]any{
			"job_id":      t.job.JobID,
			"monitor_id":  t.job.ID,
			"update_time": t.job.UpdateTime,
			"response":    t.response,
		})
	}
	if len(jobsOut) == 0 {
		w.mu.Lock()
		w.sendStateTime = time.Now().Unix()
		w.mu.Unlock()
		return
	}
	params["jobs"] = jobsOut
	stateURL := w.cfg.ServerHost + "api/monitoring/state/"
	body, code, err := w.postMonitoring(stateURL, params)
	apiOK, apiErr := w.monitoringAPIOK(body, code, err)
	if !apiOK && code >= 200 && code < 300 {
		w.cfg.LogMonitoringAPIError("server", http.MethodPost, stateURL, code, body)
	}
	w.mu.Lock()
	if !apiOK {
		for _, t := range jobsCopy {
			w.sendStateJobs = upsertSendQueueByJobID(w.sendStateJobs, t)
		}
		w.sendStateTime = time.Now().Unix()
		w.mu.Unlock()
		w.cfg.Infof("sendJobsState retry later: http=%d err=%v api_err=%q", code, err, apiErr)
		return
	}
	w.sendStateTime = time.Now().Unix()
	clearTrackedResponses(jobsCopy)
	w.mu.Unlock()
	w.cfg.Logf("Finished sendJobsState function")
}

func (w *Worker) sendDockersState() {
	w.mu.Lock()
	if len(w.sendDockerStateJobs) == 0 {
		w.sendDockerStateTime = time.Now().Unix()
		w.sendDockerStateJobs = nil
		w.mu.Unlock()
		return
	}
	dockCopy := w.sendDockerStateJobs
	w.sendDockerStateJobs = nil
	w.mu.Unlock()

	w.cfg.Logf("Starting sendDockersState function")
	params := map[string]any{
		"worker_key_hash":  w.cfg.WorkerKeyHash,
		"protocol_version": w.cfg.ProtocolVersion,
		"worker_version":   w.cfg.WorkerVersion,
	}
	out := make([]any, 0, len(dockCopy))
	for _, t := range dockCopy {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		n := handlers.DockerContainerStatesCount(t.response)
		accepted := handlers.DockerResponseAcceptedByServer(t.response)
		w.cfg.Infof("docker POST job_id=%d docker_id=%d host=%q accepted=%v containers=%d", t.job.JobID, t.job.ID, t.job.Host, accepted, n)
		if !accepted {
			w.logDockerPollFailure(t.job.JobID, &t.job, t.response)
		}
		w.cfg.Logf("Sending dockers job %d docker %d", t.job.JobID, t.job.ID)
		out = append(out, map[string]any{
			"job_id":      t.job.JobID,
			"docker_id":   t.job.ID,
			"update_time": t.job.UpdateTime,
			"response":    t.response,
		})
	}
	if len(out) == 0 {
		w.mu.Lock()
		w.sendDockerStateTime = time.Now().Unix()
		w.mu.Unlock()
		return
	}
	params["dockers"] = out
	if w.cfg.DockerDebug {
		b, _ := json.Marshal(params)
		w.cfg.Logf("Sending data: %s", string(b))
	}
	stateURL := w.cfg.ServerHost + "api/monitoring/state/"
	body, code, err := w.postMonitoring(stateURL, params)
	apiOK, apiErr := w.monitoringAPIOK(body, code, err)
	if !apiOK && code >= 200 && code < 300 {
		w.cfg.LogMonitoringAPIError("server", http.MethodPost, stateURL, code, body)
	}
	w.cfg.Infof("sendDockersState http=%d api_ok=%v err=%v api_err=%q", code, apiOK, err, apiErr)
	w.mu.Lock()
	if !apiOK {
		for _, t := range dockCopy {
			w.sendDockerStateJobs = upsertSendQueueByJobID(w.sendDockerStateJobs, t)
		}
		w.sendDockerStateTime = time.Now().Unix()
		w.mu.Unlock()
		return
	}
	w.sendDockerStateTime = time.Now().Unix()
	clearTrackedResponses(dockCopy)
	w.mu.Unlock()
	w.cfg.Logf("Finished sendDockersState function")
}

// clearTrackedResponses drops large per-job payloads (notably Docker Engine JSON)
// after they have been POSTed to the server so RSS does not grow with every poll.
func clearTrackedResponses(jobs []*trackedJob) {
	for _, t := range jobs {
		if t != nil {
			t.response = nil
		}
	}
}

func (w *Worker) logDockerPollFailure(jobID int, j *model.Job, resp map[string]any) {
	if j == nil {
		return
	}
	errMsg, _ := resp["response_error"].(string)
	w.cfg.Infof(
		"docker poll NOT saved by server job_id=%d docker_id=%d host=%q tls=%v status=%v code=%v err=%q containers=%d (need status=1, HTTP 2xx, non-empty container_states; enable DOCKER_DEBUG=true)",
		jobID, j.ID, j.Host, j.TLS,
		resp["status"], resp["status_code"], errMsg,
		handlers.DockerContainerStatesCount(resp),
	)
}

// monitoringAPIOK: HTTP 200 + non-empty body + JSON status 0 (monitoring API convention).
func (w *Worker) monitoringAPIOK(body []byte, httpCode int, httpErr error) (bool, string) {
	if httpErr != nil {
		return false, httpErr.Error()
	}
	if httpCode != 200 || len(body) == 0 {
		return false, fmt.Sprintf("http %d", httpCode)
	}
	var r apiResponseJSON
	if err := json.Unmarshal(body, &r); err != nil {
		return true, ""
	}
	if r.Status != 0 {
		msg := r.Error
		if msg == "" {
			msg = fmt.Sprintf("api status=%d", r.Status)
		}
		return false, msg
	}
	return true, ""
}

func (w *Worker) postMonitoring(url string, payload any) ([]byte, int, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:28.0) Gecko/20100101 Firefox/28.0")

	resp, err := w.http.Do(req)
	if err != nil {
		w.cfg.LogHTTPFailure("server", req.Method, url, 0, err, nil)
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		w.cfg.LogHTTPFailure("server", req.Method, url, resp.StatusCode, readErr, body)
	} else if readErr != nil {
		w.cfg.LogHTTPFailure("server", req.Method, url, resp.StatusCode, readErr, body)
	}
	return body, resp.StatusCode, readErr
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
