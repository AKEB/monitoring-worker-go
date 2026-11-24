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
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"monitoring-worker-go/internal/config"
	"monitoring-worker-go/internal/handlers"
	"monitoring-worker-go/internal/model"
	"monitoring-worker-go/internal/selfupdate"
)

type trackedJob struct {
	status       string
	state        string
	job          model.Job
	response     map[string]any
	running      bool
	cancel       context.CancelFunc
	done         chan struct{}
	started      int64
	sendQueuedAt int64 // unix: when current payload entered/refreshed on send queue
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
	pendingUpdate       *selfupdate.Command
	updateOnce          atomic.Bool

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
		upd := w.pendingUpdate
		w.pendingUpdate = nil
		w.mu.Unlock()
		if upd != nil && w.cfg.AutoUpdate {
			if w.updateOnce.CompareAndSwap(false, true) {
				w.cfg.Logf("self-update requested version=%s arch=%s", upd.Version, upd.Arch)
				if err := selfupdate.Apply(*upd, w.cfg.UpdateFromGitHub); err != nil {
					w.cfg.Errorf("self-update failed: %v", err)
					w.cfg.LastUpdateError = err.Error()
					w.updateOnce.Store(false)
				} else {
					w.cfg.Logf("self-update installed %s; exiting for restart", upd.Version)
					os.Exit(0)
				}
			}
		}
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
		w.drainSendQueues(now)

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
	for _, id := range sortedMapKeys(m) {
		t := m[id]
		if t == nil {
			continue
		}
		j := &t.job
		if j.RepeatSeconds <= 0 {
			j.RepeatSeconds = 60
		}
		// Domain/WHOIS etc. may be hourly+; keep a 24h ceiling only.
		j.RepeatSeconds = min(max(60, j.RepeatSeconds), 86400)
		j.Timeout = clampJobTimeout(j.Timeout)

		if t.running {
			if now > t.started+int64(j.Timeout) {
				if t.cancel != nil {
					t.cancel()
				}
				waitDone(t.done, 3*time.Second, func() {
					w.cfg.Errorf("job_id %d: handler did not exit after cancel, continuing", id)
				})
				t.running = false
				t.cancel = nil
				if t.response == nil {
					elapsed := time.Duration(now-t.started) * time.Second
					t.response = handlers.TimeoutResponse(elapsed)
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
					w.cfg.Errorf("Finish error job_id: %d", id)
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

		run := selectRunner(j.Type, docker)
		if run == nil {
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

	started := time.Now()
	resp := run(ctx, jobCopy)
	if ctx.Err() == context.DeadlineExceeded {
		if resp == nil {
			t.response = handlers.TimeoutResponse(time.Since(started))
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

func selectRunner(jobType int, docker bool) handlers.Runner {
	switch {
	case docker:
		return handlers.RunDocker
	case jobType == handlers.JobTypeHTTPS || jobType == handlers.JobTypeHTTPJSON || jobType == handlers.JobTypeHTTPKeyword:
		return handlers.RunHTTP
	case jobType == handlers.JobTypeTCP:
		return handlers.RunTCP
	case jobType == handlers.JobTypeUDP:
		return handlers.RunUDP
	case jobType == handlers.JobTypePing:
		return handlers.RunPing
	case jobType == handlers.JobTypeExporterDisk || jobType == handlers.JobTypeExporterMemory || jobType == handlers.JobTypeExporterCPU:
		return handlers.RunExporter
	case jobType == handlers.JobTypeDomain:
		return handlers.RunDomain
	case jobType == handlers.JobTypeDNS:
		return handlers.RunDNS
	case jobType == handlers.JobTypeSMTP:
		return handlers.RunSMTP
	case jobType == handlers.JobTypeNTP:
		return handlers.RunNTP
	case jobType == handlers.JobTypeWebsocket:
		return handlers.RunWebsocket
	case jobType == handlers.JobTypeMySQL || jobType == handlers.JobTypePostgres:
		return handlers.RunSQL
	case jobType == handlers.JobTypeRedis:
		return handlers.RunRedis
	case jobType == handlers.JobTypeMongoDB:
		return handlers.RunMongo
	case jobType == handlers.JobTypeSFTP:
		return handlers.RunSFTP
	case jobType == handlers.JobTypeSNMP:
		return handlers.RunSNMP
	case jobType == handlers.JobTypeRADIUS:
		return handlers.RunRADIUS
	case jobType == handlers.JobTypeGRPCKeyword:
		return handlers.RunGRPCKeyword
	case jobType == handlers.JobTypeMQTT:
		return handlers.RunMQTT
	case jobType == handlers.JobTypeRabbitMQ:
		return handlers.RunRabbitMQ
	case jobType == handlers.JobTypeKafkaProducer:
		return handlers.RunKafkaProducer
	default:
		return nil
	}
}

// logUpdateTime is the timestamp used to bucket monitoring_logs on the server.
// Prefer the check start so success and failure land in the same minute the
// probe actually ran, not the schedule cursor after bumpScheduleAfterFailureOrTimeout.
func logUpdateTime(t *trackedJob) int64 {
	if t == nil {
		return time.Now().Unix()
	}
	if t.started > 0 {
		return t.started
	}
	if t.job.StartTime > 0 {
		return t.job.StartTime
	}
	if t.job.UpdateTime > 0 {
		return t.job.UpdateTime
	}
	return time.Now().Unix()
}

// clampJobTimeout matches handlers.capTimeout: unset/invalid → 15, otherwise 1..60.
func clampJobTimeout(v int) int {
	if v <= 0 {
		return 15
	}
	return min(max(1, v), 60)
}

func (w *Worker) enqueueSend(t *trackedJob, docker bool) {
	if t == nil || t.job.JobID == 0 {
		return
	}
	now := time.Now().Unix()
	if docker {
		w.sendDockerStateJobs = upsertSendQueueByJobID(w.sendDockerStateJobs, t, now)
	} else {
		w.sendStateJobs = upsertSendQueueByJobID(w.sendStateJobs, t, now)
	}
}

// maxSendBatchesPerDrain caps how many /state/ batches one loop tick may send.
// Enough to catch up after downtime without dumping the entire backlog at once.
const maxSendBatchesPerDrain = 4

// drainSendQueues sends pending results FIFO, limited to maxSendBatchesPerDrain
// batches per due window. Stale queued results (unchanged payload waiting too long
// while the server was unreachable) are dropped instead of retried forever.
func (w *Worker) drainSendQueues(now int64) {
	w.mu.Lock()
	if dropped := w.pruneStaleSendQueuesLocked(now); dropped > 0 {
		w.cfg.Logf("dropped %d stale queued results (server unreachable too long)", dropped)
	}
	jobsDue := w.sendStateTime < now-int64(w.cfg.ResponseSendTimeout) && len(w.sendStateJobs) > 0
	dockDue := w.sendDockerStateTime < now-int64(w.cfg.ResponseSendTimeout) && len(w.sendDockerStateJobs) > 0
	w.mu.Unlock()

	for i := 0; jobsDue && i < maxSendBatchesPerDrain; i++ {
		if !w.sendJobsState() {
			break
		}
		w.mu.Lock()
		jobsDue = len(w.sendStateJobs) > 0
		w.mu.Unlock()
	}
	for i := 0; dockDue && i < maxSendBatchesPerDrain; i++ {
		if !w.sendDockersState() {
			break
		}
		w.mu.Lock()
		dockDue = len(w.sendDockerStateJobs) > 0
		w.mu.Unlock()
	}
}

func (w *Worker) pruneStaleSendQueuesLocked(now int64) int {
	var n int
	var dropped []*trackedJob
	w.sendStateJobs, dropped = dropStaleSendQueue(w.sendStateJobs, now, sendResultMaxAgeSec)
	n += len(dropped)
	clearTrackedResponses(dropped)
	w.sendDockerStateJobs, dropped = dropStaleSendQueue(w.sendDockerStateJobs, now, sendResultMaxAgeSec)
	n += len(dropped)
	clearTrackedResponses(dropped)
	return n
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
		// Do not claim TLS revision without cached PEM — forces server to resend tls_* after worker restart.
		if t.job.TLS && !dockerJobHasCachedTLS(t.job) {
			continue
		}
		out[strconv.Itoa(t.job.ID)] = t.job.DockerUpdateTime
	}
	return out
}

func dockerJobHasCachedTLS(j model.Job) bool {
	if !j.TLS {
		return true
	}
	// Claim docker_tls_sync only when custom CA is cached (self-signed daemons).
	// mTLS-only without CA must not suppress server PEM resend.
	return j.TLSCAFile != ""
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
		"arch":             runtime.GOARCH,
	}
	if errMsg := strings.TrimSpace(w.cfg.LastUpdateError); errMsg != "" {
		payload["update_error"] = errMsg
	}
	if len(tlsSync) > 0 {
		payload["docker_tls_sync"] = tlsSync
	}
	getURL := w.cfg.ServerHost + "api/monitoring/get/"
	body, code, err := w.postMonitoring(getURL, payload)
	if err != nil || code != 200 || len(body) == 0 {
		w.cfg.Errorf("getJobs HTTP err=%v code=%d", err, code)
		time.Sleep(10 * time.Second)
		return
	}
	w.cfg.LogMonitoringAPIError("server", http.MethodPost, getURL, code, body)
	var parsed getJobsJSON
	if err := json.Unmarshal(body, &parsed); err != nil {
		w.cfg.Errorf("getJobs JSON: %v", err)
		time.Sleep(10 * time.Second)
		return
	}
	if parsed.Error != "" {
		w.cfg.Errorf("getJobs error field: %s", parsed.Error)
		time.Sleep(10 * time.Second)
		return
	}
	if parsed.Status != 0 {
		w.cfg.Errorf("getJobs status=%d", parsed.Status)
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
	if w.cfg.LastUpdateError != "" {
		// Cleared after a successful get that carried the error.
		w.cfg.LastUpdateError = ""
	}

	w.cfg.Logf("getJobs: %d jobs, %d dockers", len(jobs), len(dockers))
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
	if u, ok := cfgObj["update"]; ok {
		if m, ok := u.(map[string]any); ok {
			cmd := selfupdate.Command{
				Version: strings.TrimSpace(fmt.Sprint(m["version"])),
				URL:     strings.TrimSpace(fmt.Sprint(m["url"])),
				Sha256:  strings.TrimSpace(fmt.Sprint(m["sha256"])),
				Arch:    strings.TrimSpace(fmt.Sprint(m["arch"])),
			}
			if cmd.URL != "" && cmd.URL != "<nil>" {
				w.pendingUpdate = &cmd
			}
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
	byKey := make(map[int]model.Job, len(incoming))
	newKeys := make([]int, 0)
	for _, j := range incoming {
		key := trackKey(j)
		if key == 0 || j.JobID == 0 {
			continue
		}
		byKey[key] = j
		if _, ok := w.jobs[key]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	for id, t := range w.jobs {
		if _, ok := byKey[id]; !ok {
			t.status = "deleted"
			t.job = model.Job{}
			t.response = nil
			continue
		}
		t.status = "updated"
		nj := byKey[id]
		if t.job.UpdateTime > nj.UpdateTime {
			nj.UpdateTime = t.job.UpdateTime
		}
		if t.job.StartTime > 0 {
			nj.StartTime = t.job.StartTime
		}
		t.job = nj
	}
	for _, id := range newKeys {
		w.jobs[id] = &trackedJob{status: "created", job: byKey[id]}
	}
}

func (w *Worker) syncDockersLocked(incoming []model.Job) {
	byKey := make(map[int]model.Job, len(incoming))
	newKeys := make([]int, 0)
	for _, j := range incoming {
		key := trackKey(j)
		if key == 0 || j.JobID == 0 {
			continue
		}
		byKey[key] = j
		if _, ok := w.dockers[key]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	for id, t := range w.dockers {
		if _, ok := byKey[id]; !ok {
			t.status = "deleted"
			t.job = model.Job{}
			t.response = nil
			continue
		}
		t.status = "updated"
		nj := model.MergeDockerTLS(t.job, byKey[id])
		if t.job.UpdateTime > nj.UpdateTime {
			nj.UpdateTime = t.job.UpdateTime
		}
		if t.job.StartTime > 0 {
			nj.StartTime = t.job.StartTime
		}
		t.job = nj
	}
	for _, id := range newKeys {
		w.dockers[id] = &trackedJob{status: "created", job: byKey[id]}
	}
}

// sendJobsState posts the oldest pending batch from sendStateJobs (FIFO).
// Returns false if the queue was empty or the POST failed (retry later).
func (w *Worker) sendJobsState() bool {
	w.mu.Lock()
	if len(w.sendStateJobs) == 0 {
		w.sendStateTime = time.Now().Unix()
		w.mu.Unlock()
		return false
	}
	batchSize := sendQueueBatchSize(w.cfg.WorkerThreads)
	var jobsCopy []*trackedJob
	jobsCopy, w.sendStateJobs = takeSendQueueFront(w.sendStateJobs, batchSize)
	w.mu.Unlock()

	w.cfg.Logf("Starting sendJobsState function")
	params := map[string]any{
		"worker_key_hash":  w.cfg.WorkerKeyHash,
		"protocol_version": w.cfg.ProtocolVersion,
		"worker_version":   w.cfg.WorkerVersion,
		"arch":             runtime.GOARCH,
	}
	jobsOut := make([]any, 0, len(jobsCopy))
	for _, t := range jobsCopy {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		w.cfg.Logf("Sending job %d monitor %d", t.job.JobID, t.job.ID)
		jobsOut = append(jobsOut, map[string]any{
			"job_id":     t.job.JobID,
			"monitor_id": t.job.ID,
			// update_time = local schedule cursor (may be now/now-repeat after errors).
			"update_time": t.job.UpdateTime,
			// start_time = when the check began; server buckets the log by this
			// so failure bumps do not push the oval into the next minute.
			"start_time": logUpdateTime(t),
			"response":   t.response,
		})
	}
	if len(jobsOut) == 0 {
		w.mu.Lock()
		w.sendStateTime = time.Now().Unix()
		w.mu.Unlock()
		return true
	}
	params["jobs"] = jobsOut
	stateURL := w.cfg.ServerHost + "api/monitoring/state/"
	body, code, err := w.postMonitoring(stateURL, params)
	apiOK, apiErr := w.monitoringAPIOK(body, code, err)
	if !apiOK && code >= 200 && code < 300 {
		w.cfg.LogMonitoringAPIError("server", http.MethodPost, stateURL, code, body)
	}
	now := time.Now().Unix()
	w.mu.Lock()
	if !apiOK {
		if batchIsStale(jobsCopy, now, sendResultMaxAgeSec) {
			clearTrackedResponses(jobsCopy)
			w.sendStateTime = now
			w.mu.Unlock()
			w.cfg.Errorf("sendJobsState drop stale batch: http=%d err=%v api_err=%q", code, err, apiErr)
			return false
		}
		w.sendStateJobs = prependSendQueueFront(w.sendStateJobs, jobsCopy)
		w.sendStateTime = now
		w.mu.Unlock()
		w.cfg.Errorf("sendJobsState retry later: http=%d err=%v api_err=%q", code, err, apiErr)
		return false
	}
	w.sendStateTime = now
	clearTrackedResponses(jobsCopy)
	w.mu.Unlock()
	w.cfg.Logf("Finished sendJobsState function")
	return true
}

func (w *Worker) sendDockersState() bool {
	w.mu.Lock()
	if len(w.sendDockerStateJobs) == 0 {
		w.sendDockerStateTime = time.Now().Unix()
		w.mu.Unlock()
		return false
	}
	batchSize := sendQueueBatchSize(w.cfg.WorkerThreads)
	var dockCopy []*trackedJob
	dockCopy, w.sendDockerStateJobs = takeSendQueueFront(w.sendDockerStateJobs, batchSize)
	w.mu.Unlock()

	w.cfg.Logf("Starting sendDockersState function")
	params := map[string]any{
		"worker_key_hash":  w.cfg.WorkerKeyHash,
		"protocol_version": w.cfg.ProtocolVersion,
		"worker_version":   w.cfg.WorkerVersion,
		"arch":             runtime.GOARCH,
	}
	out := make([]any, 0, len(dockCopy))
	for _, t := range dockCopy {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		n := handlers.DockerContainerStatesCount(t.response)
		accepted := handlers.DockerResponseAcceptedByServer(t.response)
		if !accepted {
			w.logDockerPollFailure(t.job.JobID, &t.job, t.response)
		} else {
			w.cfg.Logf("docker POST job_id=%d docker_id=%d host=%q accepted=true containers=%d", t.job.JobID, t.job.ID, t.job.Host, n)
		}
		w.cfg.Logf("Sending dockers job %d docker %d", t.job.JobID, t.job.ID)
		out = append(out, map[string]any{
			"job_id":      t.job.JobID,
			"docker_id":   t.job.ID,
			"update_time": t.job.UpdateTime,
			"start_time":  logUpdateTime(t),
			"response":    t.response,
		})
	}
	if len(out) == 0 {
		w.mu.Lock()
		w.sendDockerStateTime = time.Now().Unix()
		w.mu.Unlock()
		return true
	}
	params["dockers"] = out
	if w.cfg.Debug {
		b, _ := json.Marshal(params)
		w.cfg.Logf("sendDockersState payload: %s", string(b))
	}
	stateURL := w.cfg.ServerHost + "api/monitoring/state/"
	body, code, err := w.postMonitoring(stateURL, params)
	apiOK, apiErr := w.monitoringAPIOK(body, code, err)
	if !apiOK && code >= 200 && code < 300 {
		w.cfg.LogMonitoringAPIError("server", http.MethodPost, stateURL, code, body)
	}
	if !apiOK {
		w.cfg.Errorf("sendDockersState failed: http=%d err=%v api_err=%q", code, err, apiErr)
	} else {
		w.cfg.Logf("sendDockersState http=%d api_ok=true", code)
	}
	now := time.Now().Unix()
	w.mu.Lock()
	if !apiOK {
		if batchIsStale(dockCopy, now, sendResultMaxAgeSec) {
			clearTrackedResponses(dockCopy)
			w.sendDockerStateTime = now
			w.mu.Unlock()
			w.cfg.Errorf("sendDockersState drop stale batch: http=%d err=%v api_err=%q", code, err, apiErr)
			return false
		}
		w.sendDockerStateJobs = prependSendQueueFront(w.sendDockerStateJobs, dockCopy)
		w.sendDockerStateTime = now
		w.mu.Unlock()
		return false
	}
	w.sendDockerStateTime = now
	clearTrackedResponses(dockCopy)
	w.mu.Unlock()
	w.cfg.Logf("Finished sendDockersState function")
	return true
}

// clearTrackedResponses drops large per-job payloads (notably Docker Engine JSON)
// after they have been POSTed (or abandoned as stale) so RSS does not grow, and
// clears sendQueuedAt so the next probe result starts a fresh send-age window.
func clearTrackedResponses(jobs []*trackedJob) {
	for _, t := range jobs {
		if t != nil {
			t.response = nil
			t.sendQueuedAt = 0
		}
	}
}

func (w *Worker) logDockerPollFailure(jobID int, j *model.Job, resp map[string]any) {
	if j == nil {
		return
	}
	errMsg, _ := resp["response_error"].(string)
	w.cfg.Errorf(
		"docker poll NOT saved by server job_id=%d docker_id=%d host=%q tls=%v status=%v code=%v err=%q containers=%d (need status=1, HTTP 2xx, non-empty container_states; enable DOCKER_DEBUG=true for request/response details)",
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
