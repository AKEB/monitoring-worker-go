package app

import (
	"sort"

	"monitoring-worker-go/internal/model"
)

// jobNextDueUnix is when the job becomes eligible to run (UpdateTime + repeat_seconds).
func jobNextDueUnix(j model.Job) int64 {
	rs := j.RepeatSeconds
	if rs <= 0 {
		rs = 60
	}
	if rs < 60 {
		rs = 60
	}
	if rs > 600 {
		rs = 600
	}
	return j.UpdateTime + int64(rs)
}

// trackKey uniquely identifies a check target in the worker maps/queues.
// Prefer monitor/docker id so folder-inherited jobs (shared job_id, different id)
// do not collapse into a single tracked entry.
func trackKey(j model.Job) int {
	if j.ID > 0 {
		return j.ID
	}
	return j.JobID
}

// sortedMapKeys returns map keys ordered by next due time (longest waiting first),
// then track key for stable ordering.
func sortedMapKeys(m map[int]*trackedJob) []int {
	if len(m) == 0 {
		return nil
	}
	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ti, tj := m[ids[i]], m[ids[j]]
		if ti == nil || tj == nil {
			return ids[i] < ids[j]
		}
		dueI, dueJ := jobNextDueUnix(ti.job), jobNextDueUnix(tj.job)
		if dueI != dueJ {
			return dueI < dueJ
		}
		return ids[i] < ids[j]
	})
	return ids
}

func sendQueueBatchSize(workerThreads int) int {
	n := workerThreads * 4
	if n < 32 {
		n = 32
	}
	if n > 200 {
		n = 200
	}
	return n
}

// takeSendQueueFront removes up to max items from the front of the FIFO send queue.
func takeSendQueueFront(q []*trackedJob, max int) (batch, rest []*trackedJob) {
	if len(q) == 0 {
		return nil, nil
	}
	if max <= 0 || len(q) <= max {
		return q, nil
	}
	return q[:max], q[max:]
}

// sendResultMaxAgeSec: results queued longer than this while the monitoring
// server is unreachable are dropped instead of retried forever (anti-DDOS on recovery).
const sendResultMaxAgeSec int64 = 180

// dropStaleSendQueue removes entries whose sendQueuedAt is older than maxAge.
// Dropped jobs are returned so callers can clear large response payloads.
func dropStaleSendQueue(q []*trackedJob, now, maxAge int64) (fresh, dropped []*trackedJob) {
	if len(q) == 0 {
		return nil, nil
	}
	if maxAge < 1 {
		return q, nil
	}
	fresh = make([]*trackedJob, 0, len(q))
	for _, t := range q {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		queuedAt := t.sendQueuedAt
		if queuedAt < 1 {
			queuedAt = now
			t.sendQueuedAt = now
		}
		if now-queuedAt >= maxAge {
			dropped = append(dropped, t)
			continue
		}
		fresh = append(fresh, t)
	}
	return fresh, dropped
}

// batchIsStale reports whether every item in the batch has aged past maxAge.
func batchIsStale(batch []*trackedJob, now, maxAge int64) bool {
	if len(batch) == 0 || maxAge < 1 {
		return false
	}
	for _, t := range batch {
		if t == nil {
			continue
		}
		queuedAt := t.sendQueuedAt
		if queuedAt < 1 {
			return false
		}
		if now-queuedAt < maxAge {
			return false
		}
	}
	return true
}

// upsertSendQueueByJobID keeps at most one pending send per track key (latest result wins).
// New entries are appended; updates keep queue position (FIFO fairness) but always
// refresh sendQueuedAt. Reusing a stale sendQueuedAt after prune/drop permanently
// poisoned job_ids in v2.4.4: the same trackedJob pointer kept an old timestamp,
// so every new probe result was dropped as "stale" before /state/ could be POSTed.
// Track key is monitor/docker id when set, so folder-inherited jobs with a shared
// job_id still queue separately.
func upsertSendQueueByJobID(q []*trackedJob, t *trackedJob, now int64) []*trackedJob {
	if t == nil || t.job.JobID == 0 {
		return q
	}
	key := trackKey(t.job)
	if key == 0 {
		return q
	}
	t.sendQueuedAt = now
	for i, x := range q {
		if x != nil && trackKey(x.job) == key {
			q[i] = t
			return q
		}
	}
	return append(q, t)
}

// prependSendQueueFront puts failed batch back at the head; batch order is preserved.
// Entries already in q with the same track key are dropped in favor of the batch item.
func prependSendQueueFront(q, batch []*trackedJob) []*trackedJob {
	if len(batch) == 0 {
		return q
	}
	seen := make(map[int]struct{}, len(batch))
	out := make([]*trackedJob, 0, len(batch)+len(q))
	for _, t := range batch {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		key := trackKey(t.job)
		if key == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, t)
	}
	for _, t := range q {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		key := trackKey(t.job)
		if key == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		out = append(out, t)
	}
	return out
}
