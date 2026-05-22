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

// sortedMapKeys returns map keys ordered by next due time (longest waiting first),
// then job_id for stable ordering.
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

// upsertSendQueueByJobID keeps at most one pending send per job_id (latest result wins).
// New entries are appended; updates keep queue position and sendQueuedAt (FIFO fairness).
func upsertSendQueueByJobID(q []*trackedJob, t *trackedJob, now int64) []*trackedJob {
	if t == nil || t.job.JobID == 0 {
		return q
	}
	for i, x := range q {
		if x != nil && x.job.JobID == t.job.JobID {
			t.sendQueuedAt = x.sendQueuedAt
			q[i] = t
			return q
		}
	}
	if t.sendQueuedAt == 0 {
		t.sendQueuedAt = now
	}
	return append(q, t)
}

// prependSendQueueFront puts failed batch back at the head; batch order is preserved.
// Entries already in q with the same job_id are dropped in favor of the batch item.
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
		if _, ok := seen[t.job.JobID]; ok {
			continue
		}
		seen[t.job.JobID] = struct{}{}
		out = append(out, t)
	}
	for _, t := range q {
		if t == nil || t.job.JobID == 0 {
			continue
		}
		if _, ok := seen[t.job.JobID]; ok {
			continue
		}
		out = append(out, t)
	}
	return out
}
