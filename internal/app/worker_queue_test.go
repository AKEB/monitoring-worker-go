package app

import (
	"testing"

	"monitoring-worker-go/internal/model"
)

func TestClampJobTimeout(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, 15},
		{-1, 15},
		{1, 1},
		{5, 5},
		{15, 15},
		{60, 60},
		{61, 60},
	}
	for _, tc := range cases {
		if got := clampJobTimeout(tc.in); got != tc.want {
			t.Fatalf("clampJobTimeout(%d)=%d want %d", tc.in, got, tc.want)
		}
	}
}

func TestSortedMapKeys_longestWaitingFirst(t *testing.T) {
	m := map[int]*trackedJob{
		3: {job: model.Job{JobID: 3, UpdateTime: 100, RepeatSeconds: 60}},
		1: {job: model.Job{JobID: 1, UpdateTime: 50, RepeatSeconds: 60}},
		2: {job: model.Job{JobID: 2, UpdateTime: 200, RepeatSeconds: 60}},
	}
	got := sortedMapKeys(m)
	want := []int{1, 3, 2} // due 110, 160, 260
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestJobNextDueUnix_respectsHourlyDomainRepeat(t *testing.T) {
	due := jobNextDueUnix(model.Job{UpdateTime: 1000, RepeatSeconds: 3600})
	if due != 4600 {
		t.Fatalf("hourly due=%d want 4600 (must not clamp to 600)", due)
	}
	due = jobNextDueUnix(model.Job{UpdateTime: 1000, RepeatSeconds: 100000})
	if due != 1000+86400 {
		t.Fatalf("ceiling due=%d want %d", due, 1000+86400)
	}
}

func TestUpsertSendQueue_keepsFIFOPosition(t *testing.T) {
	a := &trackedJob{job: model.Job{JobID: 1, ID: 10}, sendQueuedAt: 10}
	b := &trackedJob{job: model.Job{JobID: 2, ID: 20}, sendQueuedAt: 20}
	q := []*trackedJob{a, b}

	updated := &trackedJob{job: model.Job{JobID: 1, ID: 10}}
	q = upsertSendQueueByJobID(q, updated, 99)
	if len(q) != 2 {
		t.Fatalf("len=%d", len(q))
	}
	// Position stays FIFO; send age refreshes with the new probe result.
	if q[0].job.JobID != 1 || q[0].sendQueuedAt != 99 {
		t.Fatalf("head job_id=1 sendQueuedAt=%d want 99", q[0].sendQueuedAt)
	}
	if q[1].job.JobID != 2 {
		t.Fatalf("tail job_id=%d", q[1].job.JobID)
	}

	c := &trackedJob{job: model.Job{JobID: 3, ID: 30}}
	q = upsertSendQueueByJobID(q, c, 30)
	if len(q) != 3 || q[2].job.JobID != 3 || q[2].sendQueuedAt != 30 {
		t.Fatalf("append: %+v", q)
	}
}

func TestUpsertSendQueue_folderInheritedSharedJobID(t *testing.T) {
	// Same folder job_id, different monitor ids must stay separate.
	a := &trackedJob{job: model.Job{JobID: 99, ID: 415}}
	b := &trackedJob{job: model.Job{JobID: 99, ID: 451}}
	q := upsertSendQueueByJobID(nil, a, 1)
	q = upsertSendQueueByJobID(q, b, 2)
	if len(q) != 2 {
		t.Fatalf("len=%d want 2", len(q))
	}
	if trackKey(q[0].job) != 415 || trackKey(q[1].job) != 451 {
		t.Fatalf("keys=%d,%d", trackKey(q[0].job), trackKey(q[1].job))
	}
	updated := &trackedJob{job: model.Job{JobID: 99, ID: 415}}
	q = upsertSendQueueByJobID(q, updated, 3)
	if len(q) != 2 || q[0].sendQueuedAt != 3 || q[0].job.ID != 415 {
		t.Fatalf("update collapsed queue: len=%d %+v", len(q), q)
	}
}

func TestTrackKey(t *testing.T) {
	if got := trackKey(model.Job{JobID: 7, ID: 415}); got != 415 {
		t.Fatalf("prefer id: %d", got)
	}
	if got := trackKey(model.Job{JobID: 7}); got != 7 {
		t.Fatalf("fallback job_id: %d", got)
	}
}

// Regression for v2.4.4: after a stale drop, the same trackedJob pointer still
// carried an old sendQueuedAt; re-enqueue then got pruned again immediately.
func TestUpsertSendQueue_refreshesPoisonedSendQueuedAt(t *testing.T) {
	now := int64(1000)
	poisoned := &trackedJob{job: model.Job{JobID: 7}, sendQueuedAt: 100} // age would be 900s
	q := upsertSendQueueByJobID(nil, poisoned, now)
	if len(q) != 1 || q[0].sendQueuedAt != now {
		t.Fatalf("re-enqueue sendQueuedAt=%d want %d", q[0].sendQueuedAt, now)
	}
	fresh, dropped := dropStaleSendQueue(q, now, 180)
	if len(dropped) != 0 || len(fresh) != 1 {
		t.Fatalf("fresh=%v dropped=%v", jobIDs(fresh), jobIDs(dropped))
	}
}

func TestPrependSendQueueFront(t *testing.T) {
	q := []*trackedJob{
		{job: model.Job{JobID: 3}, sendQueuedAt: 30},
		{job: model.Job{JobID: 4}, sendQueuedAt: 40},
	}
	batch := []*trackedJob{
		{job: model.Job{JobID: 1}, sendQueuedAt: 10},
		{job: model.Job{JobID: 2}, sendQueuedAt: 20},
	}
	got := prependSendQueueFront(q, batch)
	if len(got) != 4 {
		t.Fatalf("len=%d", len(got))
	}
	for i, id := range []int{1, 2, 3, 4} {
		if got[i].job.JobID != id {
			t.Fatalf("i=%d got job_id=%d", i, got[i].job.JobID)
		}
	}
}

func TestTakeSendQueueFront(t *testing.T) {
	q := []*trackedJob{
		{job: model.Job{JobID: 1}},
		{job: model.Job{JobID: 2}},
		{job: model.Job{JobID: 3}},
	}
	batch, rest := takeSendQueueFront(q, 2)
	if len(batch) != 2 || len(rest) != 1 || batch[0].job.JobID != 1 || rest[0].job.JobID != 3 {
		t.Fatalf("batch=%v rest=%v", batch, rest)
	}
}

func TestDropStaleSendQueue(t *testing.T) {
	now := int64(1000)
	q := []*trackedJob{
		{job: model.Job{JobID: 1}, sendQueuedAt: 100},  // stale
		{job: model.Job{JobID: 2}, sendQueuedAt: 900},  // fresh
		{job: model.Job{JobID: 3}, sendQueuedAt: 821},  // age 179 → fresh
		{job: model.Job{JobID: 4}, sendQueuedAt: 820},  // age 180 → stale
	}
	fresh, dropped := dropStaleSendQueue(q, now, 180)
	if len(fresh) != 2 || fresh[0].job.JobID != 2 || fresh[1].job.JobID != 3 {
		t.Fatalf("fresh=%v", jobIDs(fresh))
	}
	if len(dropped) != 2 || dropped[0].job.JobID != 1 || dropped[1].job.JobID != 4 {
		t.Fatalf("dropped=%v", jobIDs(dropped))
	}
}

func jobIDs(q []*trackedJob) []int {
	out := make([]int, 0, len(q))
	for _, t := range q {
		if t != nil {
			out = append(out, t.job.JobID)
		}
	}
	return out
}

func TestBatchIsStale(t *testing.T) {
	now := int64(1000)
	stale := []*trackedJob{
		{job: model.Job{JobID: 1}, sendQueuedAt: 100},
		{job: model.Job{JobID: 2}, sendQueuedAt: 200},
	}
	if !batchIsStale(stale, now, 180) {
		t.Fatal("expected stale")
	}
	mixed := []*trackedJob{
		{job: model.Job{JobID: 1}, sendQueuedAt: 100},
		{job: model.Job{JobID: 2}, sendQueuedAt: 950},
	}
	if batchIsStale(mixed, now, 180) {
		t.Fatal("mixed batch must not be stale")
	}
}
