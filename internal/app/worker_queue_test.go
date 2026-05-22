package app

import (
	"testing"

	"monitoring-worker-go/internal/model"
)

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

func TestUpsertSendQueue_keepsFIFOPosition(t *testing.T) {
	a := &trackedJob{job: model.Job{JobID: 1}, sendQueuedAt: 10}
	b := &trackedJob{job: model.Job{JobID: 2}, sendQueuedAt: 20}
	q := []*trackedJob{a, b}

	updated := &trackedJob{job: model.Job{JobID: 1}}
	q = upsertSendQueueByJobID(q, updated, 99)
	if len(q) != 2 {
		t.Fatalf("len=%d", len(q))
	}
	if q[0].job.JobID != 1 || q[0].sendQueuedAt != 10 {
		t.Fatalf("head job_id=1 sendQueuedAt=%d", q[0].sendQueuedAt)
	}
	if q[1].job.JobID != 2 {
		t.Fatalf("tail job_id=%d", q[1].job.JobID)
	}

	c := &trackedJob{job: model.Job{JobID: 3}}
	q = upsertSendQueueByJobID(q, c, 30)
	if len(q) != 3 || q[2].job.JobID != 3 || q[2].sendQueuedAt != 30 {
		t.Fatalf("append: %+v", q)
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
