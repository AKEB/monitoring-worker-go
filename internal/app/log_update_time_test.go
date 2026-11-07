package app

import (
	"testing"

	"monitoring-worker-go/internal/model"
)

func TestLogUpdateTimePrefersStarted(t *testing.T) {
	tj := &trackedJob{
		started: 1700000040,
		job: model.Job{
			StartTime:  1700000030,
			UpdateTime: 1700000100, // schedule bump after failure
		},
	}
	if got := logUpdateTime(tj); got != 1700000040 {
		t.Fatalf("expected started, got %d", got)
	}
}

func TestLogUpdateTimeFallsBackToStartTime(t *testing.T) {
	tj := &trackedJob{
		job: model.Job{
			StartTime:  1700000030,
			UpdateTime: 1700000100,
		},
	}
	if got := logUpdateTime(tj); got != 1700000030 {
		t.Fatalf("expected StartTime, got %d", got)
	}
}
