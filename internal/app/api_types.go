package app

import (
	"encoding/json"

	"monitoring-worker-go/internal/model"
)

type getJobsJSON struct {
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
	Jobs    []model.Job     `json:"jobs"`
	Dockers []model.Job     `json:"dockers"`
}
