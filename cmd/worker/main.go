package main

import (
	"fmt"
	"os"

	"monitoring-worker-go/internal/app"
	"monitoring-worker-go/internal/config"
)

func main() {
	cfg := config.Get()
	if cfg.ServerHost == "" || cfg.WorkerKeyHash == "" {
		fmt.Fprintln(os.Stderr, "SERVER_HOST and WORKER_KEY_HASH are required (e.g. from .env next to the binary).")
		os.Exit(1)
	}
	w := app.NewWorker(cfg)
	w.Init()
	w.Loop()
	fmt.Println("Exiting")
}
