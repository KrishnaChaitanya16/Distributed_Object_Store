package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/config"
)

type workerConfig struct {
	workerID       string
	port           int
	dataDir        string
	controllerAddr string
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatalf("worker config: %v", err)
	}
	log.Printf("worker %s starting on :%d data=%s controller=%s", cfg.workerID, cfg.port, cfg.dataDir, cfg.controllerAddr)

	// TODO: wire ChunkStore, scrubber, HTTP routes, registration + heartbeat loops

	waitForShutdown()
}

func loadConfig(getenv config.Getenv) (workerConfig, error) {
	id, err := config.Required(getenv, "WORKER_ID")
	if err != nil {
		return workerConfig{}, err
	}
	port, err := config.Port(getenv, "WORKER_PORT", 8001)
	if err != nil {
		return workerConfig{}, err
	}
	ctrl, err := config.HostPort(getenv, "CONTROLLER_ADDRESS", "controller:8080")
	if err != nil {
		return workerConfig{}, err
	}
	return workerConfig{
		workerID:       id,
		port:           port,
		dataDir:        config.Str(getenv, "DATA_DIR", "/data"),
		controllerAddr: ctrl,
	}, nil
}

func waitForShutdown() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("received %s, shutting down", <-sig)
}
