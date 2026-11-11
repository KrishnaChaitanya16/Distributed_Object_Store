package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	cfg := loadConfig()
	log.Printf("worker %s starting on :%d  data=%s  controller=%s",
		cfg.workerID, cfg.port, cfg.dataDir, cfg.controllerAddr)

	// TODO: construct ChunkStore (internal/worker/storage)
	// TODO: construct scrubber goroutine
	// TODO: register HTTP routes (PUT/GET/DELETE chunk, inventory, replicate-peer)
	// TODO: register with controller (with retry + backoff)
	// TODO: start heartbeat goroutine (every 2s)
	// TODO: start HTTP server + graceful shutdown on SIGTERM

	fmt.Println("worker wired — replace TODOs with real wiring")
}

type workerConfig struct {
	workerID       string
	port           int
	dataDir        string
	controllerAddr string
}

func loadConfig() workerConfig {
	wid := os.Getenv("WORKER_ID")
	if wid == "" {
		log.Fatal("WORKER_ID env var is required")
	}
	return workerConfig{
		workerID:       wid,
		port:           envInt("WORKER_PORT", 8001),
		dataDir:        envStr("DATA_DIR", "/data"),
		controllerAddr: envStr("CONTROLLER_ADDRESS", "controller:8080"),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("invalid %s=%q: %v", key, v, err)
		}
		return n
	}
	return def
}
