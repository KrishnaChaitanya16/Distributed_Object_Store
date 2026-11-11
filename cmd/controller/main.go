package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	cfg := loadConfig()
	log.Printf("controller starting on :%d  db=%s", cfg.port, cfg.dbPath)

	// TODO: open bbolt db
	// TODO: construct MetadataRepository
	// TODO: construct PlacementStrategy (hash ring)
	// TODO: construct ExistenceFilter (bloom)
	// TODO: construct HealthMonitor + subscribe to offline events
	// TODO: construct WorkerClientPool
	// TODO: construct service layer
	// TODO: start background goroutines (healer, reconciler, GC)
	// TODO: register HTTP routes
	// TODO: start HTTP server + graceful shutdown on SIGTERM

	fmt.Println("controller wired — replace TODOs with real wiring")
}

type controllerConfig struct {
	port       int
	dbPath     string
	vnodeCount int
}

func loadConfig() controllerConfig {
	return controllerConfig{
		port:       envInt("CONTROLLER_PORT", 8080),
		dbPath:     envStr("DB_PATH", "/data/meta.db"),
		vnodeCount: envInt("VNODE_COUNT", 50),
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
