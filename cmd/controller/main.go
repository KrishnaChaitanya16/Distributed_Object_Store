package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/config"
)

type controllerConfig struct {
	port       int
	dbPath     string
	vnodeCount int
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatalf("controller config: %v", err)
	}
	log.Printf("controller starting on :%d db=%s vnodes=%d", cfg.port, cfg.dbPath, cfg.vnodeCount)

	// TODO: wire bbolt, MetadataRepository, PlacementStrategy, ExistenceFilter, HealthMonitor, WorkerClientPool
	// TODO: start healer, reconciler, GC goroutines and the HTTP server

	waitForShutdown()
}

func loadConfig(getenv config.Getenv) (controllerConfig, error) {
	port, err := config.Port(getenv, "CONTROLLER_PORT", 8080)
	if err != nil {
		return controllerConfig{}, err
	}
	vnodes, err := config.PositiveInt(getenv, "VNODE_COUNT", 50)
	if err != nil {
		return controllerConfig{}, err
	}
	return controllerConfig{
		port:       port,
		dbPath:     config.Str(getenv, "DB_PATH", "/data/meta.db"),
		vnodeCount: vnodes,
	}, nil
}

func waitForShutdown() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("received %s, shutting down", <-sig)
}
