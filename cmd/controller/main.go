package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/config"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/logger"
)

type controllerConfig struct {
	addr   string
	dbPath string
}

func loadConfig(getenv config.Getenv) (controllerConfig, error) {
	port, err := config.Port(getenv, "CONTROLLER_PORT", 8080)
	if err != nil {
		return controllerConfig{}, err
	}
	dbPath := config.Str(getenv, "DB_PATH", "data/meta.db")
	return controllerConfig{
		addr:   fmt.Sprintf(":%d", port),
		dbPath: dbPath,
	}, nil
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		slog.Error("controller config", "err", err)
		os.Exit(1)
	}

	logger.Init()

	// --- infrastructure ---
	store, err := metadata.NewMetadataStore(cfg.dbPath)
	if err != nil {
		slog.Error("open metadata store", "path", cfg.dbPath, "err", err)
		os.Exit(1)
	}
	defer store.Close()

	ring := placement.NewRing()
	pool := workerclient.New()
	table := health.NewLeaseTable()
	mon := health.NewMonitor(table)

	// Re-populate ring and lease table from bbolt so uploads work immediately after a controller restart.
	// Workers that are actually offline will be marked OFFLINE by the health monitor ~6 s later.
	if knownWorkers, err := store.ListWorkers(context.Background()); err != nil {
		slog.Warn("could not pre-populate ring from bbolt", "err", err)
	} else {
		for _, w := range knownWorkers {
			addr := fmt.Sprintf("%s:%d", w.Address, w.Port)
			ring.AddWorker(w.WorkerID, addr)
			table.RecordHeartbeat(w.WorkerID)
			slog.Info("ring pre-populated from bbolt", "worker_id", w.WorkerID, "addr", addr)
		}
	}

	// --- background goroutines ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)

	// When the monitor declares a worker OFFLINE, remove it from the ring
	// so no new chunks are placed on it.
	go watchOffline(ctx, table, ring)

	// --- HTTP server ---
	srv := api.New(cfg.addr, store, ring, pool, table)

	slog.Info("controller starting", "addr", cfg.addr, "db", cfg.dbPath)

	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("server error", "err", err)
			cancel()
		}
	}()

	// --- graceful shutdown on SIGINT / SIGTERM ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		slog.Info("shutdown signal received", "signal", sig)
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	slog.Info("controller stopped")
}

// watchOffline listens for OFFLINE events and removes the worker from the ring.
func watchOffline(ctx context.Context, table *health.LeaseTable, ring *placement.Ring) {
	ch := table.Subscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			slog.Warn("removing offline worker from ring", "worker_id", event.WorkerID)
			ring.RemoveWorker(event.WorkerID)
		}
	}
}
