package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/logger"
)

func main() {
	addr   := flag.String("addr", ":8080", "controller listen address")
	dbPath := flag.String("db", "data/meta.db", "bbolt database file path")
	flag.Parse()

	logger.Init()

	// --- infrastructure ---
	store, err := metadata.NewMetadataStore(*dbPath)
	if err != nil {
		slog.Error("open metadata store", "path", *dbPath, "err", err)
		os.Exit(1)
	}
	defer store.Close()

	ring  := placement.NewRing()
	pool  := workerclient.New()
	table := health.NewLeaseTable()
	mon   := health.NewMonitor(table)

	// --- background goroutines ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)

	// When the monitor declares a worker OFFLINE, remove it from the ring
	// so no new chunks are placed on it.
	go watchOffline(ctx, table, ring)

	// --- HTTP server ---
	srv := api.New(*addr, store, ring, pool, table)

	slog.Info("controller starting", "addr", *addr, "db", *dbPath)

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
