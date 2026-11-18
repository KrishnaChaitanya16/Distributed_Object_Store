package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/config"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/rebalancer"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/web"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/logger"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type controllerConfig struct {
	addr          string
	dbPath        string
	gcInterval    time.Duration
	stagedTimeout time.Duration
	orphanGrace   time.Duration
	webPort       int
	webDir        string
	demoMode      bool
	dockerSocket  string
}

func loadConfig(getenv config.Getenv) (controllerConfig, error) {
	port, err := config.Port(getenv, "CONTROLLER_PORT", 8080)
	if err != nil {
		return controllerConfig{}, err
	}
	dbPath := config.Str(getenv, "DB_PATH", "data/meta.db")
	gcInterval, err := config.PositiveInt(getenv, "GC_INTERVAL_S", 30)
	if err != nil {
		return controllerConfig{}, err
	}
	stagedTimeout, err := config.PositiveInt(getenv, "STAGED_TIMEOUT_MIN", 5)
	if err != nil {
		return controllerConfig{}, err
	}
	orphanGrace, err := config.NonNegativeInt(getenv, "ORPHAN_GRACE_S", 900)
	if err != nil {
		return controllerConfig{}, err
	}
	webPort, err := config.OptionalPort(getenv, "WEBUI_PORT", 8081)
	if err != nil {
		return controllerConfig{}, err
	}
	demoMode, err := config.Bool(getenv, "DEMO_MODE", false)
	if err != nil {
		return controllerConfig{}, err
	}
	return controllerConfig{
		webPort:       webPort,
		webDir:        config.Str(getenv, "WEBUI_DIR", ""),
		demoMode:      demoMode,
		dockerSocket:  config.Str(getenv, "DOCKER_SOCKET", "/var/run/docker.sock"),
		addr:          fmt.Sprintf(":%d", port),
		dbPath:        dbPath,
		gcInterval:    time.Duration(gcInterval) * time.Second,
		stagedTimeout: time.Duration(stagedTimeout) * time.Minute,
		orphanGrace:   time.Duration(orphanGrace) * time.Second,
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
			// Skip workers that were persisted as OFFLINE — they never sent a
			// heartbeat before the last shutdown and should not pollute the ring.
			if w.Status == models.WorkerOffline {
				slog.Info("skipping offline worker during ring pre-population", "worker_id", w.WorkerID)
				continue
			}
			addr := fmt.Sprintf("%s:%d", w.Address, w.Port)
			ring.AddWorker(w.WorkerID, addr)
			table.RecordHeartbeat(w.WorkerID)
			slog.Info("ring pre-populated from bbolt", "worker_id", w.WorkerID, "addr", addr)
		}
		metrics.WorkersOnline.Set(float64(len(ring.Workers())))
	}

	// --- background goroutines ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mon.Start(ctx)

	// Background rebalancer: every 30 s restore RF=2 for chunks whose replica
	// is on an offline worker. Replaces the dead slot with a healthy worker so
	// the replica count stays at exactly 2 (no 3-replica accumulation).
	rb := rebalancer.New(store, pool, table, ring)
	go rb.Run(ctx)

	// When the monitor declares a worker OFFLINE, remove it from the ring
	// and immediately trigger a repair tick so RF=2 is restored in ~seconds
	// rather than waiting up to 30 s for the next periodic tick.
	go watchOffline(ctx, table, ring, rb, store)

	// Garbage collection and replica reconciliation: periodic, on demand, and whenever a worker joins or rejoins.
	recon, err := gc.NewReconciler(store, pool, table, gc.NewCollector(store, pool, table), gc.Config{
		Interval:      cfg.gcInterval,
		StagedTimeout: cfg.stagedTimeout,
		OrphanGrace:   cfg.orphanGrace,
	})
	if err != nil {
		slog.Error("reconciler config", "err", err)
		os.Exit(1)
	}
	reconDone := make(chan struct{})
	go func() {
		defer close(reconDone)
		recon.Run(ctx) //nolint:errcheck
	}()

	// --- HTTP server ---
	srv := api.New(cfg.addr, store, ring, pool, table).WithReconciler(recon)
	srv.PopulateBloom(context.Background())
	slog.Info("bloom filter pre-populated from bbolt")

	slog.Info("controller starting", "addr", cfg.addr, "db", cfg.dbPath, "gc_interval", cfg.gcInterval, "staged_timeout", cfg.stagedTimeout, "orphan_grace", cfg.orphanGrace)

	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("server error", "err", err)
			cancel()
		}
	}()

	// Demo UI: dashboard, live event stream and the REST API on one origin; off when WEBUI_PORT=0.
	var uiHTTP *http.Server
	if cfg.webPort > 0 {
		var demo *web.Docker
		if cfg.demoMode {
			demo = web.NewDocker(cfg.dockerSocket)
			slog.Warn("DEMO_MODE is on: the UI can stop containers and damage chunks through the Docker socket", "socket", cfg.dockerSocket)
		}
		ui := web.New(web.Deps{
			StaticDir: cfg.webDir,
			Meta:      store, Workers: pool, Ring: ring, Health: table, Bloom: srv.Bloom(), Recon: recon, API: srv.Handler(), Demo: demo,
			Config: web.Config{
				ChunkSize: api.ChunkSize, ReplicationLevel: 2, HedgeMs: srv.HedgeMs(),
				LeaseTimeoutS: int(health.LeaseTimeout.Seconds()), HeartbeatS: int(health.HeartbeatInterval.Seconds()),
				GCIntervalS: int(cfg.gcInterval.Seconds()), StagedTimeoutMin: int(cfg.stagedTimeout.Minutes()), OrphanGraceS: int(cfg.orphanGrace.Seconds()),
			},
		})
		go ui.Run(ctx)
		uiHTTP = &http.Server{Addr: fmt.Sprintf(":%d", cfg.webPort), Handler: ui.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
		go func() {
			slog.Info("web UI listening", "addr", uiHTTP.Addr, "demo_mode", cfg.demoMode, "static_dir", cfg.webDir)
			if err := uiHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("web UI error", "err", err)
			}
		}()
	}

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

	if uiHTTP != nil {
		uiHTTP.Shutdown(shutdownCtx) //nolint:errcheck
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	cancel()
	select {
	case <-reconDone:
	case <-shutdownCtx.Done():
	}
	slog.Info("controller stopped")
}

// watchOffline listens for OFFLINE events, removes the worker from the ring,
// persists offline status to bbolt (so the worker isn't re-added on restart),
// and triggers an immediate rebalancer tick.
func watchOffline(ctx context.Context, table *health.LeaseTable, ring *placement.Ring, rb *rebalancer.Rebalancer, store *metadata.MetadataStore) {
	ch := table.Subscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			slog.Warn("removing offline worker from ring", "worker_id", event.WorkerID)
			ring.RemoveWorker(event.WorkerID)
			metrics.WorkersOnline.Set(float64(len(ring.Workers())))
			// Persist OFFLINE so the worker isn't re-added to the ring if the
			// controller restarts while the worker is still down.
			if err := store.UpdateWorkerStatus(ctx, event.WorkerID, models.WorkerOffline); err != nil {
				slog.Warn("failed to persist worker offline status", "worker_id", event.WorkerID, "err", err)
			}
			rb.TriggerNow(ctx) // restore RF=2 immediately, don't wait for 30s tick
		}
	}
}
