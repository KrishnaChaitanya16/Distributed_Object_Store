package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/config"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/registration"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/logger"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type workerConfig struct {
	workerID          string
	address           string
	port              int
	dataDir           string
	controllerAddr    string
	heartbeatInterval time.Duration
	scrubInterval     time.Duration
	scrubPause        time.Duration
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		slog.Error("worker config", "err", err)
		os.Exit(1)
	}

	logger.Init()

	slog.Info("worker starting", "worker_id", cfg.workerID, "port", cfg.port, "data_dir", cfg.dataDir, "controller", cfg.controllerAddr, "scrub_interval", cfg.scrubInterval)

	store, err := storage.New(cfg.dataDir)
	if err != nil {
		slog.Error("open chunk store", "err", err)
		os.Exit(1)
	}
	scrub, err := scrubber.New(store, scrubber.Config{Interval: cfg.scrubInterval, Pause: cfg.scrubPause})
	if err != nil {
		slog.Error("scrubber init", "err", err)
		os.Exit(1)
	}
	selfAddr := net.JoinHostPort(cfg.address, strconv.Itoa(cfg.port))
	peer := &http.Client{Timeout: 30 * time.Second}
	srv := newHTTPServer(cfg.port, api.New(store, cfg.workerID, selfAddr, peer).WithScrubber(scrub).WithDiskStats(diskStats(cfg.dataDir)).Handler())
	// Listen before registering so the controller can reach us as soon as it knows about us.
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	scrubDone := make(chan struct{})
	go func() {
		defer close(scrubDone)
		scrub.Run(ctx)
	}()

	reg := registration.New(cfg.controllerAddr, models.RegisterRequest{
		WorkerID:      cfg.workerID,
		Address:       cfg.address,
		Port:          cfg.port,
		CapacityBytes: diskCapacity(cfg.dataDir),
	}, &http.Client{Timeout: 5 * time.Second})
	go func() {
		if _, err := reg.Register(ctx); err == nil {
			reg.Run(ctx, cfg.heartbeatInterval)
		}
	}()

	<-ctx.Done()
	slog.Info("worker shutting down", "worker_id", cfg.workerID)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	select {
	case <-scrubDone:
	case <-shutdownCtx.Done():
		slog.Warn("shutdown: scrubber did not stop in time")
	}
	slog.Info("worker stopped", "worker_id", cfg.workerID)
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
	hb, err := config.PositiveInt(getenv, "HEARTBEAT_INTERVAL_S", 2)
	if err != nil {
		return workerConfig{}, err
	}
	scrub, err := config.PositiveInt(getenv, "SCRUB_INTERVAL_S", 30)
	if err != nil {
		return workerConfig{}, err
	}
	pause, err := config.NonNegativeInt(getenv, "SCRUB_PAUSE_MS", 10)
	if err != nil {
		return workerConfig{}, err
	}
	return workerConfig{
		workerID:          id,
		address:           config.Str(getenv, "WORKER_ADDRESS", id),
		port:              port,
		dataDir:           config.Str(getenv, "DATA_DIR", "/data"),
		controllerAddr:    ctrl,
		heartbeatInterval: time.Duration(hb) * time.Second,
		scrubInterval:     time.Duration(scrub) * time.Second,
		scrubPause:        time.Duration(pause) * time.Millisecond,
	}, nil
}

// newHTTPServer bounds how long a stalled client can hold a connection; no WriteTimeout so slow chunk downloads still finish.
func newHTTPServer(port int, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// diskCapacity returns the total size of the filesystem holding dir, or 0 if it cannot be read.
// diskStats reports the free and total bytes of the filesystem holding dir.
func diskStats(dir string) func() (free, total int64) {
	return func() (int64, int64) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(dir, &st); err != nil {
			return 0, 0
		}
		return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize)
	}
}

func diskCapacity(dir string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0
	}
	return int64(st.Blocks) * int64(st.Bsize)
}
