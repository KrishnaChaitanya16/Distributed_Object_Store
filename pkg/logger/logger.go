package logger

import (
	"log/slog"
	"os"
)

// Init sets up the default structured JSON logger.
// Set LOG_LEVEL=debug to see per-chunk trace logs.
func Init() {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))
}

// WithRequestID returns a logger pre-loaded with a request_id attribute.
// Use this in HTTP handlers and pass the logger down to service calls.
//
// Usage:
//
//	log := logger.WithRequestID(requestID)
//	log.Info("upload started", "object_id", objectID, "size_bytes", size)
func WithRequestID(requestID string) *slog.Logger {
	return slog.Default().With("request_id", requestID)
}

// WithWorkerID returns a logger pre-loaded with a worker_id attribute.
// Use this in worker API handlers and background goroutines.
func WithWorkerID(workerID string) *slog.Logger {
	return slog.Default().With("worker_id", workerID)
}
