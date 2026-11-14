package logger

import (
	"log/slog"
	"os"
)

// Init sets up the default structured JSON logger for the process.
// Call once from main() in both controller and worker binaries.
// Output goes to stdout — Docker and docker-compose capture it automatically.
func Init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
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
