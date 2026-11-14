package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// POST /internal/workers/register  — called by a worker node on startup to join the cluster.
func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req models.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.WorkerID == "" || req.Address == "" || req.Port == 0 {
		http.Error(w, "worker_id, address and port are required", http.StatusBadRequest)
		return
	}

	const vnodeCount = 50
	record := &models.WorkerRecord{
		WorkerID:      req.WorkerID,
		Address:       req.Address,
		Port:          req.Port,
		CapacityBytes: req.CapacityBytes,
		Status:        models.WorkerOnline,
		LastSeen:      time.Now().UTC(),
		VnodeCount:    vnodeCount,
	}
	if err := s.meta.SaveWorker(r.Context(), record); err != nil {
		slog.Error("save worker", "worker_id", req.WorkerID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	workerAddr := addrOf(record)
	s.placement.AddWorker(req.WorkerID, workerAddr)
	s.health.RecordHeartbeat(req.WorkerID)

	slog.Info("worker registered", "worker_id", req.WorkerID, "addr", workerAddr)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(models.RegisterResponse{ //nolint:errcheck
		Status:     "registered",
		VnodeCount: vnodeCount,
	})
}

// POST /internal/workers/heartbeat  — called by worker every 2 s to stay ONLINE.
// Body: {"worker_id": "w1"}
// Returns 404 if the worker is not registered — the worker will then re-register,
// which re-adds it to the placement ring after a controller restart.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkerID == "" {
		http.Error(w, "worker_id required", http.StatusBadRequest)
		return
	}
	if _, err := s.meta.GetWorker(r.Context(), req.WorkerID); err != nil {
		http.Error(w, "worker not registered", http.StatusNotFound)
		return
	}

	s.health.RecordHeartbeat(req.WorkerID)
	w.WriteHeader(http.StatusNoContent)
}

func addrOf(w *models.WorkerRecord) string {
	return w.Address + ":" + itoa(w.Port)
}
