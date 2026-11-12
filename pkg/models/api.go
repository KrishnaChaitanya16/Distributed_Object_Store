package models

// RegisterRequest is the body of POST /internal/workers/register.
type RegisterRequest struct {
	WorkerID      string `json:"worker_id"`
	Address       string `json:"address"`
	Port          int    `json:"port"`
	CapacityBytes int64  `json:"capacity_bytes"`
}

// RegisterResponse is returned by the controller on successful registration.
type RegisterResponse struct {
	Status     string `json:"status"`
	VnodeCount int    `json:"vnode_count"`
}

// HeartbeatRequest is the body of POST /internal/workers/heartbeat.
type HeartbeatRequest struct {
	WorkerID string `json:"worker_id"`
}

// ReplicatePeerRequest asks a worker to stream chunk_id directly to target_addr ("host:port").
type ReplicatePeerRequest struct {
	ChunkID    string `json:"chunk_id"`
	TargetAddr string `json:"target_addr"`
}

// InventoryResponse is the body of GET /internal/workers/{id}/chunks.
type InventoryResponse struct {
	WorkerID string   `json:"worker_id"`
	ChunkIDs []string `json:"chunk_ids"`
	Count    int      `json:"count"`
}

// ErrorResponse is the JSON body of every non-2xx response.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}
