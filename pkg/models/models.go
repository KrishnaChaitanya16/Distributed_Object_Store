package models

import "time"

// --- Object status ---

type ObjectStatus string

const (
	StatusStaged    ObjectStatus = "STAGED"
	StatusCommitted ObjectStatus = "COMMITTED"
	StatusDeleted   ObjectStatus = "DELETED"
	StatusFailed    ObjectStatus = "FAILED"
)

// --- Chunk state ---

type ChunkState string

const (
	ChunkPending       ChunkState = "PENDING"
	ChunkDurable       ChunkState = "DURABLE"
	ChunkPendingDelete ChunkState = "PENDING_DELETE"
)

// --- Worker status ---

type WorkerStatus string

const (
	WorkerUnknown WorkerStatus = "UNKNOWN"
	WorkerOnline  WorkerStatus = "ONLINE"
	WorkerOffline WorkerStatus = "OFFLINE"
)

// --- Structs ---

type ObjectManifest struct {
	ID             string            `json:"object_id"`
	Name           string            `json:"name"`
	SizeBytes      int64             `json:"size_bytes"`
	ContentType    string            `json:"content_type"`
	Status         ObjectStatus      `json:"status"`
	Checksum       string            `json:"checksum"`
	ChunkCount     int               `json:"chunk_count"`
	CustomMetadata map[string]string `json:"custom_metadata"`
	Version        int64             `json:"version"`
	CreatedAt      time.Time         `json:"created_at"`
	ModifiedAt     time.Time         `json:"modified_at"`
}

type ChunkRef struct {
	ChunkID     string `json:"chunk_id"`
	Index       int    `json:"index"`
	Offset      int64  `json:"offset"`
	Size        int    `json:"size"`
	PrimaryID   string `json:"primary_worker_id"`
	SecondaryID string `json:"secondary_worker_id"`
}

type Replica struct {
	WorkerID   string `json:"worker_id"`
	WorkerAddr string `json:"worker_addr"`
}

type ChunkRecord struct {
	ChunkID   string     `json:"chunk_id"`
	SizeBytes int        `json:"size_bytes"`
	RefCount  int        `json:"ref_count"`
	State     ChunkState `json:"state"`
	Replicas  []Replica  `json:"replicas"`
	CreatedAt time.Time  `json:"created_at"`
}

type WorkerRecord struct {
	WorkerID      string       `json:"worker_id"`
	Address       string       `json:"address"`
	Port          int          `json:"port"`
	CapacityBytes int64        `json:"capacity_bytes"`
	Status        WorkerStatus `json:"status"`
	LastSeen      time.Time    `json:"last_seen"`
	VnodeCount    int          `json:"vnode_count"`
}

type IdempotencyRecord struct {
	Key        string    `json:"key"`
	Status     string    `json:"status"`
	StatusCode int       `json:"status_code"`
	Body       []byte    `json:"body"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type WorkerOfflineEvent struct {
	WorkerID  string    `json:"worker_id"`
	Timestamp time.Time `json:"timestamp"`
}

type RepairTask struct {
	ChunkID        string    `json:"chunk_id"`
	SourceWorkerID string    `json:"source_worker_id"`
	TargetWorkerID string    `json:"target_worker_id"`
	ScheduledAt    time.Time `json:"scheduled_at"`
}
