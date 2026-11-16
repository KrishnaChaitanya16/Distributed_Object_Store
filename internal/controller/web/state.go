package web

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const (
	bloomCells        = 1024
	maxInventoryItems = 400
	maxObjects        = 100
)

type workerView struct {
	ID               string            `json:"id"`
	Addr             string            `json:"addr"`
	Health           string            `json:"health"`
	RegisteredStatus string            `json:"registered_status"`
	LastSeen         *time.Time        `json:"last_seen,omitempty"`
	HeartbeatAgeMs   int64             `json:"heartbeat_age_ms"`
	InRing           bool              `json:"in_ring"`
	InventoryOK      bool              `json:"inventory_ok"`
	InventoryError   string            `json:"inventory_error,omitempty"`
	ChunkCount       int               `json:"chunk_count"`
	Chunks           []string          `json:"chunks"`
	Scrubber         map[string]any    `json:"scrubber,omitempty"`
	Vnodes           []placement.Point `json:"vnodes"`
}

type chunkHealth struct {
	Total       int            `json:"total"`
	ByState     map[string]int `json:"by_state"`
	Healthy     int            `json:"healthy"`
	Degraded    int            `json:"degraded"`
	Unavailable int            `json:"unavailable"`
	ExtraCopies int            `json:"extra_copies"`
}

type clusterView struct {
	WorkersTotal     int            `json:"workers_total"`
	WorkersOnline    int            `json:"workers_online"`
	WorkersUnscanned int            `json:"workers_unscanned"`
	Objects          map[string]int `json:"objects"`
	Chunks           chunkHealth    `json:"chunks"`
}

type demoView struct {
	Enabled   bool `json:"enabled"`
	Available bool `json:"available"`
}

type stateView struct {
	Time          time.Time     `json:"time"`
	UptimeS       int64         `json:"uptime_s"`
	Cluster       clusterView   `json:"cluster"`
	Workers       []workerView  `json:"workers"`
	VnodesPerNode int           `json:"vnodes_per_worker"`
	Bloom         any           `json:"bloom"`
	GC            gc.Status     `json:"gc"`
	Config        Config        `json:"config"`
	Events        eventsSummary `json:"events"`
	Demo          demoView      `json:"demo"`
}

type eventsSummary struct {
	LastSeq uint64            `json:"last_seq"`
	Counts  map[string]uint64 `json:"counts"`
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	st, err := s.buildState(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) buildState(ctx context.Context) (*stateView, error) {
	records, err := s.d.Meta.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	objects, err := s.d.Meta.ListObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	chunks, err := s.d.Meta.ListAllChunks(ctx)
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}
	inv := s.inv.get(ctx)
	lease := s.d.Health.Snapshot()
	inRing := map[string]bool{}
	for _, id := range s.d.Ring.Workers() {
		inRing[id] = true
	}
	sort.Slice(records, func(i, j int) bool { return records[i].WorkerID < records[j].WorkerID })

	now := time.Now()
	st := &stateView{
		Time:          now.UTC(),
		UptimeS:       int64(now.Sub(s.d.Started).Seconds()),
		Workers:       make([]workerView, 0, len(records)),
		VnodesPerNode: placement.VnodesPerWorker(),
		Bloom:         s.d.Bloom.Stats(bloomCells),
		Config:        s.d.Config,
		Events:        eventsSummary{LastSeq: s.d.Bus.LastSeq(), Counts: s.d.Bus.Counts()},
		Demo:          s.demoStatus(ctx),
	}
	if s.d.Recon != nil {
		st.GC = s.d.Recon.Status()
	}
	for _, rec := range records {
		wv := workerView{
			ID:               rec.WorkerID,
			Addr:             fmt.Sprintf("%s:%d", rec.Address, rec.Port),
			Health:           string(models.WorkerUnknown),
			RegisteredStatus: string(rec.Status),
			HeartbeatAgeMs:   -1,
			InRing:           inRing[rec.WorkerID],
			Chunks:           []string{},
			Vnodes:           placement.VnodePoints(rec.WorkerID),
		}
		if l, ok := lease[rec.WorkerID]; ok {
			wv.Health = string(l.Status)
			if !l.LastSeen.IsZero() {
				seen := l.LastSeen.UTC()
				wv.LastSeen = &seen
				wv.HeartbeatAgeMs = now.Sub(l.LastSeen).Milliseconds()
			}
		}
		if wi, ok := inv.Workers[rec.WorkerID]; ok {
			wv.InventoryOK = wi.OK
			wv.InventoryError = wi.Error
			wv.Scrubber = wi.Scrubber
			wv.ChunkCount = len(wi.Chunks)
			if len(wi.Chunks) <= maxInventoryItems {
				for id := range wi.Chunks {
					wv.Chunks = append(wv.Chunks, id)
				}
				sort.Strings(wv.Chunks)
			}
		}
		st.Workers = append(st.Workers, wv)
		if wv.Health == string(models.WorkerOnline) {
			st.Cluster.WorkersOnline++
			if !wv.InventoryOK {
				st.Cluster.WorkersUnscanned++
			}
		}
	}
	st.Cluster.WorkersTotal = len(records)
	st.Cluster.Objects = map[string]int{}
	for _, o := range objects {
		st.Cluster.Objects[string(o.Status)]++
	}
	st.Cluster.Chunks = classifyChunks(chunks, inv)
	return st, nil
}

// classifyChunks counts DURABLE chunks by how many copies of them are really reachable right now, and extra copies on workers the record does not list.
func classifyChunks(chunks []*models.ChunkRecord, inv *inventory) chunkHealth {
	ch := chunkHealth{ByState: map[string]int{}}
	for _, c := range chunks {
		ch.Total++
		ch.ByState[string(c.State)]++
		if c.State != models.ChunkDurable {
			continue
		}
		listed := map[string]bool{}
		reachable := 0
		for _, rep := range c.Replicas {
			listed[rep.WorkerID] = true
			if wi, ok := inv.Workers[rep.WorkerID]; ok && wi.OK && wi.Chunks[c.ChunkID] {
				reachable++
			}
		}
		switch {
		case reachable >= 2:
			ch.Healthy++
		case reachable == 1:
			ch.Degraded++
		default:
			ch.Unavailable++
		}
		for id, wi := range inv.Workers {
			if wi.OK && wi.Chunks[c.ChunkID] && !listed[id] {
				ch.ExtraCopies++
			}
		}
	}
	return ch
}

type replicaView struct {
	Worker   string `json:"worker"`
	Online   bool   `json:"online"`
	Physical *bool  `json:"physical"`
}

type chunkView struct {
	Index     int           `json:"index"`
	ChunkID   string        `json:"chunk_id"`
	Size      int           `json:"size"`
	Offset    int64         `json:"offset"`
	State     string        `json:"state"`
	RefCount  int           `json:"ref_count"`
	RingPos   uint32        `json:"ring_pos"`
	Primary   string        `json:"primary"`
	Secondary string        `json:"secondary"`
	Replicas  []replicaView `json:"replicas"`
	Extra     []string      `json:"extra_copies"`
	Reachable int           `json:"reachable_copies"`
	Missing   bool          `json:"record_missing"`
}

type objectView struct {
	ID         string            `json:"object_id"`
	Name       string            `json:"name"`
	Size       int64             `json:"size_bytes"`
	Type       string            `json:"content_type"`
	Status     string            `json:"status"`
	Checksum   string            `json:"checksum"`
	ChunkCount int               `json:"chunk_count"`
	Version    int64             `json:"version"`
	Custom     map[string]string `json:"custom_metadata"`
	CreatedAt  time.Time         `json:"created_at"`
	Chunks     []chunkView       `json:"chunks"`
}

func (s *Server) handleObjects(w http.ResponseWriter, r *http.Request) {
	limit := maxObjects
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n < limit {
			limit = n
		}
	}
	objs, err := s.buildObjects(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": objs})
}

func (s *Server) buildObjects(ctx context.Context, limit int) ([]objectView, error) {
	objects, err := s.d.Meta.ListObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].CreatedAt.After(objects[j].CreatedAt) })
	if len(objects) > limit {
		objects = objects[:limit]
	}
	inv := s.inv.get(ctx)
	lease := s.d.Health.Snapshot()
	out := make([]objectView, 0, len(objects))
	for _, o := range objects {
		ov := objectView{ID: o.ID, Name: o.Name, Size: o.SizeBytes, Type: o.ContentType, Status: string(o.Status), Checksum: o.Checksum,
			ChunkCount: o.ChunkCount, Version: o.Version, Custom: o.CustomMetadata, CreatedAt: o.CreatedAt, Chunks: []chunkView{}}
		refs, err := s.d.Meta.GetChunkRefs(ctx, o.ID)
		if err != nil {
			return nil, fmt.Errorf("chunk refs of %s: %w", o.ID, err)
		}
		for _, ref := range refs {
			cv := chunkView{Index: ref.Index, ChunkID: ref.ChunkID, Size: ref.Size, Offset: ref.Offset, RingPos: placement.KeyHash(ref.ChunkID),
				Primary: ref.PrimaryID, Secondary: ref.SecondaryID, Replicas: []replicaView{}, Extra: []string{}}
			rec, err := s.d.Meta.GetChunk(ctx, ref.ChunkID)
			if err != nil {
				cv.Missing = true
				cv.State = "NO_RECORD"
				ov.Chunks = append(ov.Chunks, cv)
				continue
			}
			cv.State = string(rec.State)
			cv.RefCount = rec.RefCount
			listed := map[string]bool{}
			for _, rep := range rec.Replicas {
				listed[rep.WorkerID] = true
				rv := replicaView{Worker: rep.WorkerID, Online: lease[rep.WorkerID].Status == models.WorkerOnline}
				if wi, ok := inv.Workers[rep.WorkerID]; ok && wi.OK {
					has := wi.Chunks[ref.ChunkID]
					rv.Physical = &has
					if has {
						cv.Reachable++
					}
				}
				cv.Replicas = append(cv.Replicas, rv)
			}
			ids := make([]string, 0, len(inv.Workers))
			for id := range inv.Workers {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				if wi := inv.Workers[id]; wi.OK && wi.Chunks[ref.ChunkID] && !listed[id] {
					cv.Extra = append(cv.Extra, id)
				}
			}
			ov.Chunks = append(ov.Chunks, cv)
		}
		out = append(out, ov)
	}
	return out, nil
}

// handleLocate replays ring placement for a chunk id, or for any text hashed with SHA-256 the way a chunk would be.
func (s *Server) handleLocate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	chunk := q.Get("chunk")
	if text, ok := q["text"]; ok && chunk == "" {
		sum := sha256.Sum256([]byte(text[0]))
		chunk = hex.EncodeToString(sum[:])
	}
	if chunk == "" || len(chunk) > 128 {
		writeError(w, http.StatusBadRequest, "give chunk=<id> or text=<anything>")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"chunk_id": chunk, "walk": s.d.Ring.Locate(chunk)})
}

// handleProbe shows which Bloom filter bits a chunk id maps to, and what the authoritative store says about it.
func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	chunk := q.Get("chunk")
	if text, ok := q["text"]; ok && chunk == "" {
		sum := sha256.Sum256([]byte(text[0]))
		chunk = hex.EncodeToString(sum[:])
	}
	if chunk == "" || len(chunk) > 128 {
		writeError(w, http.StatusBadRequest, "give chunk=<id> or text=<anything>")
		return
	}
	_, err := s.d.Meta.GetChunk(r.Context(), chunk)
	writeJSON(w, http.StatusOK, map[string]any{"probe": s.d.Bloom.Probe(chunk, bloomCells), "in_store": err == nil})
}
