package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
)

const (
	defaultEventLimit = 200
	maxEventLimit     = 2000
)

// streamPing is how often an idle event stream sends a comment so proxies keep the connection open; tests shorten it.
var streamPing = 15 * time.Second

// handleEvents returns the events newer than ?since, newest limit of them, as JSON.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	since, err := parseSeq(r.URL.Query().Get("since"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "since must be a non-negative integer")
		return
	}
	limit := defaultEventLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
	}
	if limit > maxEventLimit {
		limit = maxEventLimit
	}
	evs := s.d.Bus.Since(since, limit)
	if evs == nil {
		evs = []events.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs, "last_seq": s.d.Bus.LastSeq()})
}

// handleStream pushes events to the browser as they happen using Server-Sent Events; ?since or Last-Event-ID resume where a dropped connection left off.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	since := s.d.Bus.LastSeq()
	if v := r.URL.Query().Get("since"); v != "" {
		n, err := parseSeq(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be a non-negative integer")
			return
		}
		since = n
	} else if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := parseSeq(v); err == nil {
			since = n
		}
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "retry: 1500\n\n")
	if err := rc.Flush(); err != nil {
		return
	}

	wake, cancel := s.d.Bus.Subscribe()
	defer cancel()
	ping := time.NewTicker(streamPing)
	defer ping.Stop()
	for {
		for _, ev := range s.d.Bus.Since(since, 0) {
			data, err := json.Marshal(ev)
			if err != nil {
				since = ev.Seq
				continue
			}
			if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.Seq, data); err != nil {
				return
			}
			since = ev.Seq
		}
		if err := rc.Flush(); err != nil {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-wake:
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
		}
	}
}

func parseSeq(v string) (uint64, error) {
	if v == "" {
		return 0, nil
	}
	return strconv.ParseUint(v, 10, 64)
}
