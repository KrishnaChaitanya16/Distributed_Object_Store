// Package events is an in-memory, bounded log of what the controller is doing, fed to the web UI.
package events

import (
	"sync"
	"time"
)

// Event is one thing that happened; Fields carry the details for its Kind.
type Event struct {
	Seq    uint64         `json:"seq"`
	Time   time.Time      `json:"time"`
	Kind   string         `json:"kind"`
	Msg    string         `json:"msg"`
	Fields map[string]any `json:"fields,omitempty"`
}

// Bus keeps the most recent events and wakes subscribers when a new one arrives.
type Bus struct {
	mu     sync.Mutex
	buf    []Event
	first  int
	size   int
	next   uint64
	counts map[string]uint64
	subs   map[int]chan struct{}
	subSeq int
}

// New returns a Bus that remembers the last capacity events; a non-positive capacity means 1000.
func New(capacity int) *Bus {
	if capacity <= 0 {
		capacity = 1000
	}
	return &Bus{buf: make([]Event, capacity), next: 1, counts: make(map[string]uint64), subs: make(map[int]chan struct{})}
}

// Default is the process-wide bus the controller packages emit to.
var Default = New(5000)

// Emit records an event on the Default bus; kv is alternating string keys and values.
func Emit(kind, msg string, kv ...any) Event { return Default.Emit(kind, msg, kv...) }

// Emit records an event; kv is alternating string keys and values, and a dangling key is stored as nil.
func (b *Bus) Emit(kind, msg string, kv ...any) Event {
	var fields map[string]any
	if len(kv) > 0 {
		fields = make(map[string]any, (len(kv)+1)/2)
		for i := 0; i < len(kv); i += 2 {
			key, ok := kv[i].(string)
			if !ok {
				continue
			}
			if i+1 < len(kv) {
				fields[key] = kv[i+1]
			} else {
				fields[key] = nil
			}
		}
	}
	b.mu.Lock()
	ev := Event{Seq: b.next, Time: time.Now().UTC(), Kind: kind, Msg: msg, Fields: fields}
	b.next++
	b.counts[kind]++
	if b.size < len(b.buf) {
		b.buf[(b.first+b.size)%len(b.buf)] = ev
		b.size++
	} else {
		b.buf[b.first] = ev
		b.first = (b.first + 1) % len(b.buf)
	}
	for _, ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	b.mu.Unlock()
	return ev
}

// Since returns the events newer than seq, oldest first; limit > 0 keeps only the newest limit of them.
func (b *Bus) Since(seq uint64, limit int) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Event
	for i := 0; i < b.size; i++ {
		ev := b.buf[(b.first+i)%len(b.buf)]
		if ev.Seq > seq {
			out = append(out, ev)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// LastSeq is the sequence number of the newest event, or 0 if there is none.
func (b *Bus) LastSeq() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.next - 1
}

// Counts returns how many events of each kind have been emitted since start, including ones already dropped from the log.
func (b *Bus) Counts() map[string]uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]uint64, len(b.counts))
	for k, v := range b.counts {
		out[k] = v
	}
	return out
}

// Subscribe returns a channel that receives a signal after new events arrive (signals coalesce, so read with Since) and a function that unsubscribes.
func (b *Bus) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	id := b.subSeq
	b.subSeq++
	b.subs[id] = ch
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, id)
		b.mu.Unlock()
	}
}
