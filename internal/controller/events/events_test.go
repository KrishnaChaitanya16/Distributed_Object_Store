package events

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitAssignsIncreasingSequenceNumbers(t *testing.T) {
	b := New(10)
	assert.Equal(t, uint64(0), b.LastSeq())
	a := b.Emit("a", "first")
	c := b.Emit("b", "second")
	assert.Equal(t, uint64(1), a.Seq)
	assert.Equal(t, uint64(2), c.Seq)
	assert.Equal(t, uint64(2), b.LastSeq())
	assert.WithinDuration(t, time.Now(), a.Time, 2*time.Second)
}

func TestFieldsFromKeyValuePairs(t *testing.T) {
	b := New(10)
	ev := b.Emit("k", "m", "chunk", "abc", "n", 3, "ok", true)
	assert.Equal(t, map[string]any{"chunk": "abc", "n": 3, "ok": true}, ev.Fields)

	assert.Nil(t, b.Emit("k", "no fields").Fields)
	assert.Equal(t, map[string]any{"a": nil}, b.Emit("k", "m", "a").Fields, "a dangling key is kept with a nil value")
	assert.Equal(t, map[string]any{"b": 2}, b.Emit("k", "m", 7, "ignored", "b", 2).Fields, "a non-string key skips its pair")
}

func TestSinceReturnsOnlyNewerEventsOldestFirst(t *testing.T) {
	b := New(10)
	for i := 0; i < 5; i++ {
		b.Emit("k", fmt.Sprint(i))
	}
	got := b.Since(2, 0)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"2", "3", "4"}, []string{got[0].Msg, got[1].Msg, got[2].Msg})
	assert.Empty(t, b.Since(5, 0))
	assert.Empty(t, b.Since(99, 0))
	assert.Len(t, b.Since(0, 0), 5)
}

func TestSinceLimitKeepsTheNewest(t *testing.T) {
	b := New(10)
	for i := 0; i < 6; i++ {
		b.Emit("k", fmt.Sprint(i))
	}
	got := b.Since(0, 2)
	require.Len(t, got, 2)
	assert.Equal(t, "4", got[0].Msg)
	assert.Equal(t, "5", got[1].Msg)
}

func TestOldestEventsAreDroppedWhenFull(t *testing.T) {
	b := New(3)
	for i := 1; i <= 7; i++ {
		b.Emit("k", fmt.Sprint(i))
	}
	got := b.Since(0, 0)
	require.Len(t, got, 3)
	assert.Equal(t, []uint64{5, 6, 7}, []uint64{got[0].Seq, got[1].Seq, got[2].Seq})
	assert.Equal(t, uint64(7), b.LastSeq())
	assert.Equal(t, 3, len(b.Since(4, 0)), "events 5..7 are still there")
}

func TestNonPositiveCapacityGetsADefault(t *testing.T) {
	for _, c := range []int{0, -5} {
		b := New(c)
		for i := 0; i < 1500; i++ {
			b.Emit("k", "x")
		}
		assert.Len(t, b.Since(0, 0), 1000)
	}
}

func TestCountsSurviveDroppedEvents(t *testing.T) {
	b := New(2)
	for i := 0; i < 5; i++ {
		b.Emit("a", "")
	}
	b.Emit("b", "")
	c := b.Counts()
	assert.Equal(t, uint64(5), c["a"])
	assert.Equal(t, uint64(1), c["b"])
	c["a"] = 0
	assert.Equal(t, uint64(5), b.Counts()["a"], "Counts returns a copy")
}

func TestSubscribeSignalsAndCoalesces(t *testing.T) {
	b := New(10)
	ch, cancel := b.Subscribe()
	defer cancel()
	select {
	case <-ch:
		t.Fatal("signalled before any event")
	default:
	}
	b.Emit("k", "1")
	b.Emit("k", "2")
	b.Emit("k", "3")
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("no signal after events")
	}
	select {
	case <-ch:
		t.Fatal("signals must coalesce into one")
	default:
	}
	assert.Len(t, b.Since(0, 0), 3, "no event is lost even though the signal coalesced")
}

func TestUnsubscribeStopsSignalsAndIsIdempotent(t *testing.T) {
	b := New(10)
	ch, cancel := b.Subscribe()
	cancel()
	cancel()
	b.Emit("k", "x")
	select {
	case <-ch:
		t.Fatal("signalled after unsubscribe")
	default:
	}
}

func TestEveryEventReachesEverySubscriberViaSince(t *testing.T) {
	b := New(200)
	const subs, events = 5, 100
	var wg sync.WaitGroup
	seen := make([]int, subs)
	for s := 0; s < subs; s++ {
		ch, cancel := b.Subscribe()
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			defer cancel()
			var last uint64
			deadline := time.After(5 * time.Second)
			for seen[s] < events {
				select {
				case <-ch:
					for _, ev := range b.Since(last, 0) {
						last = ev.Seq
						seen[s]++
					}
				case <-deadline:
					return
				}
			}
		}(s)
	}
	for i := 0; i < events; i++ {
		b.Emit("k", "x")
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	wg.Wait()
	for s := range seen {
		assert.Equal(t, events, seen[s], "subscriber %d", s)
	}
}

func TestConcurrentEmitIsRaceFree(t *testing.T) {
	b := New(50)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				b.Emit("k", "x", "i", i)
				b.Since(0, 5)
				b.Counts()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, uint64(1600), b.LastSeq())
	assert.Equal(t, uint64(1600), b.Counts()["k"])
	got := b.Since(0, 0)
	require.Len(t, got, 50)
	for i := 1; i < len(got); i++ {
		assert.Equal(t, got[i-1].Seq+1, got[i].Seq, "sequence numbers stay contiguous")
	}
}

func TestJSONShape(t *testing.T) {
	b := New(5)
	ev := b.Emit("upload.chunk", "chunk hashed", "index", 0)
	data, err := json.Marshal(ev)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Equal(t, "upload.chunk", m["kind"])
	assert.Equal(t, "chunk hashed", m["msg"])
	assert.EqualValues(t, 1, m["seq"])
	assert.NotEmpty(t, m["time"])
	assert.Equal(t, map[string]any{"index": float64(0)}, m["fields"])

	data, _ = json.Marshal(b.Emit("k", "bare"))
	assert.NotContains(t, string(data), "fields", "no fields are omitted from the JSON")
}

func TestPackageLevelEmitUsesDefault(t *testing.T) {
	before := Default.LastSeq()
	Emit("pkg.test", "hello", "a", 1)
	assert.Equal(t, before+1, Default.LastSeq())
}
