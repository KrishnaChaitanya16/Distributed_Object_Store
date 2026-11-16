package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func quarantineDir(chunkDir string) string {
	return filepath.Join(filepath.Dir(chunkDir), "quarantine")
}

func quarantined(t *testing.T, chunkDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(quarantineDir(chunkDir))
	if err != nil {
		t.Fatalf("read quarantine dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func TestNewCreatesQuarantineDir(t *testing.T) {
	_, dir := newStore(t)
	st, err := os.Stat(quarantineDir(dir))
	if err != nil || !st.IsDir() {
		t.Fatalf("quarantine dir missing: %v", err)
	}
}

func TestNewFailsWhenQuarantinePathIsAFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "quarantine"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root); err == nil {
		t.Fatal("New must fail when quarantine cannot be a directory")
	}
}

func TestNewKeepsExistingQuarantine(t *testing.T) {
	root := t.TempDir()
	if _, err := New(root); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(root, "quarantine", "evidence.bin")
	if err := os.WriteFile(old, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatal("restart must not wipe quarantined evidence")
	}
}

func TestQuarantineHealthyChunkIsLeftAlone(t *testing.T) {
	s, dir := newStore(t)
	for _, n := range []int{0, 1, 4096, MaxChunkSize} {
		data, id := chunk(t, n)
		put(t, s, data, id)
		moved, err := s.Quarantine(context.Background(), id)
		if err != nil || moved {
			t.Fatalf("%d bytes: moved=%v err=%v", n, moved, err)
		}
		if !bytes.Equal(readAll(t, s, id), data) {
			t.Fatalf("%d bytes: healthy chunk changed", n)
		}
	}
	if q := quarantined(t, dir); len(q) != 0 {
		t.Fatalf("healthy chunks were quarantined: %v", q)
	}
}

func TestQuarantineCorruptions(t *testing.T) {
	cases := []struct {
		name    string
		size    int
		corrupt func(t *testing.T, path string, dataLen int64)
	}{
		{"first data byte flipped", 4096, func(t *testing.T, p string, _ int64) { flipByte(t, p, 0) }},
		{"middle data byte flipped", MaxChunkSize, func(t *testing.T, p string, n int64) { flipByte(t, p, n/2) }},
		{"last data byte flipped", 4096, func(t *testing.T, p string, n int64) { flipByte(t, p, n-1) }},
		{"footer byte flipped", 4096, func(t *testing.T, p string, n int64) { flipByte(t, p, n+2) }},
		{"footer of empty chunk flipped", 0, func(t *testing.T, p string, _ int64) { flipByte(t, p, 0) }},
		{"truncated mid data", 4096, func(t *testing.T, p string, n int64) { os.Truncate(p, n/2) }},
		{"truncated inside footer", 4096, func(t *testing.T, p string, n int64) { os.Truncate(p, n+2) }},
		{"truncated below footer size", 4096, func(t *testing.T, p string, _ int64) { os.Truncate(p, 3) }},
		{"truncated to zero", 4096, func(t *testing.T, p string, _ int64) { os.Truncate(p, 0) }},
		{"bytes appended", 4096, func(t *testing.T, p string, _ int64) {
			f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
			f.Write([]byte("junk"))
			f.Close()
		}},
		{"whole file replaced by another chunk", 4096, func(t *testing.T, p string, _ int64) {
			other, _ := chunk(t, 4096)
			os.WriteFile(p, append(other, 0, 0, 0, 0), 0o644)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, dir := newStore(t)
			data, id := chunk(t, c.size)
			put(t, s, data, id)
			path := filepath.Join(dir, id+chunkExt)
			raw, _ := os.ReadFile(path)
			c.corrupt(t, path, int64(c.size))
			corrupted, _ := os.ReadFile(path)
			if bytes.Equal(raw, corrupted) {
				t.Fatal("test did not change the file")
			}

			moved, err := s.Quarantine(context.Background(), id)
			if err != nil || !moved {
				t.Fatalf("moved=%v err=%v", moved, err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("corrupt file still in the serving directory")
			}
			q := quarantined(t, dir)
			if len(q) != 1 || !strings.HasPrefix(q[0], id+".") || !strings.HasSuffix(q[0], chunkExt) {
				t.Fatalf("quarantine contents %v", q)
			}
			kept, _ := os.ReadFile(filepath.Join(quarantineDir(dir), q[0]))
			if !bytes.Equal(kept, corrupted) {
				t.Fatal("quarantined copy must be byte-for-byte the corrupt file")
			}
		})
	}
}

func TestQuarantinedChunkIsInvisible(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	data, id := chunk(t, 2048)
	put(t, s, data, id)
	keep, keepID := chunk(t, 100)
	put(t, s, keep, keepID)
	flipByte(t, filepath.Join(dir, id+chunkExt), 10)
	if moved, err := s.Quarantine(ctx, id); !moved || err != nil {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
	if _, err := s.Read(ctx, id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("Read after quarantine: %v", err)
	}
	if ok, err := s.Exists(ctx, id); ok || err != nil {
		t.Fatalf("Exists after quarantine: %v %v", ok, err)
	}
	if _, err := s.VerifyCRC(ctx, id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("VerifyCRC after quarantine: %v", err)
	}
	ids, err := s.List(ctx)
	if err != nil || len(ids) != 1 || ids[0] != keepID {
		t.Fatalf("List after quarantine: %v %v", ids, err)
	}
	if err := s.Delete(ctx, id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("Delete after quarantine: %v", err)
	}
	if len(quarantined(t, dir)) != 1 {
		t.Fatal("Delete must not touch quarantined evidence")
	}
}

func TestRewriteAfterQuarantineRestoresChunk(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	data, id := chunk(t, MaxChunkSize)
	put(t, s, data, id)
	flipByte(t, filepath.Join(dir, id+chunkExt), 99)
	s.Quarantine(ctx, id)
	put(t, s, data, id)
	if !bytes.Equal(readAll(t, s, id), data) {
		t.Fatal("restored chunk differs")
	}
	if ok, err := s.VerifyCRC(ctx, id); !ok || err != nil {
		t.Fatalf("restored chunk fails CRC: %v %v", ok, err)
	}
}

func TestWriteOverCorruptCopyRepairsInPlace(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	data, id := chunk(t, 4096)
	put(t, s, data, id)
	flipByte(t, filepath.Join(dir, id+chunkExt), 5)
	put(t, s, data, id)
	if moved, err := s.Quarantine(ctx, id); moved || err != nil {
		t.Fatalf("repaired chunk was quarantined: moved=%v err=%v", moved, err)
	}
}

func TestRepeatedQuarantineKeepsEveryCopy(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	data, id := chunk(t, 512)
	for i := 0; i < 5; i++ {
		put(t, s, data, id)
		flipByte(t, filepath.Join(dir, id+chunkExt), int64(i))
		if moved, err := s.Quarantine(ctx, id); !moved || err != nil {
			t.Fatalf("round %d: moved=%v err=%v", i, moved, err)
		}
	}
	if q := quarantined(t, dir); len(q) != 5 {
		t.Fatalf("expected 5 distinct quarantined copies, got %v", q)
	}
}

func TestQuarantineNameCollisionGetsSuffix(t *testing.T) {
	s, _ := newStore(t)
	_, id := chunk(t, 1)
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		p, err := s.quarantinePath(id)
		if err != nil {
			t.Fatal(err)
		}
		if seen[p] {
			t.Fatalf("duplicate quarantine path %s", p)
		}
		seen[p] = true
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQuarantineMissingChunk(t *testing.T) {
	s, _ := newStore(t)
	_, id := chunk(t, 10)
	if moved, err := s.Quarantine(context.Background(), id); moved || !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
}

func TestQuarantineInvalidIDs(t *testing.T) {
	s, _ := newStore(t)
	for _, id := range []string{"", "abc", strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("a", 65), "../" + strings.Repeat("a", 61)} {
		if _, err := s.Quarantine(context.Background(), id); !errors.Is(err, ErrInvalidChunkID) {
			t.Fatalf("%q: %v", id, err)
		}
	}
}

func TestQuarantineCancelledContext(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 100)
	put(t, s, data, id)
	flipByte(t, filepath.Join(dir, id+chunkExt), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if moved, err := s.Quarantine(ctx, id); moved || !errors.Is(err, context.Canceled) {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
	if _, err := os.Stat(filepath.Join(dir, id+chunkExt)); err != nil {
		t.Fatal("a cancelled quarantine must leave the chunk where it was")
	}
}

func TestQuarantineDirectoryInsteadOfChunk(t *testing.T) {
	s, dir := newStore(t)
	_, id := chunk(t, 10)
	if err := os.Mkdir(filepath.Join(dir, id+chunkExt), 0o755); err != nil {
		t.Fatal(err)
	}
	if moved, err := s.Quarantine(context.Background(), id); moved || !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
}

func TestQuarantineFailsSafelyWhenQuarantineDirIsGone(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 100)
	put(t, s, data, id)
	path := filepath.Join(dir, id+chunkExt)
	flipByte(t, path, 1)
	q := quarantineDir(dir)
	if err := os.RemoveAll(q); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q, []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	moved, err := s.Quarantine(context.Background(), id)
	if moved || err == nil {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("a failed quarantine must never lose the chunk file")
	}
}

func TestQuarantineReadOnlyQuarantineDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	s, dir := newStore(t)
	data, id := chunk(t, 100)
	put(t, s, data, id)
	path := filepath.Join(dir, id+chunkExt)
	flipByte(t, path, 1)
	q := quarantineDir(dir)
	os.Chmod(q, 0o555)
	defer os.Chmod(q, 0o755)
	if moved, err := s.Quarantine(context.Background(), id); moved || err == nil {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("chunk must stay in place")
	}
}

func TestQuarantineUnreadableChunkIsNotMoved(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	s, dir := newStore(t)
	data, id := chunk(t, 100)
	put(t, s, data, id)
	path := filepath.Join(dir, id+chunkExt)
	os.Chmod(path, 0o000)
	defer os.Chmod(path, 0o644)
	if moved, err := s.Quarantine(context.Background(), id); moved || err == nil {
		t.Fatalf("an I/O error is not proof of corruption: moved=%v err=%v", moved, err)
	}
	os.Chmod(path, 0o644)
	if _, err := os.Stat(path); err != nil {
		t.Fatal("chunk must stay in place")
	}
}

// TestQuarantineRacingRepairNeverLosesGoodCopy runs a repair Write against Quarantine on the same chunk; the good copy must always survive.
func TestQuarantineRacingRepairNeverLosesGoodCopy(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	data, id := chunk(t, 64<<10)
	path := filepath.Join(dir, id+chunkExt)
	for i := 0; i < 200; i++ {
		put(t, s, data, id)
		flipByte(t, path, int64(i))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := s.Write(ctx, id, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Errorf("repair write: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := s.Quarantine(ctx, id); err != nil && !errors.Is(err, models.ErrNotFound) {
				t.Errorf("quarantine: %v", err)
			}
		}()
		wg.Wait()
		if ok, err := s.VerifyCRC(ctx, id); !ok || err != nil {
			t.Fatalf("round %d: good copy lost after racing repair: ok=%v err=%v", i, ok, err)
		}
	}
	for _, name := range quarantined(t, dir) {
		b, _ := os.ReadFile(filepath.Join(quarantineDir(dir), name))
		if crcOK(b) {
			t.Fatalf("a healthy copy %s ended up in quarantine", name)
		}
	}
}

// TestQuarantineBlocksRepairUntilMoved starts a repair inside the verify-to-rename window; the lock must hold it until the corrupt copy is moved.
func TestQuarantineBlocksRepairUntilMoved(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	data, id := chunk(t, 4096)
	put(t, s, data, id)
	flipByte(t, filepath.Join(dir, id+chunkExt), 7)
	repaired := make(chan error, 1)
	finishedInsideWindow := false
	s.beforeMove = func() {
		go func() { repaired <- s.Write(ctx, id, bytes.NewReader(data), int64(len(data))) }()
		select {
		case err := <-repaired:
			finishedInsideWindow = true
			repaired <- err
		case <-time.After(200 * time.Millisecond):
		}
	}
	moved, err := s.Quarantine(ctx, id)
	if err != nil || !moved {
		t.Fatalf("moved=%v err=%v", moved, err)
	}
	if err := <-repaired; err != nil {
		t.Fatalf("repair write: %v", err)
	}
	if finishedInsideWindow {
		t.Fatal("repair rename ran while Quarantine held the chunk; the lock is not protecting the window")
	}
	if ok, err := s.VerifyCRC(ctx, id); !ok || err != nil {
		t.Fatalf("repaired copy lost: ok=%v err=%v", ok, err)
	}
	q := quarantined(t, dir)
	if len(q) != 1 {
		t.Fatalf("quarantine %v", q)
	}
	if b, _ := os.ReadFile(filepath.Join(quarantineDir(dir), q[0])); crcOK(b) {
		t.Fatal("the healthy repair was quarantined instead of the corrupt copy")
	}
}

func crcOK(file []byte) bool {
	n := len(file) - footerSize
	if n < 0 {
		return false
	}
	return crc32.Checksum(file[:n], castagnoli) == binary.BigEndian.Uint32(file[n:])
}

func TestQuarantineConcurrentDifferentChunks(t *testing.T) {
	ctx := context.Background()
	s, dir := newStore(t)
	const n = 64
	ids := make([]string, n)
	for i := range ids {
		data, id := chunk(t, 1000)
		put(t, s, data, id)
		ids[i] = id
		if i%2 == 0 {
			flipByte(t, filepath.Join(dir, id+chunkExt), 3)
		}
	}
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			moved, err := s.Quarantine(ctx, id)
			if err != nil || moved != (i%2 == 0) {
				t.Errorf("chunk %d: moved=%v err=%v", i, moved, err)
			}
		}(i, id)
	}
	wg.Wait()
	if got := len(quarantined(t, dir)); got != n/2 {
		t.Fatalf("quarantined %d, want %d", got, n/2)
	}
	left, _ := s.List(ctx)
	if len(left) != n/2 {
		t.Fatalf("%d chunks left, want %d", len(left), n/2)
	}
}

func TestLockStripeCoversEveryHexPrefix(t *testing.T) {
	seen := map[int]bool{}
	hex := "0123456789abcdef"
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			k := hexByte(string([]byte{hex[i], hex[j]}) + strings.Repeat("0", 62))
			if k < 0 || k >= lockStripes {
				t.Fatalf("stripe %d out of range", k)
			}
			seen[k] = true
		}
	}
	if len(seen) != lockStripes {
		t.Fatalf("only %d of %d stripes used", len(seen), lockStripes)
	}
}
