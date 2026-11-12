package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, filepath.Join(root, "chunks")
}

func chunk(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

func put(t *testing.T, s *Store, data []byte, id string) {
	t.Helper()
	if err := s.Write(context.Background(), id, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func readAll(t *testing.T, s *Store, id string) []byte {
	t.Helper()
	rc, err := s.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return b
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tmpExt) {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func assertDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected empty dir, found %d entries", len(entries))
	}
}

// ── New ──────────────────────────────────────────────────────────────────────

func TestNewCreatesChunkDir(t *testing.T) {
	_, dir := newStore(t)
	st, err := os.Stat(dir)
	if err != nil || !st.IsDir() {
		t.Fatalf("chunk dir not created: %v", err)
	}
}

func TestNewCreatesMissingParents(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b", "c")
	if _, err := New(root); err != nil {
		t.Fatalf("New with missing parents: %v", err)
	}
}

func TestNewOnExistingDirKeepsChunks(t *testing.T) {
	root := t.TempDir()
	s1, _ := New(root)
	data, id := chunk(t, 100)
	put(t, s1, data, id)
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, s2, id), data) {
		t.Fatal("chunk lost after reopening store")
	}
}

func TestNewFailsWhenDataDirIsAFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	os.WriteFile(f, []byte("x"), 0o644)
	if _, err := New(f); err == nil {
		t.Fatal("expected error when data dir is a regular file")
	}
}

func TestNewRemovesStaleTempFilesOnly(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	dir := filepath.Join(root, "chunks")
	data, id := chunk(t, 10)
	put(t, s, data, id)
	os.WriteFile(filepath.Join(dir, id+".123.tmp"), []byte("half"), 0o644)
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0o644)
	os.Mkdir(filepath.Join(dir, "sub.tmp"), 0o755)

	if _, err := New(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, id+".123.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale temp file not removed")
	}
	for _, keep := range []string{id + chunkExt, "notes.txt", "sub.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, keep)); err != nil {
			t.Fatalf("%s should be untouched: %v", keep, err)
		}
	}
}

// ── Write + Read round trip ──────────────────────────────────────────────────

func TestRoundTripSizes(t *testing.T) {
	for _, n := range []int{0, 1, 3, footerSize, 5, 4095, 4096, 4097, 65536, MaxChunkSize - 1, MaxChunkSize} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s, _ := newStore(t)
			data, id := chunk(t, n)
			put(t, s, data, id)
			if got := readAll(t, s, id); !bytes.Equal(got, data) {
				t.Fatalf("read %d bytes, want %d", len(got), len(data))
			}
			ok, err := s.VerifyCRC(context.Background(), id)
			if err != nil || !ok {
				t.Fatalf("VerifyCRC ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestOnDiskFormat(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 1000)
	put(t, s, data, id)
	raw, err := os.ReadFile(filepath.Join(dir, id+chunkExt))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != len(data)+footerSize {
		t.Fatalf("file is %d bytes, want %d", len(raw), len(data)+footerSize)
	}
	if !bytes.Equal(raw[:len(data)], data) {
		t.Fatal("data section differs")
	}
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if got := binary.BigEndian.Uint32(raw[len(data):]); got != want {
		t.Fatalf("footer %08x want %08x", got, want)
	}
}

func TestEmptyChunkMatchesKnownHash(t *testing.T) {
	s, _ := newStore(t)
	const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if err := s.Write(context.Background(), emptySHA, bytes.NewReader(nil), 0); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s, emptySHA); len(got) != 0 {
		t.Fatalf("empty chunk read back %d bytes", len(got))
	}
}

func TestWriteHandlesSlowReader(t *testing.T) {
	s, _ := newStore(t)
	data, id := chunk(t, 10000)
	if err := s.Write(context.Background(), id, oneByteReader{bytes.NewReader(data)}, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, s, id), data) {
		t.Fatal("data mismatch with 1-byte reads")
	}
}

type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

// ── Write rejections leave nothing behind ────────────────────────────────────

func TestWriteRejections(t *testing.T) {
	data, id := chunk(t, 100)
	_, otherID := chunk(t, 100)
	big := make([]byte, MaxChunkSize+1)
	bigSum := sha256.Sum256(big)
	cases := []struct {
		name string
		id   string
		body io.Reader
		size int64
		want error
	}{
		{"body shorter than size", id, bytes.NewReader(data[:50]), 100, ErrSizeMismatch},
		{"body longer than size", id, bytes.NewReader(append(data, 'x')), 100, ErrSizeMismatch},
		{"size zero with body", id, bytes.NewReader(data), 0, ErrSizeMismatch},
		{"negative size", id, bytes.NewReader(data), -1, ErrSizeMismatch},
		{"over max size", hex.EncodeToString(bigSum[:]), bytes.NewReader(big), int64(len(big)), ErrChunkTooLarge},
		{"hash does not match id", otherID, bytes.NewReader(data), 100, ErrChecksumMismatch},
		{"reader fails midway", id, io.MultiReader(bytes.NewReader(data[:40]), errReader{}), 100, errBoom},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, dir := newStore(t)
			err := s.Write(context.Background(), c.id, c.body, c.size)
			if !errors.Is(err, c.want) {
				t.Fatalf("err=%v want %v", err, c.want)
			}
			assertDirEmpty(t, dir)
		})
	}
}

var errBoom = errors.New("boom")

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errBoom }

func TestFailedOverwriteKeepsOldCopy(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 100)
	put(t, s, data, id)
	err := s.Write(context.Background(), id, bytes.NewReader(data[:10]), 100)
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("err=%v", err)
	}
	if !bytes.Equal(readAll(t, s, id), data) {
		t.Fatal("failed write damaged the existing chunk")
	}
	assertNoTempFiles(t, dir)
}

// ── Context cancellation ─────────────────────────────────────────────────────

func TestWriteCancelledBeforeStart(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Write(ctx, id, bytes.NewReader(data), 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	assertDirEmpty(t, dir)
}

func TestWriteCancelledMidStream(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	r := &cancelAfter{r: bytes.NewReader(data), n: 3, cancel: cancel}
	if err := s.Write(ctx, id, oneByteReader{r}, int64(len(data))); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	assertDirEmpty(t, dir)
}

type cancelAfter struct {
	r      io.Reader
	n      int
	cancel func()
}

func (c *cancelAfter) Read(p []byte) (int, error) {
	if c.n == 0 {
		c.cancel()
	}
	c.n--
	return c.r.Read(p)
}

func TestCancelledContextRejectedEverywhere(t *testing.T) {
	s, _ := newStore(t)
	data, id := chunk(t, 10)
	put(t, s, data, id)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Read(ctx, id); !errors.Is(err, context.Canceled) {
		t.Errorf("Read err=%v", err)
	}
	if _, err := s.VerifyCRC(ctx, id); !errors.Is(err, context.Canceled) {
		t.Errorf("VerifyCRC err=%v", err)
	}
	if _, err := s.Exists(ctx, id); !errors.Is(err, context.Canceled) {
		t.Errorf("Exists err=%v", err)
	}
	if _, err := s.List(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("List err=%v", err)
	}
	if err := s.Delete(ctx, id); !errors.Is(err, context.Canceled) {
		t.Errorf("Delete err=%v", err)
	}
	if ok, _ := s.Exists(context.Background(), id); !ok {
		t.Error("cancelled Delete must not remove the chunk")
	}
}

// ── Chunk id validation ──────────────────────────────────────────────────────

func TestInvalidIDsRejectedByEveryMethod(t *testing.T) {
	valid := strings.Repeat("a", 64)
	bad := []string{
		"",
		"abc",
		valid[:63],
		valid + "a",
		strings.Repeat("A", 64),
		strings.Repeat("g", 64),
		"../../../../etc/passwd" + strings.Repeat("a", 42),
		strings.Repeat("a", 31) + "/" + strings.Repeat("a", 32),
		strings.Repeat("a", 63) + "\x00",
		strings.Repeat("a", 63) + " ",
		strings.Repeat("a", 60) + ".bin",
	}
	s, dir := newStore(t)
	ctx := context.Background()
	for _, id := range bad {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			if err := s.Write(ctx, id, bytes.NewReader(nil), 0); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Write err=%v", err)
			}
			if _, err := s.Read(ctx, id); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Read err=%v", err)
			}
			if err := s.Delete(ctx, id); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Delete err=%v", err)
			}
			if _, err := s.Exists(ctx, id); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Exists err=%v", err)
			}
			if _, err := s.VerifyCRC(ctx, id); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("VerifyCRC err=%v", err)
			}
		})
	}
	assertDirEmpty(t, dir)
}

// ── Read ─────────────────────────────────────────────────────────────────────

func TestReadMissing(t *testing.T) {
	s, _ := newStore(t)
	_, id := chunk(t, 10)
	if _, err := s.Read(context.Background(), id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestReadTruncatedFileIsCorrupt(t *testing.T) {
	for _, n := range []int{0, 1, footerSize - 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s, dir := newStore(t)
			_, id := chunk(t, 10)
			os.WriteFile(filepath.Join(dir, id+chunkExt), make([]byte, n), 0o644)
			if _, err := s.Read(context.Background(), id); !errors.Is(err, ErrCorruptChunk) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestReadDirectoryInPlaceOfChunk(t *testing.T) {
	s, dir := newStore(t)
	_, id := chunk(t, 10)
	os.Mkdir(filepath.Join(dir, id+chunkExt), 0o755)
	if _, err := s.Read(context.Background(), id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("Read err=%v", err)
	}
	if ok, err := s.Exists(context.Background(), id); ok || err != nil {
		t.Fatalf("Exists ok=%v err=%v", ok, err)
	}
	if _, err := s.VerifyCRC(context.Background(), id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("VerifyCRC err=%v", err)
	}
}

// sendfile(2) is only used when net.TCPConn.ReadFrom receives an *os.File or an *io.LimitedReader wrapping one.
func TestReadPreservesSendfilePath(t *testing.T) {
	s, _ := newStore(t)
	data, id := chunk(t, 5000)
	put(t, s, data, id)
	rc, err := s.Read(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	dst := &spyReaderFrom{}
	if _, err := io.Copy(dst, rc); err != nil {
		t.Fatal(err)
	}
	lr, ok := dst.src.(*io.LimitedReader)
	if !ok {
		t.Fatalf("ReadFrom got %T, want *io.LimitedReader", dst.src)
	}
	if _, ok := lr.R.(*os.File); !ok {
		t.Fatalf("LimitedReader wraps %T, want *os.File", lr.R)
	}
	if !bytes.Equal(dst.buf.Bytes(), data) {
		t.Fatal("bytes copied via WriteTo differ")
	}
}

type spyReaderFrom struct {
	src io.Reader
	buf bytes.Buffer
}

func (s *spyReaderFrom) Write(p []byte) (int, error) { return s.buf.Write(p) }

func (s *spyReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	s.src = r
	return s.buf.ReadFrom(r)
}

func TestReadSurvivesConcurrentDelete(t *testing.T) {
	s, _ := newStore(t)
	data, id := chunk(t, 8000)
	put(t, s, data, id)
	rc, err := s.Read(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("open reader broke after delete: err=%v", err)
	}
}

func TestReadSurvivesConcurrentOverwrite(t *testing.T) {
	s, _ := newStore(t)
	data, id := chunk(t, 8000)
	put(t, s, data, id)
	rc, _ := s.Read(context.Background(), id)
	defer rc.Close()
	put(t, s, data, id)
	got, err := io.ReadAll(rc)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("open reader broke after overwrite: %d bytes, err=%v", len(got), err)
	}
}

// ── VerifyCRC ────────────────────────────────────────────────────────────────

func corrupt(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	f.ReadAt(b, offset)
	b[0] ^= 0xFF
	f.WriteAt(b, offset)
}

func TestVerifyCRCDetectsCorruption(t *testing.T) {
	const n = 2000
	cases := []struct {
		name   string
		mutate func(t *testing.T, path string)
	}{
		{"first data byte flipped", func(t *testing.T, p string) { corrupt(t, p, 0) }},
		{"middle data byte flipped", func(t *testing.T, p string) { corrupt(t, p, n/2) }},
		{"last data byte flipped", func(t *testing.T, p string) { corrupt(t, p, n-1) }},
		{"footer byte flipped", func(t *testing.T, p string) { corrupt(t, p, n+2) }},
		{"truncated by one byte", func(t *testing.T, p string) { os.Truncate(p, n+footerSize-1) }},
		{"truncated below footer", func(t *testing.T, p string) { os.Truncate(p, 2) }},
		{"truncated to empty", func(t *testing.T, p string) { os.Truncate(p, 0) }},
		{"extra byte appended", func(t *testing.T, p string) {
			f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
			f.Write([]byte{0})
			f.Close()
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, dir := newStore(t)
			data, id := chunk(t, n)
			put(t, s, data, id)
			c.mutate(t, filepath.Join(dir, id+chunkExt))
			ok, err := s.VerifyCRC(context.Background(), id)
			if err != nil {
				t.Fatalf("corruption should be reported as ok=false, got err=%v", err)
			}
			if ok {
				t.Fatal("corruption not detected")
			}
		})
	}
}

func TestVerifyCRCMissing(t *testing.T) {
	s, _ := newStore(t)
	_, id := chunk(t, 10)
	if _, err := s.VerifyCRC(context.Background(), id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestOverwriteRepairsCorruptChunk(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 3000)
	put(t, s, data, id)
	corrupt(t, filepath.Join(dir, id+chunkExt), 100)
	if ok, _ := s.VerifyCRC(context.Background(), id); ok {
		t.Fatal("setup: corruption not detected")
	}
	put(t, s, data, id)
	ok, err := s.VerifyCRC(context.Background(), id)
	if err != nil || !ok {
		t.Fatalf("after repair ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(readAll(t, s, id), data) {
		t.Fatal("repaired data differs")
	}
}

// ── Delete / Exists ──────────────────────────────────────────────────────────

func TestDeleteAndExists(t *testing.T) {
	s, dir := newStore(t)
	ctx := context.Background()
	data, id := chunk(t, 10)
	if ok, err := s.Exists(ctx, id); ok || err != nil {
		t.Fatalf("before write ok=%v err=%v", ok, err)
	}
	put(t, s, data, id)
	if ok, err := s.Exists(ctx, id); !ok || err != nil {
		t.Fatalf("after write ok=%v err=%v", ok, err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Exists(ctx, id); ok {
		t.Fatal("still exists after delete")
	}
	if err := s.Delete(ctx, id); !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("second delete err=%v", err)
	}
	assertDirEmpty(t, dir)
}

func TestDeleteOnlyTargetsOneChunk(t *testing.T) {
	s, _ := newStore(t)
	a, idA := chunk(t, 10)
	b, idB := chunk(t, 10)
	put(t, s, a, idA)
	put(t, s, b, idB)
	s.Delete(context.Background(), idA)
	if !bytes.Equal(readAll(t, s, idB), b) {
		t.Fatal("deleting one chunk affected another")
	}
}

// ── List ─────────────────────────────────────────────────────────────────────

func TestListEmpty(t *testing.T) {
	s, _ := newStore(t)
	ids, err := s.List(context.Background())
	if err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("ids=%v err=%v (want non-nil empty slice so JSON encodes [])", ids, err)
	}
}

func TestListSortedAndFiltered(t *testing.T) {
	s, dir := newStore(t)
	var want []string
	for i := 0; i < 5; i++ {
		data, id := chunk(t, 20)
		put(t, s, data, id)
		want = append(want, id)
	}
	_, unrelated := chunk(t, 1)
	os.WriteFile(filepath.Join(dir, unrelated+".abc.tmp"), nil, 0o644)
	os.WriteFile(filepath.Join(dir, "short.bin"), nil, 0o644)
	os.WriteFile(filepath.Join(dir, strings.ToUpper(unrelated)+".bin"), nil, 0o644)
	os.WriteFile(filepath.Join(dir, unrelated), nil, 0o644)
	os.Mkdir(filepath.Join(dir, unrelated+chunkExt), 0o755)
	os.Symlink(filepath.Join(dir, want[0]+chunkExt), filepath.Join(dir, strings.Repeat("f", 64)+chunkExt))

	ids, err := s.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(want)
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v\nwant %v", ids, want)
	}
}

func TestListAfterDataDirRemoved(t *testing.T) {
	s, dir := newStore(t)
	os.RemoveAll(dir)
	if _, err := s.List(context.Background()); err == nil {
		t.Fatal("expected error when chunk dir disappears")
	}
}

// ── Concurrency (run with -race) ─────────────────────────────────────────────

func TestConcurrentWritesSameChunk(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 50000)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Write(context.Background(), id, bytes.NewReader(data), int64(len(data)))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := s.VerifyCRC(context.Background(), id); !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	assertNoTempFiles(t, dir)
}

func TestConcurrentMixedOperations(t *testing.T) {
	s, dir := newStore(t)
	ctx := context.Background()
	type item struct {
		data []byte
		id   string
	}
	items := make([]item, 20)
	for i := range items {
		items[i].data, items[i].id = chunk(t, 4096)
	}
	var wg sync.WaitGroup
	for round := 0; round < 5; round++ {
		for _, it := range items {
			wg.Add(4)
			go func() { defer wg.Done(); s.Write(ctx, it.id, bytes.NewReader(it.data), int64(len(it.data))) }()
			go func() {
				defer wg.Done()
				if rc, err := s.Read(ctx, it.id); err == nil {
					b, _ := io.ReadAll(rc)
					rc.Close()
					if !bytes.Equal(b, it.data) {
						t.Errorf("reader saw partial or wrong data (%d bytes)", len(b))
					}
				}
			}()
			go func() { defer wg.Done(); s.VerifyCRC(ctx, it.id) }()
			go func() { defer wg.Done(); s.List(ctx) }()
		}
	}
	wg.Wait()
	for _, it := range items {
		if ok, err := s.VerifyCRC(ctx, it.id); !ok || err != nil {
			t.Fatalf("chunk %s ok=%v err=%v", it.id[:8], ok, err)
		}
	}
	assertNoTempFiles(t, dir)
}

// ── Filesystem failures ──────────────────────────────────────────────────────

func TestWriteFailsWhenChunkDirRemoved(t *testing.T) {
	s, dir := newStore(t)
	os.RemoveAll(dir)
	data, id := chunk(t, 10)
	if err := s.Write(context.Background(), id, bytes.NewReader(data), 10); err == nil {
		t.Fatal("expected error when chunk dir is gone")
	}
}

func TestWriteRenameFailureCleansUp(t *testing.T) {
	s, dir := newStore(t)
	data, id := chunk(t, 10)
	blocker := filepath.Join(dir, id+chunkExt)
	os.Mkdir(blocker, 0o755)
	os.WriteFile(filepath.Join(blocker, "x"), nil, 0o644)
	if err := s.Write(context.Background(), id, bytes.NewReader(data), 10); err == nil {
		t.Fatal("expected rename onto a non-empty directory to fail")
	}
	assertNoTempFiles(t, dir)
}

func TestDeleteNonNotFoundErrorIsNotMasked(t *testing.T) {
	s, dir := newStore(t)
	_, id := chunk(t, 10)
	blocker := filepath.Join(dir, id+chunkExt)
	os.Mkdir(blocker, 0o755)
	os.WriteFile(filepath.Join(blocker, "x"), nil, 0o644)
	err := s.Delete(context.Background(), id)
	if err == nil || errors.Is(err, models.ErrNotFound) {
		t.Fatalf("want a real error, got %v", err)
	}
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permissions")
	}
}

func TestPermissionErrorsAreNotReportedAsMissing(t *testing.T) {
	skipIfRoot(t)
	s, dir := newStore(t)
	data, id := chunk(t, 10)
	put(t, s, data, id)
	os.Chmod(dir, 0o000)
	defer os.Chmod(dir, 0o755)
	ctx := context.Background()
	if ok, err := s.Exists(ctx, id); err == nil {
		t.Errorf("Exists ok=%v with nil error; unreadable must not look absent", ok)
	}
	if _, err := s.Read(ctx, id); err == nil || errors.Is(err, models.ErrNotFound) {
		t.Errorf("Read err=%v", err)
	}
	if _, err := s.VerifyCRC(ctx, id); err == nil || errors.Is(err, models.ErrNotFound) {
		t.Errorf("VerifyCRC err=%v", err)
	}
	if _, err := s.List(ctx); err == nil {
		t.Error("List should fail on unreadable dir")
	}
	if err := s.Write(ctx, id, bytes.NewReader(data), 10); err == nil {
		t.Error("Write should fail on unwritable dir")
	}
}

func TestUnreadableChunkFile(t *testing.T) {
	skipIfRoot(t)
	s, dir := newStore(t)
	data, id := chunk(t, 10)
	put(t, s, data, id)
	p := filepath.Join(dir, id+chunkExt)
	os.Chmod(p, 0o000)
	defer os.Chmod(p, 0o644)
	if _, err := s.Read(context.Background(), id); err == nil || errors.Is(err, models.ErrNotFound) {
		t.Fatalf("Read err=%v", err)
	}
}

func TestNewFailsOnUnreadableDir(t *testing.T) {
	skipIfRoot(t)
	root := t.TempDir()
	dir := filepath.Join(root, "chunks")
	os.Mkdir(dir, 0o000)
	defer os.Chmod(dir, 0o755)
	if _, err := New(root); err == nil {
		t.Fatal("expected error scanning unreadable chunk dir")
	}
}

func TestReadReportsSize(t *testing.T) {
	for _, n := range []int{0, 1, 5000, MaxChunkSize} {
		s, _ := newStore(t)
		data, id := chunk(t, n)
		put(t, s, data, id)
		rc, err := s.Read(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		sz, ok := rc.(interface{ Size() int64 })
		if !ok || sz.Size() != int64(n) {
			t.Fatalf("Size() missing or wrong for %d-byte chunk", n)
		}
		rc.Close()
	}
}
