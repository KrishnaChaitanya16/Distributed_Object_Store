package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const (
	MaxChunkSize = 1 << 20
	footerSize   = 4
	chunkExt     = ".bin"
	tmpExt       = ".tmp"
)

var (
	ErrInvalidChunkID   = errors.New("chunk id must be 64 lowercase hex characters")
	ErrChunkTooLarge    = fmt.Errorf("chunk exceeds %d bytes", MaxChunkSize)
	ErrSizeMismatch     = errors.New("chunk body length does not match declared size")
	ErrChecksumMismatch = errors.New("chunk body sha256 does not match chunk id")
	ErrCorruptChunk     = errors.New("chunk file is shorter than its crc footer")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var _ models.ChunkStore = (*Store)(nil)

// Store keeps each chunk as <dir>/<chunkID>.bin holding the data followed by a big-endian CRC32C footer.
type Store struct {
	dir string
}

// New prepares <dataDir>/chunks and removes temp files left behind by a crash mid-write.
func New(dataDir string) (*Store, error) {
	dir := filepath.Join(dataDir, "chunks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk dir: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("scan chunk dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), tmpExt) {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("remove stale temp file: %w", err)
			}
		}
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(chunkID string) string {
	return filepath.Join(s.dir, chunkID+chunkExt)
}

func validID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
