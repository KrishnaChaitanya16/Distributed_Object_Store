package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Quarantine re-verifies under the chunk's lock and moves the chunk aside only if still corrupt, so a concurrent repair is never lost.
func (s *Store) Quarantine(ctx context.Context, chunkID string) (moved bool, err error) {
	if !validID(chunkID) {
		return false, ErrInvalidChunkID
	}
	mu := s.lock(chunkID)
	mu.Lock()
	defer mu.Unlock()

	ok, err := s.VerifyCRC(ctx, chunkID)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	if s.beforeMove != nil {
		s.beforeMove()
	}
	dst, err := s.quarantinePath(chunkID)
	if err != nil {
		return false, err
	}
	if err := os.Rename(s.path(chunkID), dst); err != nil {
		return false, fmt.Errorf("move chunk to quarantine: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return true, fmt.Errorf("fsync chunk dir: %w", err)
	}
	if err := syncDir(s.quarantine); err != nil {
		return true, fmt.Errorf("fsync quarantine dir: %w", err)
	}
	return true, nil
}

// quarantinePath picks a name that never overwrites an earlier quarantined copy of the same chunk.
func (s *Store) quarantinePath(chunkID string) (string, error) {
	base := fmt.Sprintf("%s.%d", chunkID, time.Now().UnixNano())
	for i := 0; i < 1000; i++ {
		name := base + chunkExt
		if i > 0 {
			name = fmt.Sprintf("%s-%d%s", base, i, chunkExt)
		}
		p := filepath.Join(s.quarantine, name)
		if _, err := os.Lstat(p); errors.Is(err, os.ErrNotExist) {
			return p, nil
		} else if err != nil {
			return "", fmt.Errorf("check quarantine name: %w", err)
		}
	}
	return "", errors.New("no free quarantine file name")
}
