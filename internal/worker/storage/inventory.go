package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func (s *Store) Exists(ctx context.Context, chunkID string) (bool, error) {
	if !validID(chunkID) {
		return false, ErrInvalidChunkID
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	st, err := os.Stat(s.path(chunkID))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat chunk: %w", err)
	}
	return st.Mode().IsRegular(), nil
}

// Delete removes the chunk and returns models.ErrNotFound if it was not present.
func (s *Store) Delete(ctx context.Context, chunkID string) error {
	if !validID(chunkID) {
		return ErrInvalidChunkID
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(s.path(chunkID)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return models.ErrNotFound
		}
		return fmt.Errorf("remove chunk: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("fsync chunk dir: %w", err)
	}
	return nil
}

// List returns the sorted ids of every stored chunk, ignoring temp files and anything not named <id>.bin.
func (s *Store) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read chunk dir: %w", err)
	}
	ids := []string{}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		id, ok := strings.CutSuffix(e.Name(), chunkExt)
		if ok && validID(id) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}
