package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Write atomically stores exactly size bytes whose sha256 must equal chunkID, replacing any existing copy.
func (s *Store) Write(ctx context.Context, chunkID string, data io.Reader, size int64) (err error) {
	if !validID(chunkID) {
		return ErrInvalidChunkID
	}
	if size < 0 {
		return fmt.Errorf("negative size %d: %w", size, ErrSizeMismatch)
	}
	if size > MaxChunkSize {
		return ErrChunkTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(s.dir, chunkID+".*"+tmpExt)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	crc := crc32.New(castagnoli)
	sum := sha256.New()
	// Read one byte past size so an over-long body is detected instead of silently truncated.
	n, err := io.Copy(io.MultiWriter(tmp, crc, sum), io.LimitReader(ctxReader{ctx, data}, size+1))
	if err != nil {
		return fmt.Errorf("write chunk body: %w", err)
	}
	if n != size {
		return fmt.Errorf("got %d bytes, declared %d: %w", n, size, ErrSizeMismatch)
	}
	if hex.EncodeToString(sum.Sum(nil)) != chunkID {
		return ErrChecksumMismatch
	}

	var footer [footerSize]byte
	binary.BigEndian.PutUint32(footer[:], crc.Sum32())
	if _, err = tmp.Write(footer[:]); err != nil {
		return fmt.Errorf("write crc footer: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	mu := s.lock(chunkID)
	mu.Lock()
	err = os.Rename(tmp.Name(), s.path(chunkID))
	mu.Unlock()
	if err != nil {
		return fmt.Errorf("rename into place: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("fsync chunk dir: %w", err)
	}
	return nil
}

// ctxReader aborts a long upload as soon as the request context is cancelled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
