package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Read returns the chunk data without its footer; the caller must Close it.
func (s *Store) Read(ctx context.Context, chunkID string) (io.ReadCloser, error) {
	if !validID(chunkID) {
		return nil, ErrInvalidChunkID
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, dataLen, err := s.open(chunkID)
	if err != nil {
		return nil, err
	}
	if dataLen < 0 {
		f.Close()
		return nil, ErrCorruptChunk
	}
	return &chunkReader{lr: io.LimitedReader{R: f, N: dataLen}, f: f, size: dataLen}, nil
}

// VerifyCRC reports whether the stored data still matches its footer; a truncated file is reported as not ok.
func (s *Store) VerifyCRC(ctx context.Context, chunkID string) (bool, error) {
	if !validID(chunkID) {
		return false, ErrInvalidChunkID
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f, dataLen, err := s.open(chunkID)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if dataLen < 0 {
		return false, nil
	}

	crc := crc32.New(castagnoli)
	br := bufio.NewReader(f)
	if _, err := io.CopyN(crc, ctxReader{ctx, br}, dataLen); err != nil {
		return false, fmt.Errorf("read chunk data: %w", err)
	}
	var footer [footerSize]byte
	if _, err := io.ReadFull(br, footer[:]); err != nil {
		return false, fmt.Errorf("read crc footer: %w", err)
	}
	return binary.BigEndian.Uint32(footer[:]) == crc.Sum32(), nil
}

// open returns the file and the length of its data section, which is negative if the footer is missing.
func (s *Store) open(chunkID string) (*os.File, int64, error) {
	f, err := os.Open(s.path(chunkID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, models.ErrNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open chunk: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("stat chunk: %w", err)
	}
	if !st.Mode().IsRegular() {
		f.Close()
		return nil, 0, models.ErrNotFound
	}
	return f, st.Size() - footerSize, nil
}

// chunkReader hands io.Copy an *io.LimitedReader over *os.File so net.TCPConn can use sendfile(2).
type chunkReader struct {
	lr   io.LimitedReader
	f    *os.File
	size int64
}

// Size is the data length, so HTTP handlers can set Content-Length and keep the sendfile path.
func (c *chunkReader) Size() int64 { return c.size }

func (c *chunkReader) Read(p []byte) (int, error) { return c.lr.Read(p) }

func (c *chunkReader) WriteTo(w io.Writer) (int64, error) { return io.Copy(w, &c.lr) }

func (c *chunkReader) Close() error { return c.f.Close() }
