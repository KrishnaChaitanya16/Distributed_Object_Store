package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"strconv"
)

// newID generates a random 16-byte hex object ID.
func newID() string {
	b := make([]byte, 16)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

func newBytesReader(b []byte) io.Reader {
	return bytes.NewReader(b)
}

// noCancel wraps any context-like value and returns a background context.
// Used when the request context is already cancelled but we still need to
// write metadata (rollback, status update).
func noCancel(_ interface{ Done() <-chan struct{} }) context.Context {
	return context.Background()
}

func noCancel2() context.Context {
	return context.Background()
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
