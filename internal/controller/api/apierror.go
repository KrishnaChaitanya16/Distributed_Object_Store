package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

type requestIDKey struct{}

// newRequestID generates a short random request identifier (16 hex chars = 8 bytes).
func newRequestID() string {
	var b [8]byte
	rand.Read(b[:]) //nolint:errcheck — rand.Read never returns an error
	return hex.EncodeToString(b[:])
}

// requestIDFrom returns the request ID injected by the instrument middleware.
func requestIDFrom(r *http.Request) string {
	id, _ := r.Context().Value(requestIDKey{}).(string)
	return id
}

// codeFor maps an HTTP status code to a machine-readable error code string.
func codeFor(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusRequestedRangeNotSatisfiable:
		return "range_not_satisfiable"
	case http.StatusServiceUnavailable:
		return "service_unavailable"
	case http.StatusBadGateway:
		return "bad_gateway"
	default:
		return "internal_error"
	}
}

// writeError writes a JSON error response body of the form
//
//	{"error": "...", "code": "...", "request_id": "..."}
//
// It replaces bare http.Error calls throughout the api package, ensuring a
// consistent Content-Type: application/json and machine-readable error codes.
func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
		"error":      msg,
		"code":       codeFor(status),
		"request_id": requestIDFrom(r),
	})
}
