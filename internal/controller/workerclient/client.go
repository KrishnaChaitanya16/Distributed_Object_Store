package workerclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// workerClient makes HTTP calls to a single worker node.
// The worker API is defined by the data-plane (teammate's server).
type workerClient struct {
	baseURL    string // e.g. "http://10.0.0.5:8080"
	httpClient *http.Client
}

func newWorkerClient(baseURL string, httpClient *http.Client) *workerClient {
	return &workerClient{baseURL: baseURL, httpClient: httpClient}
}

// PutChunk streams chunk data to the worker via PUT /chunks/{chunkID}.
func (c *workerClient) PutChunk(ctx context.Context, chunkID string, data io.Reader, size int64) error {
	url := fmt.Sprintf("%s/internal/chunks/%s", c.baseURL, chunkID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, data)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("put chunk %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("put chunk %s: worker returned %d", chunkID, resp.StatusCode)
	}
	return nil
}

// GetChunk fetches chunk data from the worker via GET /chunks/{chunkID}.
// Caller must close the returned ReadCloser.
func (c *workerClient) GetChunk(ctx context.Context, chunkID string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/internal/chunks/%s", c.baseURL, chunkID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get chunk %s: %w", chunkID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, fmt.Errorf("get chunk %s: %w", chunkID, errNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("get chunk %s: worker returned %d", chunkID, resp.StatusCode)
	}
	return resp.Body, nil
}

// DeleteChunk removes a chunk from the worker via DELETE /chunks/{chunkID}.
func (c *workerClient) DeleteChunk(ctx context.Context, chunkID string) error {
	url := fmt.Sprintf("%s/internal/chunks/%s", c.baseURL, chunkID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete chunk %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete chunk %s: worker returned %d", chunkID, resp.StatusCode)
	}
	return nil
}

// ReplicatePeer tells the source worker to stream a chunk directly to targetAddr
// via POST /chunks/{chunkID}/replicate.
func (c *workerClient) ReplicatePeer(ctx context.Context, chunkID string, targetAddr string) error {
	url := fmt.Sprintf("%s/internal/workers/replicate-peer", c.baseURL)

	body, err := json.Marshal(map[string]string{"chunk_id": chunkID, "target_addr": targetAddr})
	if err != nil {
		return fmt.Errorf("marshal replicate body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
		io.NopCloser(bytesReader(body)))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("replicate chunk %s to %s: %w", chunkID, targetAddr, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("replicate chunk %s: worker returned %d", chunkID, resp.StatusCode)
	}
	return nil
}

// ListChunks fetches the list of chunk IDs stored on the worker via GET /internal/workers/{workerID}/chunks.
func (c *workerClient) ListChunks(ctx context.Context, workerID string) ([]string, error) {
	url := fmt.Sprintf("%s/internal/workers/%s/chunks", c.baseURL, workerID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list chunks: worker returned %d", resp.StatusCode)
	}

	var inv models.InventoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
		return nil, fmt.Errorf("decode chunk list: %w", err)
	}
	return inv.ChunkIDs, nil
}
