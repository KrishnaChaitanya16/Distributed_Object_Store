package placement

import "github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"

// Primary returns the first clockwise worker for chunkID.
// Implements models.PlacementStrategy.
func (r *Ring) Primary(chunkID string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.workers) == 0 {
		return "", models.ErrNoWorkers
	}
	workerID := r.lookup(chunkID, nil)
	if workerID == "" {
		return "", models.ErrNoWorkers
	}
	return workerID, nil
}

// Secondary returns the next clockwise worker on a different physical node.
// Implements models.PlacementStrategy.
func (r *Ring) Secondary(chunkID string, excludeWorkerID string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.workers) < 2 {
		return "", models.ErrNoWorkers
	}
	exclude := map[string]struct{}{excludeWorkerID: {}}
	workerID := r.lookup(chunkID, exclude)
	if workerID == "" {
		return "", models.ErrNoWorkers
	}
	return workerID, nil
}
