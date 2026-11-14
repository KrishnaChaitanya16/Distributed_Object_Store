package api

import (
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// openMetadataStore opens a real bbolt-backed MetadataRepository for use in tests.
func openMetadataStore(path string) (models.MetadataRepository, error) {
	return metadata.NewMetadataStore(path)
}
