package consumer

import (
	"github.com/deso-protocol/core/lib"
)

// MigrationHeightInfo stores information about a migration height
type MigrationHeightInfo struct {
	Name    string
	Height  uint64
	Version byte
}

// GetMigrationHeights returns all migration heights for the configured network
func GetMigrationHeights(params *lib.DeSoParams) []MigrationHeightInfo {
	migrationHeights := lib.GetEncoderMigrationHeightsList(&params.ForkHeights)

	result := make([]MigrationHeightInfo, len(migrationHeights))
	for i, mh := range migrationHeights {
		result[i] = MigrationHeightInfo{
			Name:    string(mh.Name),
			Height:  mh.Height,
			Version: mh.Version,
		}
	}
	return result
}
