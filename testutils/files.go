package testutils

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/protobuf/proto"
)

// CreateBadgerBackupFile creates a file in badger backup format with the given entries
// Returns an error instead of using require for better testability
func CreateBadgerBackupFile(filePath string, entries []TestEntry) error {
	// Convert test entries to protobuf KV format
	var kvs []*pb.KV
	for _, entry := range entries {
		kv := &pb.KV{
			Key:   entry.Key,
			Value: entry.Value,
		}
		kvs = append(kvs, kv)
	}

	kvList := &pb.KVList{Kv: kvs}
	kvBytes, err := proto.Marshal(kvList)
	if err != nil {
		return fmt.Errorf("failed to marshal KV list: %w", err)
	}

	// Write to file in badger backup format
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	// Write length (4 bytes)
	if err := binary.Write(file, binary.LittleEndian, uint32(len(kvBytes))); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write CRC (4 bytes) - placeholder
	if err := binary.Write(file, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write CRC: %w", err)
	}

	// Write data
	if _, err := file.Write(kvBytes); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// StateFileSet manages creation of multiple related state change files
type StateFileSet struct {
	baseDir string
}

// NewStateFileSet creates a new file set manager for the given base directory
func NewStateFileSet(baseDir string) *StateFileSet {
	return &StateFileSet{baseDir: baseDir}
}

// CreateHypersyncFile creates a hypersync chunk file with the given entries
func (s *StateFileSet) CreateHypersyncFile(blockHeight uint64, chunkId int, timestampNanos uint64, entries []TestEntry) error {
	filename := fmt.Sprintf("hypersync_chunk_%d_%d_%d.bin", blockHeight, chunkId, timestampNanos)
	filePath := filepath.Join(s.baseDir, filename)
	return CreateBadgerBackupFile(filePath, entries)
}

// CreateCommittedBlockFile creates a committed block state change file
func (s *StateFileSet) CreateCommittedBlockFile(blockHeight uint64, entries []TestEntry) error {
	filename := fmt.Sprintf("state_changes_%d.bin", blockHeight)
	filePath := filepath.Join(s.baseDir, filename)
	return CreateBadgerBackupFile(filePath, entries)
}

// CreateMempoolFile creates a mempool state change file
func (s *StateFileSet) CreateMempoolFile(blockHeight uint64, timestampNanos uint64, entries []TestEntry) error {
	filename := fmt.Sprintf("mempool_%d_%d.bin", blockHeight, timestampNanos)
	filePath := filepath.Join(s.baseDir, filename)
	return CreateBadgerBackupFile(filePath, entries)
}

// CreateAncestralFile creates an ancestral record file (for mempool reverts)
func (s *StateFileSet) CreateAncestralFile(blockHeight uint64, timestampNanos uint64, entries []TestEntry) error {
	filename := fmt.Sprintf("mempool_ancestral_%d_%d.bin", blockHeight, timestampNanos)
	filePath := filepath.Join(s.baseDir, filename)
	return CreateBadgerBackupFile(filePath, entries)
}

// CreateEmptyAncestralFile creates an empty ancestral record file for simple test scenarios
func (s *StateFileSet) CreateEmptyAncestralFile(blockHeight uint64, timestampNanos uint64) error {
	filename := fmt.Sprintf("mempool_ancestral_%d_%d.bin", blockHeight, timestampNanos)
	filePath := filepath.Join(s.baseDir, filename)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create empty ancestral file %s: %w", filePath, err)
	}
	defer file.Close()

	return nil
}

// Convenience methods that combine builder with file creation

// CreateHypersyncFileWithBuilder creates a hypersync file using the builder pattern
func (s *StateFileSet) CreateHypersyncFileWithBuilder(blockHeight uint64, chunkId int, timestampNanos uint64, builderFunc func(*StateFileBuilder) *StateFileBuilder) error {
	builder := NewStateFileBuilder()
	builder = builderFunc(builder)
	return s.CreateHypersyncFile(blockHeight, chunkId, timestampNanos, builder.GetEntries())
}

// CreateCommittedBlockFileWithBuilder creates a committed block file using the builder pattern
func (s *StateFileSet) CreateCommittedBlockFileWithBuilder(blockHeight uint64, builderFunc func(*StateFileBuilder) *StateFileBuilder) error {
	builder := NewStateFileBuilder()
	builder = builderFunc(builder)
	return s.CreateCommittedBlockFile(blockHeight, builder.GetEntries())
}

// CreateMempoolFileWithBuilder creates a mempool file using the builder pattern
func (s *StateFileSet) CreateMempoolFileWithBuilder(blockHeight uint64, timestampNanos uint64, builderFunc func(*StateFileBuilder) *StateFileBuilder) error {
	builder := NewStateFileBuilder()
	builder = builderFunc(builder)
	return s.CreateMempoolFile(blockHeight, timestampNanos, builder.GetEntries())
}
