package consumer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deso-protocol/core/lib"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestNewCommittedBlockProcessor(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()

	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)
	require.NotNil(processor)
	require.Equal(uint64(100), processor.batchSize)
	require.NotNil(processor.processedBlocks)
	require.NotNil(processor.stopProcessing)
}

func TestCommittedBlockProcessor_ProcessNextCommittedBlock_NoFiles(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// No files available
	result, err := processor.ProcessNextCommittedBlock()
	require.NoError(err)
	require.Nil(result) // No file to process
}

func TestCommittedBlockProcessor_ProcessNextCommittedBlock_WithFile(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 2) // Small batch for testing

	// Create a committed block file
	blockFile := filepath.Join(tempDir, "state_changes_100.bin")
	createTestCommittedBlockFile(t, blockFile, 3)

	// Process the block
	result, err := processor.ProcessNextCommittedBlock()
	require.NoError(err)
	require.NotNil(result)
	require.Equal(uint64(100), result.BlockHeight)
	require.Equal(3, result.EntriesProcessed)
	require.NoError(result.Error)
	require.Greater(result.ProcessingTime, time.Duration(0))

	// Verify batches were created
	require.Len(handler.batches, 2) // Should be 2 batches: [2 entries, 1 entry]
	require.Len(handler.batches[0], 2)
	require.Len(handler.batches[1], 1)

	// Verify progress was updated
	state := pm.GetCurrentState()
	require.Equal(uint64(100), state.LastCommittedBlockHeight)

	// Verify block is marked as processed
	require.True(processor.IsBlockProcessed(100))
}

func TestCommittedBlockProcessor_KvToStateChangeEntry(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// Test KV conversion
	kv := &pb.KV{
		Key:   []byte{3, 1, 2, 3}, // Profile entry prefix
		Value: []byte("test_data"),
	}

	entry, err := processor.kvToStateChangeEntry(kv, 100)
	require.NoError(err)
	require.NotNil(entry)
	require.Equal(lib.DbOperationTypeUpsert, entry.OperationType)
	require.Equal(kv.Key, entry.KeyBytes)
	require.Equal(kv.Value, entry.EncoderBytes)
	require.Equal(uint64(100), entry.BlockHeight)
}

func TestCommittedBlockProcessor_ShouldTransitionToMempool(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// Set last committed block height
	err = pm.UpdateCommittedBlockProgress(100)
	require.NoError(err)

	// No mempool files - should not transition
	shouldTransition := processor.shouldTransitionToMempool()
	require.False(shouldTransition)

	// Create a mempool file for the same block height
	mempoolFile := filepath.Join(tempDir, "mempool_100_1640995200000000000.bin")
	err = os.WriteFile(mempoolFile, []byte("test"), 0644)
	require.NoError(err)

	// Now should transition
	shouldTransition = processor.shouldTransitionToMempool()
	require.True(shouldTransition)
}

func TestCommittedBlockProcessor_GetProcessingStats(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// Mark some blocks as processed
	processor.processedBlocks[100] = true
	processor.processedBlocks[101] = true

	// Update progress
	err = pm.UpdateCommittedBlockProgress(101)
	require.NoError(err)

	stats := processor.GetProcessingStats()
	require.NotNil(stats)
	require.Equal("committed_blocks", stats["mode"])
	require.Equal(uint64(101), stats["last_processed_block"])
	require.Equal(2, stats["processed_blocks_count"])
	require.Equal(uint64(100), stats["batch_size"])
}

func TestCommittedBlockProcessor_Reset(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// Mark some blocks as processed
	processor.processedBlocks[100] = true
	processor.processedBlocks[101] = true
	require.Len(processor.processedBlocks, 2)

	// Reset
	processor.Reset()
	require.Len(processor.processedBlocks, 0)
}

func TestCommittedBlockProcessor_ProcessCommittedBlockSequence(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// Create multiple committed block files
	createTestCommittedBlockFile(t, filepath.Join(tempDir, "state_changes_100.bin"), 2)
	createTestCommittedBlockFile(t, filepath.Join(tempDir, "state_changes_101.bin"), 2)
	createTestCommittedBlockFile(t, filepath.Join(tempDir, "state_changes_102.bin"), 2)

	// Process sequence
	err = processor.ProcessCommittedBlockSequence()
	require.NoError(err)

	// Verify all blocks were processed
	require.True(processor.IsBlockProcessed(100))
	require.True(processor.IsBlockProcessed(101))
	require.True(processor.IsBlockProcessed(102))

	// Verify final progress state
	state := pm.GetCurrentState()
	require.Equal(uint64(102), state.LastCommittedBlockHeight)

	// Verify all entries were processed
	require.Greater(handler.GetProcessedEntryCount(), 0)
}

func TestCommittedBlockProcessor_StopProcessing(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "committed-block-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())

	handler := NewMockDataHandler()
	processor := NewCommittedBlockProcessor(fm, pm, am, handler, 100)

	// Start processing in background
	done := make(chan error, 1)
	go func() {
		done <- processor.ProcessCommittedBlockSequence()
	}()

	// Stop processing
	go func() {
		time.Sleep(50 * time.Millisecond)
		processor.StopProcessing()
	}()

	// Wait for completion
	err = <-done
	require.NoError(err)
}

// Helper function to create test committed block files
func createTestCommittedBlockFile(t *testing.T, filePath string, numEntries int) {
	require := require.New(t)

	// Create test KV entries
	var kvs []*pb.KV
	for i := 0; i < numEntries; i++ {
		kv := &pb.KV{
			Key:   []byte{3, byte(i), byte(i + 1)}, // Profile entry prefix + unique data
			Value: []byte(fmt.Sprintf("committed_block_data_%d", i)),
		}
		kvs = append(kvs, kv)
	}

	kvList := &pb.KVList{Kv: kvs}
	kvBytes, err := proto.Marshal(kvList)
	require.NoError(err)

	// Write to file in badger backup format
	file, err := os.Create(filePath)
	require.NoError(err)
	defer file.Close()

	// Write length (4 bytes)
	err = binary.Write(file, binary.LittleEndian, uint32(len(kvBytes)))
	require.NoError(err)

	// Write CRC (4 bytes) - placeholder
	err = binary.Write(file, binary.LittleEndian, uint32(0))
	require.NoError(err)

	// Write data
	_, err = file.Write(kvBytes)
	require.NoError(err)
}
