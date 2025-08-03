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

func TestNewMempoolProcessor(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()

	processor := NewMempoolProcessor(fm, pm, am, handler, 100)
	require.NotNil(processor)
	require.Equal(uint64(100), processor.batchSize)
	require.NotNil(processor.processedFiles)
	require.NotNil(processor.stopProcessing)
}

func TestMempoolProcessor_ProcessNextMempoolFile_NoFiles(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// No files available
	result, err := processor.ProcessNextMempoolFile()
	require.NoError(err)
	require.Nil(result) // No file to process
}

func TestMempoolProcessor_ProcessNextMempoolFile_WithFile(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 2) // Small batch for testing

	// Set current mempool block height to 100
	err = pm.UpdateCommittedBlockProgress(100)
	require.NoError(err)

	// Create a mempool file
	mempoolFile := filepath.Join(tempDir, "mempool_100_1640995200000000000.bin")
	createTestMempoolFile(t, mempoolFile, 3)

	// Process the mempool file
	result, err := processor.ProcessNextMempoolFile()
	require.NoError(err)
	require.NotNil(result)
	require.Equal(uint64(100), result.BlockHeight)
	require.Equal(int64(1640995200000000000), result.Timestamp)
	require.Equal(3, result.EntriesProcessed)
	require.NoError(result.Error)
	require.Greater(result.ProcessingTime, time.Duration(0))

	// Verify batches were created (isMempool = true)
	require.Len(handler.batches, 2) // Should be 2 batches: [2 entries, 1 entry]
	require.Len(handler.batches[0], 2)
	require.Len(handler.batches[1], 1)

	// Verify progress was updated
	state := pm.GetCurrentState()
	require.Equal(int64(1640995200000000000), state.LastMempoolTimestamp)
	require.Contains(state.AppliedMempoolFiles, mempoolFile)

	// Verify file is marked as processed
	require.True(processor.IsFileProcessed(mempoolFile))
}

func TestMempoolProcessor_KvToStateChangeEntry(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Test KV conversion
	kv := &pb.KV{
		Key:   []byte{5, 1, 2, 3}, // Post entry prefix
		Value: []byte("mempool_test_data"),
	}

	entry, err := processor.kvToStateChangeEntry(kv, 100, 1640995200000000000)
	require.NoError(err)
	require.NotNil(entry)
	require.Equal(lib.DbOperationTypeUpsert, entry.OperationType)
	require.Equal(kv.Key, entry.KeyBytes)
	require.Equal(kv.Value, entry.EncoderBytes)
	require.Equal(uint64(100), entry.BlockHeight)
}

func TestMempoolProcessor_ShouldTransitionToCommittedBlocks(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Set last committed block height
	err = pm.UpdateCommittedBlockProgress(100)
	require.NoError(err)

	// No new committed block files - should not transition
	shouldTransition := processor.shouldTransitionToCommittedBlocks()
	require.False(shouldTransition)

	// Create a new committed block file
	committedFile := filepath.Join(tempDir, "state_changes_101.bin")
	err = os.WriteFile(committedFile, []byte("test"), 0644)
	require.NoError(err)

	// Now should transition
	shouldTransition = processor.shouldTransitionToCommittedBlocks()
	require.True(shouldTransition)
}

func TestMempoolProcessor_ProcessMempoolFilesForBlock(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Create multiple mempool files for the same block height
	createTestMempoolFile(t, filepath.Join(tempDir, "mempool_100_1640995200000000000.bin"), 2)
	createTestMempoolFile(t, filepath.Join(tempDir, "mempool_100_1640995201000000000.bin"), 3)
	createTestMempoolFile(t, filepath.Join(tempDir, "mempool_100_1640995202000000000.bin"), 1)

	// Process all mempool files for block 100
	err = processor.ProcessMempoolFilesForBlock(100)
	require.NoError(err)

	// Verify all files were processed
	require.Equal(3, processor.GetProcessedFilesCount())

	// Verify entries were processed in correct order (chronological by timestamp)
	require.Greater(handler.GetProcessedEntryCount(), 0)

	// Verify final progress state
	state := pm.GetCurrentState()
	require.Equal(int64(1640995202000000000), state.LastMempoolTimestamp)
	require.Len(state.AppliedMempoolFiles, 3)
}

func TestMempoolProcessor_ProcessMempoolSequence(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Set current mempool block height to 100
	err = pm.UpdateCommittedBlockProgress(100)
	require.NoError(err)

	// Create mempool files
	createTestMempoolFile(t, filepath.Join(tempDir, "mempool_100_1640995200000000000.bin"), 2)
	createTestMempoolFile(t, filepath.Join(tempDir, "mempool_100_1640995201000000000.bin"), 2)

	// Process sequence
	err = processor.ProcessMempoolSequence()
	require.NoError(err)

	// Verify files were processed
	require.Equal(2, processor.GetProcessedFilesCount())
	require.Greater(handler.GetProcessedEntryCount(), 0)
}

func TestMempoolProcessor_GetProcessingStats(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Mark some files as processed
	processor.processedFiles["file1.bin"] = true
	processor.processedFiles["file2.bin"] = true
	processor.currentBlockHeight = 100

	// Update progress
	err = pm.UpdateMempoolProgress("file1.bin", 1640995200000000000)
	require.NoError(err)

	stats := processor.GetProcessingStats()
	require.NotNil(stats)
	require.Equal("mempool", stats["mode"])
	require.Equal(2, stats["processed_files_count"])
	require.Equal(uint64(100), stats["batch_size"])
	require.Equal(int64(1640995200000000000), stats["last_timestamp"])
}

func TestMempoolProcessor_Reset(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Mark some files as processed
	processor.processedFiles["file1.bin"] = true
	processor.processedFiles["file2.bin"] = true
	processor.currentBlockHeight = 100
	require.Len(processor.processedFiles, 2)
	require.Equal(uint64(100), processor.currentBlockHeight)

	// Reset
	processor.Reset()
	require.Len(processor.processedFiles, 0)
	require.Equal(uint64(0), processor.currentBlockHeight)
}

func TestMempoolProcessor_StopProcessing(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Start processing in background
	done := make(chan error, 1)
	go func() {
		done <- processor.ProcessMempoolSequence()
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

func TestMempoolProcessor_SequentialProcessing(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "mempool-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	am := NewAncestralRecordManager(tempDir, NewMockDataHandler())
	handler := NewMockDataHandler()
	processor := NewMempoolProcessor(fm, pm, am, handler, 100)

	// Set current mempool block height to 100
	err = pm.UpdateCommittedBlockProgress(100)
	require.NoError(err)

	// Create sequential mempool files with different timestamps
	files := []struct {
		name      string
		timestamp int64
		entries   int
	}{
		{"mempool_100_1640995200000000000.bin", 1640995200000000000, 2},
		{"mempool_100_1640995201000000000.bin", 1640995201000000000, 3},
		{"mempool_100_1640995202000000000.bin", 1640995202000000000, 1},
	}

	for _, file := range files {
		createTestMempoolFile(t, filepath.Join(tempDir, file.name), file.entries)
	}

	// Process files one by one
	for i, file := range files {
		result, err := processor.ProcessNextMempoolFile()
		require.NoError(err)
		require.NotNil(result)
		require.Equal(file.timestamp, result.Timestamp)
		require.Equal(file.entries, result.EntriesProcessed)

		// Verify progress is updated correctly
		state := pm.GetCurrentState()
		require.Equal(file.timestamp, state.LastMempoolTimestamp)
		require.Len(state.AppliedMempoolFiles, i+1)
	}

	// No more files to process
	result, err := processor.ProcessNextMempoolFile()
	require.NoError(err)
	require.Nil(result)
}

// Helper function to create test mempool files
func createTestMempoolFile(t *testing.T, filePath string, numEntries int) {
	require := require.New(t)

	// Create test KV entries
	var kvs []*pb.KV
	for i := 0; i < numEntries; i++ {
		kv := &pb.KV{
			Key:   []byte{5, byte(i), byte(i + 1)}, // Post entry prefix + unique data
			Value: []byte(fmt.Sprintf("mempool_data_%d", i)),
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
