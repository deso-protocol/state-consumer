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

// MockDataHandler implements StateSyncerDataHandler for testing
type MockDataHandler struct {
	batches     [][]*lib.StateChangeEntry
	syncEvents  []SyncEvent
	shouldError bool
}

func NewMockDataHandler() *MockDataHandler {
	return &MockDataHandler{
		batches:    make([][]*lib.StateChangeEntry, 0),
		syncEvents: make([]SyncEvent, 0),
	}
}

func (m *MockDataHandler) HandleEntryBatch(entries []*lib.StateChangeEntry, isMempool bool) error {
	if m.shouldError {
		return fmt.Errorf("mock error")
	}
	m.batches = append(m.batches, entries)
	return nil
}

func (m *MockDataHandler) HandleSyncEvent(eventType SyncEvent) error {
	if m.shouldError {
		return fmt.Errorf("mock sync event error")
	}
	m.syncEvents = append(m.syncEvents, eventType)
	return nil
}

func (m *MockDataHandler) InitiateTransaction() error {
	return nil
}

func (m *MockDataHandler) CommitTransaction() error {
	return nil
}

func (m *MockDataHandler) RollbackTransaction() error {
	return nil
}

func (m *MockDataHandler) GetParams() *lib.DeSoParams {
	return &lib.DeSoParams{}
}

func (m *MockDataHandler) GetProcessedEntryCount() int {
	count := 0
	for _, batch := range m.batches {
		count += len(batch)
	}
	return count
}

func TestNewHypersyncProcessor(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	handler := NewMockDataHandler()

	processor := NewHypersyncProcessor(fm, pm, handler, 4, 100)
	require.NotNil(processor)
	require.Equal(4, processor.maxConcurrentChunks)
	require.Equal(uint64(100), processor.batchSize)
	require.NotNil(processor.processedChunks)
	require.NotNil(processor.stopProcessing)
}

func TestCreateHypersyncChunkFile(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	// Create a test hypersync chunk file
	chunkFile := filepath.Join(tempDir, "hypersync_chunk_100_1_1640995200000000000.bin")

	// Create test KV data
	kv1 := &pb.KV{
		Key:   []byte{3, 1, 2, 3}, // Profile entry prefix + data
		Value: []byte("test_profile_data"),
	}
	kv2 := &pb.KV{
		Key:   []byte{5, 4, 5, 6}, // Post entry prefix + data
		Value: []byte("test_post_data"),
	}

	kvList := &pb.KVList{Kv: []*pb.KV{kv1, kv2}}
	kvBytes, err := proto.Marshal(kvList)
	require.NoError(err)

	// Write to file in badger backup format
	file, err := os.Create(chunkFile)
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

	file.Close()

	// Verify file was created and has content
	stat, err := os.Stat(chunkFile)
	require.NoError(err)
	require.Greater(stat.Size(), int64(0))
}

func TestHypersyncProcessor_ReadAndProcessChunkFile(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	handler := NewMockDataHandler()
	processor := NewHypersyncProcessor(fm, pm, handler, 4, 2) // Small batch size for testing

	// Create test chunk file
	chunkFile := filepath.Join(tempDir, "hypersync_chunk_100_1_1640995200000000000.bin")
	createTestChunkFile(t, chunkFile, 3) // Create file with 3 entries

	fileInfo := &FileInfo{
		Path:        chunkFile,
		Type:        FileTypeHypersyncChunk,
		BlockHeight: 100,
		ChunkId:     func() *uint64 { v := uint64(1); return &v }(),
		Timestamp:   1640995200000000000,
	}

	// Process the chunk file
	entriesProcessed, err := processor.readAndProcessChunkFile(fileInfo)
	require.NoError(err)
	require.Equal(3, entriesProcessed)

	// Verify batches were created (should be 2 batches: [2 entries, 1 entry])
	require.Len(handler.batches, 2)
	require.Len(handler.batches[0], 2) // First batch
	require.Len(handler.batches[1], 1) // Second batch
}

func TestHypersyncProcessor_KvToStateChangeEntry(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	handler := NewMockDataHandler()
	processor := NewHypersyncProcessor(fm, pm, handler, 4, 100)

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

func TestHypersyncProcessor_ShouldTransitionFromHypersync(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	handler := NewMockDataHandler()
	processor := NewHypersyncProcessor(fm, pm, handler, 4, 100)

	// Test with no hypersync files
	state := &ProgressState{LastHypersyncChunk: 0}
	allFiles := []*FileInfo{}

	shouldTransition := processor.shouldTransitionFromHypersync(allFiles, state)
	require.True(shouldTransition)

	// Test with unprocessed hypersync files
	allFiles = []*FileInfo{
		{
			Type:    FileTypeHypersyncChunk,
			ChunkId: func() *uint64 { v := uint64(5); return &v }(),
		},
	}
	state.LastHypersyncChunk = 3

	shouldTransition = processor.shouldTransitionFromHypersync(allFiles, state)
	require.False(shouldTransition)

	// Test with all files processed
	state.LastHypersyncChunk = 5
	shouldTransition = processor.shouldTransitionFromHypersync(allFiles, state)
	require.True(shouldTransition)
}

func TestHypersyncProcessor_GetProcessingStats(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	handler := NewMockDataHandler()
	processor := NewHypersyncProcessor(fm, pm, handler, 4, 100)

	// Mark some chunks as processed
	processor.processedChunks[1] = true
	processor.processedChunks[2] = true

	stats := processor.GetProcessingStats()
	require.NotNil(stats)
	require.Equal("hypersync", stats["mode"])
	require.Equal(2, stats["processed_chunks"])
	require.Equal(4, stats["max_concurrent"])
	require.Equal(uint64(100), stats["batch_size"])
}

func TestIsCoreStateKey(t *testing.T) {
	require := require.New(t)

	// Test core state keys
	tests := []struct {
		key         []byte
		isCore      bool
		description string
	}{
		{[]byte{}, false, "empty key"},
		{[]byte{0, 1, 2}, true, "private message prefix"},
		{[]byte{1, 1, 2}, true, "block hash prefix"},
		{[]byte{3, 1, 2}, true, "profile entry prefix"},
		{[]byte{5, 1, 2}, true, "post entry prefix"},
		{[]byte{255, 1, 2}, false, "unknown prefix"},
		{[]byte{100, 1, 2}, false, "non-core prefix"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			result := isCoreStateKey(test.key)
			require.Equal(test.isCore, result, "Key %v should have isCore=%v", test.key, test.isCore)
		})
	}
}

func TestHypersyncProcessor_ProcessChunkResults(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "hypersync-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	pm, err := NewProgressManager(tempDir)
	require.NoError(err)

	handler := NewMockDataHandler()
	processor := NewHypersyncProcessor(fm, pm, handler, 4, 100)

	results := make(chan HypersyncChunkResult, 2)

	// Start result processor in background
	go processor.processChunkResults(results)

	// Send successful result
	results <- HypersyncChunkResult{
		ChunkId:          1,
		EntriesProcessed: 100,
		ProcessingTime:   time.Millisecond * 50,
		Error:            nil,
	}

	// Send error result
	results <- HypersyncChunkResult{
		ChunkId:          2,
		EntriesProcessed: 0,
		ProcessingTime:   time.Millisecond * 10,
		Error:            fmt.Errorf("test error"),
	}

	// Wait a bit for processing
	time.Sleep(time.Millisecond * 100)

	// Check that successful chunk was marked as processed in progress
	state := pm.GetCurrentState()
	require.Equal(uint64(1), state.LastHypersyncChunk)

	// Check that error chunk was not marked as processed
	processor.chunkMutex.RLock()
	_, exists := processor.processedChunks[2]
	processor.chunkMutex.RUnlock()
	require.False(exists)

	close(results)
}

// Helper function to create test chunk files
func createTestChunkFile(t *testing.T, filePath string, numEntries int) {
	require := require.New(t)

	// Create test KV entries
	var kvs []*pb.KV
	for i := 0; i < numEntries; i++ {
		kv := &pb.KV{
			Key:   []byte{3, byte(i), byte(i + 1)}, // Profile entry prefix + unique data
			Value: []byte(fmt.Sprintf("test_data_%d", i)),
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
