package consumer

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/deso-protocol/core/lib"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// SimpleIntegrationDataHandler for basic integration testing
type SimpleIntegrationDataHandler struct {
	processedEntries []*lib.StateChangeEntry
	events           []SyncEvent
	batchCount       int
}

func NewSimpleIntegrationDataHandler() *SimpleIntegrationDataHandler {
	return &SimpleIntegrationDataHandler{
		processedEntries: make([]*lib.StateChangeEntry, 0),
		events:           make([]SyncEvent, 0),
	}
}

func (h *SimpleIntegrationDataHandler) HandleEntryBatch(entries []*lib.StateChangeEntry, isMempool bool) error {
	h.processedEntries = append(h.processedEntries, entries...)
	h.batchCount++
	return nil
}

func (h *SimpleIntegrationDataHandler) HandleSyncEvent(event SyncEvent) error {
	h.events = append(h.events, event)
	return nil
}

func (h *SimpleIntegrationDataHandler) InitiateTransaction() error { return nil }
func (h *SimpleIntegrationDataHandler) CommitTransaction() error   { return nil }
func (h *SimpleIntegrationDataHandler) RollbackTransaction() error { return nil }
func (h *SimpleIntegrationDataHandler) GetParams() *lib.DeSoParams { return &lib.DeSoParams{} }

func (h *SimpleIntegrationDataHandler) GetProcessedEntryCount() int { return len(h.processedEntries) }
func (h *SimpleIntegrationDataHandler) GetBatchCount() int          { return h.batchCount }
func (h *SimpleIntegrationDataHandler) GetEvents() []SyncEvent      { return h.events }

// TestSimpleEndToEndFlow tests the basic end-to-end flow without complex goroutines
func TestSimpleEndToEndFlow(t *testing.T) {
	require := require.New(t)

	// Setup test environment
	tempDir, err := os.MkdirTemp("", "simple-integration")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	stateDir := filepath.Join(tempDir, "state")
	progressDir := filepath.Join(tempDir, "progress")

	err = os.MkdirAll(stateDir, 0755)
	require.NoError(err)
	err = os.MkdirAll(progressDir, 0755)
	require.NoError(err)

	handler := NewSimpleIntegrationDataHandler()

	// Test 1: Hypersync Processing
	t.Log("=== Testing Hypersync Processing ===")
	testHypersyncProcessing(t, stateDir, progressDir, handler)

	// Test 2: Committed Block Processing
	t.Log("=== Testing Committed Block Processing ===")
	testCommittedBlockProcessing(t, stateDir, progressDir, handler)

	// Test 3: Mempool Processing
	t.Log("=== Testing Mempool Processing ===")
	testMempoolProcessing(t, stateDir, progressDir, handler)

	// Test 4: File Management
	t.Log("=== Testing File Management ===")
	testFileManagement(t, stateDir, progressDir)

	t.Logf("Integration test completed successfully!")
	t.Logf("Total entries processed: %d", handler.GetProcessedEntryCount())
	t.Logf("Total batches: %d", handler.GetBatchCount())
	t.Logf("Events emitted: %d", len(handler.GetEvents()))
}

// testHypersyncProcessing tests hypersync chunk processing
func testHypersyncProcessing(t *testing.T, stateDir, progressDir string, handler *SimpleIntegrationDataHandler) {
	require := require.New(t)

	// Create hypersync chunk files
	createTestHypersyncFiles(t, stateDir)

	// Initialize components
	fm, err := NewFileManager(stateDir)
	require.NoError(err)

	// Discover files
	allFiles, err := fm.DiscoverAllFiles()
	require.NoError(err)
	require.Greater(len(allFiles), 0, "Should discover hypersync files")

	// Check for hypersync files
	hypersyncFiles := fm.GetFilesByType(allFiles, FileTypeHypersyncChunk)
	require.Greater(len(hypersyncFiles), 0, "Should have hypersync chunk files")

	// Process a single hypersync chunk manually by reading the file
	chunkFile := hypersyncFiles[0]
	entriesProcessed, err := readAndProcessChunkFile(t, chunkFile.Path, handler)
	require.NoError(err)
	require.Greater(entriesProcessed, 0, "Should process entries from hypersync chunk")

	t.Logf("Processed hypersync chunk: %d entries", entriesProcessed)
}

// testCommittedBlockProcessing tests committed block diff processing
func testCommittedBlockProcessing(t *testing.T, stateDir, progressDir string, handler *SimpleIntegrationDataHandler) {
	require := require.New(t)

	// Create committed block files
	createTestCommittedBlockFiles(t, stateDir)

	// Initialize components
	fm, err := NewFileManager(stateDir)
	require.NoError(err)

	pm, err := NewProgressManager(progressDir)
	require.NoError(err)

	am := NewAncestralRecordManager(progressDir, handler)

	// Create committed block processor
	committedBlockProcessor := NewCommittedBlockProcessor(fm, pm, am, handler, 5)

	// Process a committed block manually
	result, err := committedBlockProcessor.ProcessNextCommittedBlock()
	require.NoError(err)

	if result != nil {
		require.Greater(result.EntriesProcessed, 0, "Should process entries from committed block")
		t.Logf("Processed committed block %d: %d entries", result.BlockHeight, result.EntriesProcessed)
	}
}

// testMempoolProcessing tests mempool diff processing
func testMempoolProcessing(t *testing.T, stateDir, progressDir string, handler *SimpleIntegrationDataHandler) {
	require := require.New(t)

	// Create mempool files
	createTestMempoolFiles(t, stateDir)

	// Initialize components
	fm, err := NewFileManager(stateDir)
	require.NoError(err)

	pm, err := NewProgressManager(progressDir)
	require.NoError(err)

	// Set mempool block height so files can be discovered
	err = pm.UpdateCommittedBlockProgress(101)
	require.NoError(err)

	am := NewAncestralRecordManager(progressDir, handler)

	// Create mempool processor
	mempoolProcessor := NewMempoolProcessor(fm, pm, am, handler, 5)

	// Process a mempool file manually
	result, err := mempoolProcessor.ProcessNextMempoolFile()
	require.NoError(err)

	if result != nil {
		require.Greater(result.EntriesProcessed, 0, "Should process entries from mempool file")
		t.Logf("Processed mempool file: %d entries", result.EntriesProcessed)
	}
}

// testFileManagement tests file discovery and classification
func testFileManagement(t *testing.T, stateDir, progressDir string) {
	require := require.New(t)

	fm, err := NewFileManager(stateDir)
	require.NoError(err)

	// Discover all files
	allFiles, err := fm.DiscoverAllFiles()
	require.NoError(err)
	require.Greater(len(allFiles), 0, "Should discover files")

	// Test file type classification
	hypersyncFiles := fm.GetFilesByType(allFiles, FileTypeHypersyncChunk)
	committedFiles := fm.GetFilesByType(allFiles, FileTypeCommittedBlock)
	mempoolFiles := fm.GetFilesByType(allFiles, FileTypeMempoolDiff)

	t.Logf("File discovery results:")
	t.Logf("  Hypersync chunks: %d", len(hypersyncFiles))
	t.Logf("  Committed blocks: %d", len(committedFiles))
	t.Logf("  Mempool files: %d", len(mempoolFiles))

	require.Greater(len(hypersyncFiles), 0, "Should find hypersync files")
	require.Greater(len(committedFiles), 0, "Should find committed block files")
	require.Greater(len(mempoolFiles), 0, "Should find mempool files")

	// Test file parsing
	for _, file := range allFiles {
		require.NotEmpty(file.Path, "File should have path")
		require.Greater(file.BlockHeight, uint64(0), "File should have block height")
		if file.Type == FileTypeHypersyncChunk {
			require.NotNil(file.ChunkId, "Hypersync file should have chunk ID")
		}
	}
}

// Helper functions

// readAndProcessChunkFile manually reads and processes a chunk file
func readAndProcessChunkFile(t *testing.T, filePath string, handler *SimpleIntegrationDataHandler) (int, error) {
	require := require.New(t)

	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Read badger backup format
	var length uint32
	err = binary.Read(file, binary.LittleEndian, &length)
	require.NoError(err)

	var crc uint32
	err = binary.Read(file, binary.LittleEndian, &crc)
	require.NoError(err)

	data := make([]byte, length)
	_, err = file.Read(data)
	require.NoError(err)

	// Parse KVList
	var kvList pb.KVList
	err = proto.Unmarshal(data, &kvList)
	require.NoError(err)

	// Convert to state change entries and process
	var entries []*lib.StateChangeEntry
	for _, kv := range kvList.Kv {
		entry := &lib.StateChangeEntry{
			OperationType: lib.DbOperationTypeUpsert,
			KeyBytes:      kv.Key,
			EncoderBytes:  kv.Value,
			BlockHeight:   100,
		}
		entries = append(entries, entry)
	}

	// Process the batch
	err = handler.HandleEntryBatch(entries, false)
	require.NoError(err)

	return len(entries), nil
}

// Helper functions to create test files

func createTestHypersyncFiles(t *testing.T, stateDir string) {
	entries := []testSimpleEntry{
		{key: []byte{3, 1, 1, 1}, value: []byte("profile_hypersync_1")},
		{key: []byte{5, 1, 1, 1}, value: []byte("post_hypersync_1")},
	}

	createSimpleBadgerFile(t, filepath.Join(stateDir, "hypersync_chunk_100_1_1640995200000000000.bin"), entries)

	entries2 := []testSimpleEntry{
		{key: []byte{3, 1, 1, 2}, value: []byte("profile_hypersync_2")},
		{key: []byte{5, 1, 1, 2}, value: []byte("post_hypersync_2")},
	}
	createSimpleBadgerFile(t, filepath.Join(stateDir, "hypersync_chunk_100_2_1640995200000000000.bin"), entries2)
}

func createTestCommittedBlockFiles(t *testing.T, stateDir string) {
	entries := []testSimpleEntry{
		{key: []byte{3, 1, 2, 1}, value: []byte("profile_block_101")},
		{key: []byte{5, 1, 2, 1}, value: []byte("post_block_101")},
		{key: []byte{7, 1, 2, 1}, value: []byte("like_block_101")},
	}

	createSimpleBadgerFile(t, filepath.Join(stateDir, "state_changes_101.bin"), entries)
}

func createTestMempoolFiles(t *testing.T, stateDir string) {
	entries := []testSimpleEntry{
		{key: []byte{5, 1, 3, 1}, value: []byte("mempool_post_1")},
		{key: []byte{7, 1, 3, 1}, value: []byte("mempool_like_1")},
	}

	createSimpleBadgerFile(t, filepath.Join(stateDir, "mempool_101_1640995300000000000.bin"), entries)

	entries2 := []testSimpleEntry{
		{key: []byte{5, 1, 3, 2}, value: []byte("mempool_post_2")},
		{key: []byte{3, 1, 3, 1}, value: []byte("mempool_profile_1")},
	}
	createSimpleBadgerFile(t, filepath.Join(stateDir, "mempool_101_1640995301000000000.bin"), entries2)
}

type testSimpleEntry struct {
	key   []byte
	value []byte
}

func createSimpleBadgerFile(t *testing.T, filePath string, entries []testSimpleEntry) {
	require := require.New(t)

	// Convert to protobuf KV format
	var kvs []*pb.KV
	for _, entry := range entries {
		kvs = append(kvs, &pb.KV{Key: entry.key, Value: entry.value})
	}

	kvList := &pb.KVList{Kv: kvs}
	kvBytes, err := proto.Marshal(kvList)
	require.NoError(err)

	// Write badger backup format
	file, err := os.Create(filePath)
	require.NoError(err)
	defer file.Close()

	// Length + CRC + Data
	binary.Write(file, binary.LittleEndian, uint32(len(kvBytes)))
	binary.Write(file, binary.LittleEndian, uint32(0)) // CRC placeholder
	file.Write(kvBytes)
}

// TestComponentIntegration tests that all components work together
func TestComponentIntegration(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "component-integration")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	stateDir := filepath.Join(tempDir, "state")
	progressDir := filepath.Join(tempDir, "progress")

	err = os.MkdirAll(stateDir, 0755)
	require.NoError(err)
	err = os.MkdirAll(progressDir, 0755)
	require.NoError(err)

	// Create test files
	createTestHypersyncFiles(t, stateDir)
	createTestCommittedBlockFiles(t, stateDir)
	createTestMempoolFiles(t, stateDir)

	handler := NewSimpleIntegrationDataHandler()

	// Test FileProcessor creation and basic functionality
	config := FileProcessorConfig{
		StateChangeDir:      stateDir,
		ProgressDir:         progressDir,
		MaxConcurrentChunks: 1,
		BatchSize:           5,
	}

	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)
	require.NotNil(processor)

	// Test mode determination
	mode, err := processor.determineProcessingMode()
	require.NoError(err)
	// Mode should be one of the valid modes (hypersync, committed blocks, or mempool)
	require.True(mode == ModeHypersync || mode == ModeCommittedBlocks || mode == ModeMempool,
		"Should determine a valid processing mode, got: %v", mode)

	// Test state access
	currentState := processor.GetCurrentState()
	require.NotNil(currentState)

	// Test stats
	stats := processor.GetProcessingStats()
	require.NotNil(stats)

	// Test managers access
	require.NotNil(processor.GetFileManager())
	require.NotNil(processor.GetProgressManager())

	t.Logf("Component integration test passed!")
	t.Logf("Initial processing mode: %v", mode)
	t.Logf("Initial state: hypersync_complete=%v, last_block=%d",
		currentState.HypersyncComplete, currentState.LastCommittedBlockHeight)
}

// TestProcessorStates tests state transitions and persistence
func TestProcessorStates(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "state-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	progressDir := filepath.Join(tempDir, "progress")
	err = os.MkdirAll(progressDir, 0755)
	require.NoError(err)

	// Test ProgressManager state management
	pm, err := NewProgressManager(progressDir)
	require.NoError(err)

	// Test initial state
	initialState := pm.GetCurrentState()
	require.Equal(ModeHypersync, initialState.Mode)
	require.False(initialState.HypersyncComplete)

	// Test state updates
	err = pm.UpdateHypersyncProgress(5)
	require.NoError(err)

	err = pm.MarkHypersyncComplete()
	require.NoError(err)

	err = pm.UpdateCommittedBlockProgress(100)
	require.NoError(err)

	err = pm.UpdateMempoolProgress("test_file.bin", 1640995200000000000)
	require.NoError(err)

	// Verify state changes
	finalState := pm.GetCurrentState()
	require.True(finalState.HypersyncComplete)
	require.Equal(ModeCommittedBlocks, finalState.Mode)
	require.Equal(uint64(100), finalState.LastCommittedBlockHeight)
	require.Contains(finalState.AppliedMempoolFiles, "test_file.bin")

	// Test persistence by creating new manager
	pm2, err := NewProgressManager(progressDir)
	require.NoError(err)

	loadedState := pm2.GetCurrentState()
	require.True(loadedState.HypersyncComplete)
	require.Equal(uint64(100), loadedState.LastCommittedBlockHeight)

	t.Logf("State management test passed!")
}

// TestErrorHandling tests error scenarios
func TestErrorHandling(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "error-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	// Test invalid file operations (try to process a non-existent file)
	handler := NewSimpleIntegrationDataHandler()
	am := NewAncestralRecordManager(tempDir, handler)

	nonExistentFiles := []*FileInfo{{Path: "/nonexistent/file.bin"}}
	err = am.RevertMempoolChangesFromFiles(nonExistentFiles)
	require.Error(err, "Should fail when trying to read non-existent ancestral files")

	// Test FileManager with invalid directory during file discovery
	var fm *FileManager
	fm, err = NewFileManager("/nonexistent/directory")
	require.NoError(err) // Creation succeeds, but discovery should fail

	_, err = fm.DiscoverAllFiles()
	require.Error(err, "Should fail when trying to discover files in non-existent directory")

	// Test empty state directory
	emptyDir := filepath.Join(tempDir, "empty")
	err = os.MkdirAll(emptyDir, 0755)
	require.NoError(err)

	fm, err = NewFileManager(emptyDir)
	require.NoError(err)

	files, err := fm.DiscoverAllFiles()
	require.NoError(err)
	require.Len(files, 0, "Should find no files in empty directory")

	t.Logf("Error handling test passed!")
}
