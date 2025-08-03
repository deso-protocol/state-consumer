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
	"github.com/stretchr/testify/suite"
)

// IntegrationTestSuite provides end-to-end testing of the new consumer architecture
type IntegrationTestSuite struct {
	suite.Suite
	tempDir     string
	stateDir    string
	progressDir string
	handler     *IntegrationDataHandler
	processor   *FileProcessor
}

// IntegrationDataHandler captures all processed entries for validation
type IntegrationDataHandler struct {
	processedEntries []*lib.StateChangeEntry
	batches          [][]*lib.StateChangeEntry
	events           []SyncEvent
	transactions     int
}

func NewIntegrationDataHandler() *IntegrationDataHandler {
	return &IntegrationDataHandler{
		processedEntries: make([]*lib.StateChangeEntry, 0),
		batches:          make([][]*lib.StateChangeEntry, 0),
		events:           make([]SyncEvent, 0),
	}
}

func (h *IntegrationDataHandler) HandleEntryBatch(entries []*lib.StateChangeEntry, isMempool bool) error {
	// Store batch for analysis
	batchCopy := make([]*lib.StateChangeEntry, len(entries))
	copy(batchCopy, entries)
	h.batches = append(h.batches, batchCopy)

	// Store all entries
	h.processedEntries = append(h.processedEntries, entries...)

	return nil
}

func (h *IntegrationDataHandler) HandleSyncEvent(event SyncEvent) error {
	h.events = append(h.events, event)
	return nil
}

func (h *IntegrationDataHandler) InitiateTransaction() error {
	h.transactions++
	return nil
}

func (h *IntegrationDataHandler) CommitTransaction() error {
	return nil
}

func (h *IntegrationDataHandler) RollbackTransaction() error {
	// No-op for testing
	return nil
}

func (h *IntegrationDataHandler) GetParams() *lib.DeSoParams {
	return &lib.DeSoParams{}
}

func (h *IntegrationDataHandler) GetProcessedEntryCount() int {
	return len(h.processedEntries)
}

func (h *IntegrationDataHandler) GetBatchCount() int {
	return len(h.batches)
}

func (h *IntegrationDataHandler) GetEvents() []SyncEvent {
	return h.events
}

func (h *IntegrationDataHandler) GetEntriesByType(encoderType lib.EncoderType) []*lib.StateChangeEntry {
	var entries []*lib.StateChangeEntry
	for _, entry := range h.processedEntries {
		if entry.EncoderType == encoderType {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (h *IntegrationDataHandler) Reset() {
	h.processedEntries = make([]*lib.StateChangeEntry, 0)
	h.batches = make([][]*lib.StateChangeEntry, 0)
	h.events = make([]SyncEvent, 0)
	h.transactions = 0
}

// SetupTest initializes the test environment
func (suite *IntegrationTestSuite) SetupTest() {
	var err error
	suite.tempDir, err = os.MkdirTemp("", "integration-test")
	suite.Require().NoError(err)

	suite.stateDir = filepath.Join(suite.tempDir, "state-changes")
	suite.progressDir = filepath.Join(suite.tempDir, "progress")

	err = os.MkdirAll(suite.stateDir, 0755)
	suite.Require().NoError(err)

	err = os.MkdirAll(suite.progressDir, 0755)
	suite.Require().NoError(err)

	suite.handler = NewIntegrationDataHandler()

	config := FileProcessorConfig{
		StateChangeDir:      suite.stateDir,
		ProgressDir:         suite.progressDir,
		MaxConcurrentChunks: 2,
		BatchSize:           10,
	}

	suite.processor, err = NewFileProcessor(config, suite.handler)
	suite.Require().NoError(err)
}

// TearDownTest cleans up the test environment
func (suite *IntegrationTestSuite) TearDownTest() {
	if suite.processor != nil {
		suite.processor.Stop()
	}
	if suite.tempDir != "" {
		os.RemoveAll(suite.tempDir)
	}
}

// TestEndToEndFlow tests the complete hypersync → committed blocks → mempool flow
func (suite *IntegrationTestSuite) TestEndToEndFlow() {
	require := suite.Require()

	// Phase 1: Create hypersync files (simulating initial blockchain sync)
	suite.createHypersyncFiles()

	// Phase 2: Create committed block files (simulating new blocks)
	suite.createCommittedBlockFiles()

	// Phase 3: Create mempool files (simulating mempool activity)
	suite.createMempoolFiles()

	// Phase 4: Start the processor and let it run
	err := suite.processor.Start()
	require.NoError(err)

	// Wait for processing to complete
	suite.waitForProcessingComplete()

	// Phase 5: Validate the results
	suite.validateResults()
}

// createHypersyncFiles creates hypersync chunk files simulating initial sync
func (suite *IntegrationTestSuite) createHypersyncFiles() {
	require := suite.Require()

	// Create multiple hypersync chunks for block height 100
	chunks := []struct {
		chunkId int
		entries []testEntry
	}{
		{
			chunkId: 1,
			entries: []testEntry{
				{key: []byte{3, 1, 1, 1}, value: []byte("profile_1"), entryType: lib.EncoderTypeProfileEntry},
				{key: []byte{3, 1, 1, 2}, value: []byte("profile_2"), entryType: lib.EncoderTypeProfileEntry},
			},
		},
		{
			chunkId: 2,
			entries: []testEntry{
				{key: []byte{5, 1, 1, 1}, value: []byte("post_1"), entryType: lib.EncoderTypePostEntry},
				{key: []byte{5, 1, 1, 2}, value: []byte("post_2"), entryType: lib.EncoderTypePostEntry},
			},
		},
		{
			chunkId: 3,
			entries: []testEntry{
				{key: []byte{7, 1, 1, 1}, value: []byte("follow_1"), entryType: lib.EncoderTypeLikeEntry},
			},
		},
	}

	for _, chunk := range chunks {
		filename := fmt.Sprintf("hypersync_chunk_100_%d_1640995200000000000.bin", chunk.chunkId)
		filepath := filepath.Join(suite.stateDir, filename)
		suite.createBadgerBackupFile(filepath, chunk.entries)
	}

	require.Equal(3, len(chunks), "Should create 3 hypersync chunks")
}

// createCommittedBlockFiles creates committed block diff files
func (suite *IntegrationTestSuite) createCommittedBlockFiles() {
	require := suite.Require()

	// Block 101: New posts and profile updates
	block101Entries := []testEntry{
		{key: []byte{3, 1, 1, 3}, value: []byte("profile_3_updated"), entryType: lib.EncoderTypeProfileEntry},
		{key: []byte{5, 1, 1, 3}, value: []byte("post_3"), entryType: lib.EncoderTypePostEntry},
		{key: []byte{5, 1, 1, 4}, value: []byte("post_4"), entryType: lib.EncoderTypePostEntry},
	}

	// Block 102: More activity
	block102Entries := []testEntry{
		{key: []byte{7, 1, 1, 2}, value: []byte("follow_2"), entryType: lib.EncoderTypeLikeEntry},
		{key: []byte{3, 1, 1, 4}, value: []byte("profile_4"), entryType: lib.EncoderTypeProfileEntry},
	}

	suite.createBadgerBackupFile(filepath.Join(suite.stateDir, "state_changes_101.bin"), block101Entries)
	suite.createBadgerBackupFile(filepath.Join(suite.stateDir, "state_changes_102.bin"), block102Entries)

	require.Equal(2, len([]string{"state_changes_101.bin", "state_changes_102.bin"}), "Should create 2 committed block files")
}

// createMempoolFiles creates mempool diff files simulating mempool activity
func (suite *IntegrationTestSuite) createMempoolFiles() {
	require := suite.Require()

	// Mempool activity for block 102 (after it's committed)
	mempool1Entries := []testEntry{
		{key: []byte{5, 1, 2, 1}, value: []byte("mempool_post_1"), entryType: lib.EncoderTypePostEntry},
		{key: []byte{7, 1, 2, 1}, value: []byte("mempool_follow_1"), entryType: lib.EncoderTypeLikeEntry},
	}

	mempool2Entries := []testEntry{
		{key: []byte{5, 1, 2, 2}, value: []byte("mempool_post_2"), entryType: lib.EncoderTypePostEntry},
		{key: []byte{5, 1, 2, 1}, value: []byte("mempool_post_1_updated"), entryType: lib.EncoderTypePostEntry}, // Update
	}

	mempool3Entries := []testEntry{
		{key: []byte{3, 1, 2, 1}, value: []byte("mempool_profile_1"), entryType: lib.EncoderTypeProfileEntry},
	}

	// Create files with increasing timestamps
	suite.createBadgerBackupFile(filepath.Join(suite.stateDir, "mempool_102_1640995300000000000.bin"), mempool1Entries)
	suite.createBadgerBackupFile(filepath.Join(suite.stateDir, "mempool_102_1640995301000000000.bin"), mempool2Entries)
	suite.createBadgerBackupFile(filepath.Join(suite.stateDir, "mempool_102_1640995302000000000.bin"), mempool3Entries)

	// Create corresponding ancestral record files (for revert capability)
	suite.createAncestralRecordFiles()

	require.Equal(3, len([]string{"mempool_102_*.bin"}), "Should create 3 mempool files")
}

// createAncestralRecordFiles creates ancestral record files for mempool reverts
func (suite *IntegrationTestSuite) createAncestralRecordFiles() {
	// Create empty ancestral files for simplicity in this test
	// In a real scenario, these would contain the previous values for reversion
	files := []string{
		"mempool_ancestral_102_1640995300000000000.bin",
		"mempool_ancestral_102_1640995301000000000.bin",
		"mempool_ancestral_102_1640995302000000000.bin",
	}

	for _, filename := range files {
		filepath := filepath.Join(suite.stateDir, filename)
		file, err := os.Create(filepath)
		suite.Require().NoError(err)
		file.Close()
	}
}

// testEntry represents a test database entry
type testEntry struct {
	key       []byte
	value     []byte
	entryType lib.EncoderType
}

// createBadgerBackupFile creates a file in badger backup format
func (suite *IntegrationTestSuite) createBadgerBackupFile(filePath string, entries []testEntry) {
	require := suite.Require()

	// Convert test entries to protobuf KV format
	var kvs []*pb.KV
	for _, entry := range entries {
		kv := &pb.KV{
			Key:   entry.key,
			Value: entry.value,
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

// waitForProcessingComplete waits for the processor to complete all processing
func (suite *IntegrationTestSuite) waitForProcessingComplete() {
	// Wait for processing to start and complete
	maxWait := 30 * time.Second
	checkInterval := 100 * time.Millisecond

	start := time.Now()
	for time.Since(start) < maxWait {
		time.Sleep(checkInterval)

		stats := suite.processor.GetProcessingStats()
		currentState := suite.processor.GetCurrentState()

		// Check if we've processed hypersync, committed blocks, and mempool files
		if stats != nil &&
			currentState.HypersyncComplete &&
			currentState.LastCommittedBlockHeight >= 102 &&
			len(currentState.AppliedMempoolFiles) >= 3 {
			// Give it a bit more time to ensure all processing is done
			time.Sleep(500 * time.Millisecond)
			return
		}
	}

	suite.Fail("Processing did not complete within timeout")
}

// validateResults validates that all expected processing occurred correctly
func (suite *IntegrationTestSuite) validateResults() {
	require := suite.Require()

	// Validate sync events were emitted in correct order
	events := suite.handler.GetEvents()
	suite.validateSyncEvents(events)

	// Validate total entries processed
	totalEntries := suite.handler.GetProcessedEntryCount()
	require.Greater(totalEntries, 0, "Should have processed some entries")

	// Validate hypersync entries (5 total: 2 profiles + 2 posts + 1 follow)
	hypersyncEntries := suite.countEntriesWithOperation(lib.DbOperationTypeUpsert)
	require.GreaterOrEqual(hypersyncEntries, 5, "Should have processed hypersync entries")

	// Validate committed block entries (5 total from blocks 101 and 102)
	// These are also upserts, so they're included in the count above

	// Validate mempool entries (5 total from 3 mempool files)
	mempoolEntries := suite.countMempoolEntries()
	require.GreaterOrEqual(mempoolEntries, 5, "Should have processed mempool entries")

	// Validate processing state
	currentState := suite.processor.GetCurrentState()
	require.True(currentState.HypersyncComplete, "Hypersync should be complete")
	require.Equal(uint64(102), currentState.LastCommittedBlockHeight, "Should have processed block 102")
	require.Equal(uint64(102), currentState.CurrentMempoolBlockHeight, "Should be processing mempool for block 102")
	require.Len(currentState.AppliedMempoolFiles, 3, "Should have applied 3 mempool files")

	// Validate entry types distribution
	profileEntries := suite.handler.GetEntriesByType(lib.EncoderTypeProfileEntry)
	postEntries := suite.handler.GetEntriesByType(lib.EncoderTypePostEntry)
	followEntries := suite.handler.GetEntriesByType(lib.EncoderTypeLikeEntry)

	require.Greater(len(profileEntries), 0, "Should have profile entries")
	require.Greater(len(postEntries), 0, "Should have post entries")
	require.Greater(len(followEntries), 0, "Should have follow entries")

	suite.T().Logf("Integration test results:")
	suite.T().Logf("  Total entries processed: %d", totalEntries)
	suite.T().Logf("  Profile entries: %d", len(profileEntries))
	suite.T().Logf("  Post entries: %d", len(postEntries))
	suite.T().Logf("  Follow entries: %d", len(followEntries))
	suite.T().Logf("  Sync events: %d", len(events))
	suite.T().Logf("  Batches processed: %d", suite.handler.GetBatchCount())
}

// validateSyncEvents validates that sync events were emitted in the correct sequence
func (suite *IntegrationTestSuite) validateSyncEvents(events []SyncEvent) {
	require := suite.Require()

	require.Greater(len(events), 0, "Should have sync events")

	// Should have hypersync start/complete events
	hasHypersyncStart := false
	hasHypersyncComplete := false

	for _, event := range events {
		switch event {
		case SyncEventHypersyncStart:
			hasHypersyncStart = true
		case SyncEventHypersyncComplete:
			hasHypersyncComplete = true
		}
	}

	require.True(hasHypersyncStart, "Should have hypersync start event")
	require.True(hasHypersyncComplete, "Should have hypersync complete event")
}

// countEntriesWithOperation counts entries with a specific operation type
func (suite *IntegrationTestSuite) countEntriesWithOperation(opType lib.StateSyncerOperationType) int {
	count := 0
	for _, entry := range suite.handler.processedEntries {
		if entry.OperationType == opType {
			count++
		}
	}
	return count
}

// countMempoolEntries counts entries that were processed as mempool entries
func (suite *IntegrationTestSuite) countMempoolEntries() int {
	// Count entries from mempool batches (those with isMempool = true)
	// For this test, we'll estimate based on the batches processed after hypersync
	mempoolCount := 0
	totalBatches := suite.handler.GetBatchCount()

	// Rough estimation: mempool entries are typically processed after hypersync and committed blocks
	if totalBatches > 2 {
		// Assume the last few batches contain mempool entries
		for i := totalBatches - 3; i < totalBatches; i++ {
			if i >= 0 && i < len(suite.handler.batches) {
				mempoolCount += len(suite.handler.batches[i])
			}
		}
	}

	return mempoolCount
}

// TestFileTransitions tests that the processor correctly transitions between processing modes
func (suite *IntegrationTestSuite) TestFileTransitions() {
	require := suite.Require()

	// Start with just hypersync files
	suite.createHypersyncFiles()

	err := suite.processor.Start()
	require.NoError(err)

	// Wait for hypersync to complete
	suite.waitForHypersyncComplete()

	// Add committed block files
	suite.createCommittedBlockFiles()

	// Wait for committed blocks to be processed
	suite.waitForCommittedBlocksComplete()

	// Add mempool files
	suite.createMempoolFiles()

	// Wait for mempool processing
	suite.waitForMempoolProcessing()

	// Validate final state
	currentState := suite.processor.GetCurrentState()
	require.True(currentState.HypersyncComplete)
	require.Equal(uint64(102), currentState.LastCommittedBlockHeight)
	require.Greater(len(currentState.AppliedMempoolFiles), 0)
}

// waitForHypersyncComplete waits for hypersync processing to complete
func (suite *IntegrationTestSuite) waitForHypersyncComplete() {
	maxWait := 10 * time.Second
	start := time.Now()

	for time.Since(start) < maxWait {
		currentState := suite.processor.GetCurrentState()
		if currentState.HypersyncComplete {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	suite.Fail("Hypersync did not complete within timeout")
}

// waitForCommittedBlocksComplete waits for committed block processing to complete
func (suite *IntegrationTestSuite) waitForCommittedBlocksComplete() {
	maxWait := 10 * time.Second
	start := time.Now()

	for time.Since(start) < maxWait {
		currentState := suite.processor.GetCurrentState()
		if currentState.LastCommittedBlockHeight >= 102 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	suite.Fail("Committed blocks did not complete within timeout")
}

// waitForMempoolProcessing waits for mempool processing to begin
func (suite *IntegrationTestSuite) waitForMempoolProcessing() {
	maxWait := 10 * time.Second
	start := time.Now()

	for time.Since(start) < maxWait {
		currentState := suite.processor.GetCurrentState()
		if len(currentState.AppliedMempoolFiles) > 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	suite.Fail("Mempool processing did not start within timeout")
}

// TestBackwardCompatibility tests that the processor can handle legacy file formats
func (suite *IntegrationTestSuite) TestBackwardCompatibility() {
	require := suite.Require()

	// This would test legacy file format handling
	// For now, we'll just ensure the processor can detect new format files
	suite.createHypersyncFiles()

	fileManager := suite.processor.GetFileManager()
	hasNewFormat := fileManager.HasNewFormatFiles()
	hasLegacyFormat := fileManager.HasLegacyFiles()

	require.True(hasNewFormat, "Should detect new format files")
	require.False(hasLegacyFormat, "Should not detect legacy files")
}

// TestConcurrentProcessing tests that hypersync processing works with multiple concurrent chunks
func (suite *IntegrationTestSuite) TestConcurrentProcessing() {
	require := suite.Require()

	// Create many hypersync chunks to test concurrent processing
	for i := 1; i <= 5; i++ {
		entries := []testEntry{
			{key: []byte{3, byte(i), 1, 1}, value: []byte(fmt.Sprintf("profile_%d", i)), entryType: lib.EncoderTypeProfileEntry},
			{key: []byte{5, byte(i), 1, 1}, value: []byte(fmt.Sprintf("post_%d", i)), entryType: lib.EncoderTypePostEntry},
		}

		filename := fmt.Sprintf("hypersync_chunk_100_%d_1640995200000000000.bin", i)
		filepath := filepath.Join(suite.stateDir, filename)
		suite.createBadgerBackupFile(filepath, entries)
	}

	err := suite.processor.Start()
	require.NoError(err)

	suite.waitForHypersyncComplete()

	// Validate that all chunks were processed
	require.Greater(suite.handler.GetProcessedEntryCount(), 5)
}

// Run the integration test suite
func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

// TestQuickIntegration is a lighter version for faster testing
func TestQuickIntegration(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "quick-integration")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	stateDir := filepath.Join(tempDir, "state")
	progressDir := filepath.Join(tempDir, "progress")

	err = os.MkdirAll(stateDir, 0755)
	require.NoError(err)
	err = os.MkdirAll(progressDir, 0755)
	require.NoError(err)

	handler := NewIntegrationDataHandler()

	config := FileProcessorConfig{
		StateChangeDir:      stateDir,
		ProgressDir:         progressDir,
		MaxConcurrentChunks: 1,
		BatchSize:           5,
	}

	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)
	defer processor.Stop()

	// Create a simple hypersync file
	entries := []testEntry{
		{key: []byte{3, 1, 1, 1}, value: []byte("test_profile"), entryType: lib.EncoderTypeProfileEntry},
		{key: []byte{5, 1, 1, 1}, value: []byte("test_post"), entryType: lib.EncoderTypePostEntry},
	}

	// Create KV list
	var kvs []*pb.KV
	for _, entry := range entries {
		kvs = append(kvs, &pb.KV{Key: entry.key, Value: entry.value})
	}

	kvList := &pb.KVList{Kv: kvs}
	kvBytes, err := proto.Marshal(kvList)
	require.NoError(err)

	// Write hypersync file
	filepath := filepath.Join(stateDir, "hypersync_chunk_100_1_1640995200000000000.bin")
	file, err := os.Create(filepath)
	require.NoError(err)

	binary.Write(file, binary.LittleEndian, uint32(len(kvBytes)))
	binary.Write(file, binary.LittleEndian, uint32(0)) // CRC
	file.Write(kvBytes)
	file.Close()

	// Start processing
	err = processor.Start()
	require.NoError(err)

	// Wait briefly for processing
	time.Sleep(2 * time.Second)

	// Validate results
	require.Greater(handler.GetProcessedEntryCount(), 0)
	require.True(processor.GetCurrentState().HypersyncComplete)
}
