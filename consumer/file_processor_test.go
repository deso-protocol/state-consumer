package consumer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewFileProcessor(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 4,
		BatchSize:           100,
	}

	handler := NewMockDataHandler()

	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)
	require.NotNil(processor)
	require.NotNil(processor.fileManager)
	require.NotNil(processor.progressManager)
	require.NotNil(processor.hypersyncProcessor)
	require.Equal(4, processor.maxConcurrentChunks)
	require.Equal(uint64(100), processor.batchSize)
	require.False(processor.isRunning)
}

func TestFileProcessor_DetermineProcessingMode(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 4,
		BatchSize:           100,
	}

	handler := NewMockDataHandler()
	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)

	// Test default mode with no files
	mode, err := processor.determineProcessingMode()
	require.NoError(err)
	require.Equal(ModeMempool, mode) // Should default to mempool when no files (hypersync auto-completes)

	// Create a hypersync chunk file and reset state for clean test
	chunkFile := filepath.Join(tempDir, "hypersync_chunk_100_1_1640995200000000000.bin")
	createTestChunkFile(t, chunkFile, 1)

	// Create a fresh processor to test hypersync mode detection
	processor2, err := NewFileProcessor(config, handler)
	require.NoError(err)

	mode, err = processor2.determineProcessingMode()
	require.NoError(err)
	require.Equal(ModeHypersync, mode)

	// Mark hypersync as complete and add committed block file
	err = processor.progressManager.MarkHypersyncComplete()
	require.NoError(err)

	blockFile := filepath.Join(tempDir, "state_changes_100.bin")
	err = os.WriteFile(blockFile, []byte("test"), 0644)
	require.NoError(err)

	mode, err = processor.determineProcessingMode()
	require.NoError(err)
	require.Equal(ModeCommittedBlocks, mode)
}

func TestFileProcessor_StartStop(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 4,
		BatchSize:           100,
	}

	handler := NewMockDataHandler()
	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)

	// Test starting without files (should start in mempool mode)
	err = processor.Start()
	require.NoError(err)
	require.True(processor.IsRunning())

	// Test getting stats
	stats := processor.GetProcessingStats()
	require.NotNil(stats)
	require.True(stats["is_running"].(bool))

	// Test stopping
	err = processor.Stop()
	require.NoError(err)
	require.False(processor.IsRunning())

	// Test stopping when already stopped
	err = processor.Stop()
	require.NoError(err)
}

func TestFileProcessor_HypersyncIntegration(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 2,
		BatchSize:           50,
	}

	handler := NewMockDataHandler()
	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)

	// Create a hypersync chunk file
	chunkFile := filepath.Join(tempDir, "hypersync_chunk_100_1_1640995200000000000.bin")
	createTestChunkFile(t, chunkFile, 5) // 5 entries

	// Start processor (should detect hypersync mode)
	err = processor.Start()
	require.NoError(err)
	require.True(processor.IsRunning())

	// Wait a bit for processing
	time.Sleep(200 * time.Millisecond)

	// Check stats
	stats := processor.GetProcessingStats()
	require.NotNil(stats)
	require.Equal("hypersync", stats["mode"])

	// Verify hypersync processor started
	hypersyncStats, ok := stats["hypersync"].(map[string]interface{})
	require.True(ok)
	require.Equal("hypersync", hypersyncStats["mode"])

	// Stop processor
	err = processor.Stop()
	require.NoError(err)
}

func TestFileProcessor_GetCurrentState(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 4,
		BatchSize:           100,
	}

	handler := NewMockDataHandler()
	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)

	// Test initial state
	state := processor.GetCurrentState()
	require.NotNil(state)
	require.Equal(ModeHypersync, state.Mode) // Default mode (before any processing mode detection)
	require.False(state.HypersyncComplete)

	// Update state
	err = processor.progressManager.MarkHypersyncComplete()
	require.NoError(err)

	state = processor.GetCurrentState()
	require.True(state.HypersyncComplete)
}

func TestFileProcessor_GetManagers(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 4,
		BatchSize:           100,
	}

	handler := NewMockDataHandler()
	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)

	// Test getting managers
	fileManager := processor.GetFileManager()
	require.NotNil(fileManager)
	require.Equal(tempDir, fileManager.stateChangeDir)

	progressManager := processor.GetProgressManager()
	require.NotNil(progressManager)
	require.Equal(tempDir, progressManager.progressDir)
}

func TestFileProcessor_MultipleStartError(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-processor-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	config := FileProcessorConfig{
		StateChangeDir:      tempDir,
		ProgressDir:         tempDir,
		MaxConcurrentChunks: 4,
		BatchSize:           100,
	}

	handler := NewMockDataHandler()
	processor, err := NewFileProcessor(config, handler)
	require.NoError(err)

	// Start processor
	err = processor.Start()
	require.NoError(err)
	require.True(processor.IsRunning())

	// Try to start again - should error
	err = processor.Start()
	require.Error(err)
	require.Contains(err.Error(), "already running")

	// Stop processor
	err = processor.Stop()
	require.NoError(err)
}
