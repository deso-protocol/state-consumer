package consumer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConsumerIntegrationWithNewArchitecture demonstrates how the existing consumer
// automatically detects and uses the new FileProcessor architecture
func TestConsumerIntegrationWithNewArchitecture(t *testing.T) {
	require := require.New(t)

	// Setup test environment
	tempDir, err := os.MkdirTemp("", "consumer-integration")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	stateDir := filepath.Join(tempDir, "state")
	progressDir := filepath.Join(tempDir, "progress")

	err = os.MkdirAll(stateDir, 0755)
	require.NoError(err)
	err = os.MkdirAll(progressDir, 0755)
	require.NoError(err)

	// Create some new format files to trigger new architecture
	createTestHypersyncFiles(t, stateDir)

	// Create a mock data handler
	handler := NewSimpleIntegrationDataHandler()

	// Create the consumer (existing interface)
	consumer := &StateSyncerConsumer{}

	// Test architecture detection
	useNew, err := consumer.shouldUseNewArchitecture(stateDir)
	require.NoError(err)
	require.True(useNew, "Should detect new architecture due to hypersync files")

	// Test the new architecture setup (without starting the full processing to avoid goroutine issues)
	err = consumer.runWithNewArchitecture(
		stateDir,    // stateChangeDir
		progressDir, // consumerProgressDir
		1000,        // batchBytes
		2,           // threadLimit
		true,        // syncMempool
		handler,     // handler
	)
	require.NoError(err)

	// Verify the FileProcessor was created
	require.NotNil(consumer.FileProcessor, "FileProcessor should be created for new architecture")

	// Verify we can get status from the new architecture
	stats := consumer.FileProcessor.GetProcessingStats()
	require.NotNil(stats, "Should be able to get processing stats")

	currentState := consumer.FileProcessor.GetCurrentState()
	require.NotNil(currentState, "Should be able to get current state")

	// Test the managers
	require.NotNil(consumer.FileProcessor.GetFileManager(), "Should have FileManager")
	require.NotNil(consumer.FileProcessor.GetProgressManager(), "Should have ProgressManager")

	// Test stopping the consumer
	consumer.Stop()

	t.Logf("Consumer integration test passed!")
	t.Logf("Architecture: New FileProcessor")
	t.Logf("Initial mode: %v", currentState.Mode)
	t.Logf("FileProcessor created successfully with all components")
}

// TestConsumerIntegrationWithLegacyArchitecture demonstrates how the existing consumer
// falls back to legacy processing when no new format files are found
func TestConsumerIntegrationWithLegacyArchitecture(t *testing.T) {
	require := require.New(t)

	// Setup test environment with NO files (should default to new architecture)
	tempDir, err := os.MkdirTemp("", "consumer-legacy")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	stateDir := filepath.Join(tempDir, "state")
	progressDir := filepath.Join(tempDir, "progress")

	err = os.MkdirAll(stateDir, 0755)
	require.NoError(err)
	err = os.MkdirAll(progressDir, 0755)
	require.NoError(err)

	// Create legacy files (state-changes.bin, mempool.bin)
	legacyStateFile := filepath.Join(stateDir, "state-changes.bin")
	legacyMempoolFile := filepath.Join(stateDir, "mempool.bin")
	indexFile := filepath.Join(stateDir, "state-change-indexes.bin")

	// Create minimal legacy files
	err = os.WriteFile(legacyStateFile, []byte("legacy"), 0644)
	require.NoError(err)
	err = os.WriteFile(legacyMempoolFile, []byte("mempool"), 0644)
	require.NoError(err)
	err = os.WriteFile(indexFile, []byte("index"), 0644)
	require.NoError(err)

	consumer := &StateSyncerConsumer{}

	// Test architecture detection
	useNew, err := consumer.shouldUseNewArchitecture(stateDir)
	require.NoError(err)
	require.False(useNew, "Should detect legacy architecture due to legacy files")

	t.Logf("Legacy architecture detection test passed!")
	t.Logf("Would use legacy single-file processing")
}

// TestArchitectureDetection tests the file format detection logic
func TestArchitectureDetection(t *testing.T) {
	require := require.New(t)

	testCases := []struct {
		name            string
		setupFiles      func(string)
		expectedNewArch bool
		description     string
	}{
		{
			name: "hypersync_files_present",
			setupFiles: func(dir string) {
				createTestHypersyncFiles(t, dir)
			},
			expectedNewArch: true,
			description:     "Should use new architecture when hypersync files are present",
		},
		{
			name: "committed_block_files_present",
			setupFiles: func(dir string) {
				createTestCommittedBlockFiles(t, dir)
			},
			expectedNewArch: true,
			description:     "Should use new architecture when committed block files are present",
		},
		{
			name: "mempool_diff_files_present",
			setupFiles: func(dir string) {
				createTestMempoolFiles(t, dir)
			},
			expectedNewArch: true,
			description:     "Should use new architecture when mempool diff files are present",
		},
		{
			name: "legacy_files_present",
			setupFiles: func(dir string) {
				os.WriteFile(filepath.Join(dir, "state-changes.bin"), []byte("test"), 0644)
				os.WriteFile(filepath.Join(dir, "mempool.bin"), []byte("test"), 0644)
			},
			expectedNewArch: false,
			description:     "Should use legacy architecture when only legacy files are present",
		},
		{
			name: "no_files_present",
			setupFiles: func(dir string) {
				// No files created
			},
			expectedNewArch: true,
			description:     "Should default to new architecture when no files are present",
		},
		{
			name: "mixed_files_present",
			setupFiles: func(dir string) {
				createTestHypersyncFiles(t, dir)
				os.WriteFile(filepath.Join(dir, "state-changes.bin"), []byte("test"), 0644)
			},
			expectedNewArch: true,
			description:     "Should prefer new architecture when both file types are present",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "detection-test")
			require.NoError(err)
			defer os.RemoveAll(tempDir)

			// Setup files according to test case
			tc.setupFiles(tempDir)

			consumer := &StateSyncerConsumer{}
			useNew, err := consumer.shouldUseNewArchitecture(tempDir)
			require.NoError(err)
			require.Equal(tc.expectedNewArch, useNew, tc.description)

			t.Logf("✓ %s", tc.description)
		})
	}
}
