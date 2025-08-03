package consumer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewFileManager(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)
	require.NotNil(fm)
	require.Equal(tempDir, fm.stateChangeDir)
	require.Equal(ModeHypersync, fm.currentMode)
}

func TestParseFileName(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	tests := []struct {
		filename          string
		expectedType      FileType
		expectedHeight    uint64
		expectedTimestamp int64
		expectedChunkId   *uint64
		shouldError       bool
	}{
		{
			filename:       "state_changes_100.bin",
			expectedType:   FileTypeCommittedBlock,
			expectedHeight: 100,
		},
		{
			filename:          "mempool_200_1640995200123456789.bin",
			expectedType:      FileTypeMempoolDiff,
			expectedHeight:    200,
			expectedTimestamp: 1640995200123456789,
		},
		{
			filename:          "mempool_ancestral_200_1640995200123456789.bin",
			expectedType:      FileTypeMempoolAncestral,
			expectedHeight:    200,
			expectedTimestamp: 1640995200123456789,
		},
		{
			filename:          "hypersync_chunk_300_5_1640995200123456789.bin",
			expectedType:      FileTypeHypersyncChunk,
			expectedHeight:    300,
			expectedTimestamp: 1640995200123456789,
			expectedChunkId:   func() *uint64 { v := uint64(5); return &v }(),
		},
		{
			filename:     "state-changes.bin",
			expectedType: FileTypeLegacy,
		},
		{
			filename:     "mempool.bin",
			expectedType: FileTypeLegacy,
		},
		{
			filename:    "invalid_file.bin",
			shouldError: true,
		},
		{
			filename:    "state_changes_invalid.bin",
			shouldError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.filename, func(t *testing.T) {
			fullPath := filepath.Join(tempDir, test.filename)
			fileInfo, err := fm.parseFileName(test.filename, fullPath)

			if test.shouldError {
				require.Error(err)
				return
			}

			require.NoError(err)
			require.NotNil(fileInfo)
			require.Equal(test.expectedType, fileInfo.Type)
			require.Equal(fullPath, fileInfo.Path)

			if test.expectedType != FileTypeLegacy {
				require.Equal(test.expectedHeight, fileInfo.BlockHeight)
				if test.expectedType != FileTypeCommittedBlock {
					require.Equal(test.expectedTimestamp, fileInfo.Timestamp)
				}
				if test.expectedChunkId != nil {
					require.NotNil(fileInfo.ChunkId)
					require.Equal(*test.expectedChunkId, *fileInfo.ChunkId)
				}
			}
		})
	}
}

func TestDiscoverAllFiles(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	// Create test files
	testFiles := []string{
		"state_changes_100.bin",
		"state_changes_101.bin",
		"mempool_100_1640995200000000000.bin",
		"mempool_100_1640995200000000001.bin",
		"mempool_ancestral_100_1640995200000000000.bin",
		"hypersync_chunk_50_1_1640995200000000000.bin",
		"hypersync_chunk_50_2_1640995200000000001.bin",
		"state-changes.bin",
		"invalid_file.txt",
	}

	for _, filename := range testFiles {
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, []byte("test content"), 0644)
		require.NoError(err)
	}

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	files, err := fm.DiscoverAllFiles()
	require.NoError(err)

	// Should discover all valid files except invalid_file.txt
	require.Len(files, 8)

	// Verify file types are correct
	typeCount := make(map[FileType]int)
	for _, file := range files {
		typeCount[file.Type]++
	}

	require.Equal(2, typeCount[FileTypeCommittedBlock])
	require.Equal(2, typeCount[FileTypeMempoolDiff])
	require.Equal(1, typeCount[FileTypeMempoolAncestral])
	require.Equal(2, typeCount[FileTypeHypersyncChunk])
	require.Equal(1, typeCount[FileTypeLegacy])
}

func TestGetFilesByType(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	// Create test file infos
	files := []*FileInfo{
		{Path: "state_changes_102.bin", Type: FileTypeCommittedBlock, BlockHeight: 102},
		{Path: "state_changes_100.bin", Type: FileTypeCommittedBlock, BlockHeight: 100},
		{Path: "state_changes_101.bin", Type: FileTypeCommittedBlock, BlockHeight: 101},
		{Path: "mempool_100_1640995200000000002.bin", Type: FileTypeMempoolDiff, BlockHeight: 100, Timestamp: 1640995200000000002},
		{Path: "mempool_100_1640995200000000001.bin", Type: FileTypeMempoolDiff, BlockHeight: 100, Timestamp: 1640995200000000001},
		{Path: "hypersync_chunk_50_2_1640995200000000000.bin", Type: FileTypeHypersyncChunk, BlockHeight: 50, ChunkId: func() *uint64 { v := uint64(2); return &v }(), Timestamp: 1640995200000000000},
		{Path: "hypersync_chunk_50_1_1640995200000000000.bin", Type: FileTypeHypersyncChunk, BlockHeight: 50, ChunkId: func() *uint64 { v := uint64(1); return &v }(), Timestamp: 1640995200000000000},
	}

	// Test committed block sorting (by height)
	committedFiles := fm.GetFilesByType(files, FileTypeCommittedBlock)
	require.Len(committedFiles, 3)
	require.Equal(uint64(100), committedFiles[0].BlockHeight)
	require.Equal(uint64(101), committedFiles[1].BlockHeight)
	require.Equal(uint64(102), committedFiles[2].BlockHeight)

	// Test mempool sorting (by height, then timestamp)
	mempoolFiles := fm.GetFilesByType(files, FileTypeMempoolDiff)
	require.Len(mempoolFiles, 2)
	require.Equal(int64(1640995200000000001), mempoolFiles[0].Timestamp)
	require.Equal(int64(1640995200000000002), mempoolFiles[1].Timestamp)

	// Test hypersync sorting (by height, then chunk ID, then timestamp)
	hypersyncFiles := fm.GetFilesByType(files, FileTypeHypersyncChunk)
	require.Len(hypersyncFiles, 2)
	require.Equal(uint64(1), *hypersyncFiles[0].ChunkId)
	require.Equal(uint64(2), *hypersyncFiles[1].ChunkId)
}

func TestGetNextCommittedBlockFile(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	files := []*FileInfo{
		{Path: "state_changes_100.bin", Type: FileTypeCommittedBlock, BlockHeight: 100},
		{Path: "state_changes_101.bin", Type: FileTypeCommittedBlock, BlockHeight: 101},
		{Path: "state_changes_103.bin", Type: FileTypeCommittedBlock, BlockHeight: 103},
	}

	// Test getting next file after height 100
	nextFile, err := fm.GetNextCommittedBlockFile(files, 100)
	require.NoError(err)
	require.NotNil(nextFile)
	require.Equal(uint64(101), nextFile.BlockHeight)

	// Test getting next file after height 101
	nextFile, err = fm.GetNextCommittedBlockFile(files, 101)
	require.NoError(err)
	require.NotNil(nextFile)
	require.Equal(uint64(103), nextFile.BlockHeight)

	// Test no next file available
	nextFile, err = fm.GetNextCommittedBlockFile(files, 103)
	require.NoError(err)
	require.Nil(nextFile)
}

func TestGetNextMempoolFile(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	files := []*FileInfo{
		{Path: "mempool_100_1000.bin", Type: FileTypeMempoolDiff, BlockHeight: 100, Timestamp: 1000},
		{Path: "mempool_100_1001.bin", Type: FileTypeMempoolDiff, BlockHeight: 100, Timestamp: 1001},
		{Path: "mempool_100_1003.bin", Type: FileTypeMempoolDiff, BlockHeight: 100, Timestamp: 1003},
		{Path: "mempool_101_1000.bin", Type: FileTypeMempoolDiff, BlockHeight: 101, Timestamp: 1000},
	}

	// Test getting next mempool file for block 100 after timestamp 1000
	nextFile, err := fm.GetNextMempoolFile(files, 100, 1000)
	require.NoError(err)
	require.NotNil(nextFile)
	require.Equal(int64(1001), nextFile.Timestamp)

	// Test getting next mempool file for block 100 after timestamp 1001
	nextFile, err = fm.GetNextMempoolFile(files, 100, 1001)
	require.NoError(err)
	require.NotNil(nextFile)
	require.Equal(int64(1003), nextFile.Timestamp)

	// Test no next file for block 100 after timestamp 1003
	nextFile, err = fm.GetNextMempoolFile(files, 100, 1003)
	require.NoError(err)
	require.Nil(nextFile)

	// Test getting next file for different block
	nextFile, err = fm.GetNextMempoolFile(files, 101, 999)
	require.NoError(err)
	require.NotNil(nextFile)
	require.Equal(uint64(101), nextFile.BlockHeight)
	require.Equal(int64(1000), nextFile.Timestamp)
}

func TestGetAncestralFilesForBlock(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	files := []*FileInfo{
		{Path: "mempool_ancestral_100_1000.bin", Type: FileTypeMempoolAncestral, BlockHeight: 100, Timestamp: 1000},
		{Path: "mempool_ancestral_100_1002.bin", Type: FileTypeMempoolAncestral, BlockHeight: 100, Timestamp: 1002},
		{Path: "mempool_ancestral_100_1001.bin", Type: FileTypeMempoolAncestral, BlockHeight: 100, Timestamp: 1001},
		{Path: "mempool_ancestral_101_1000.bin", Type: FileTypeMempoolAncestral, BlockHeight: 101, Timestamp: 1000},
	}

	ancestralFiles := fm.GetAncestralFilesForBlock(files, 100)
	require.Len(ancestralFiles, 3)

	// Should be sorted by timestamp descending for proper reversion order
	require.Equal(int64(1002), ancestralFiles[0].Timestamp)
	require.Equal(int64(1001), ancestralFiles[1].Timestamp)
	require.Equal(int64(1000), ancestralFiles[2].Timestamp)

	// Test different block
	ancestralFiles = fm.GetAncestralFilesForBlock(files, 101)
	require.Len(ancestralFiles, 1)
	require.Equal(uint64(101), ancestralFiles[0].BlockHeight)

	// Test non-existent block
	ancestralFiles = fm.GetAncestralFilesForBlock(files, 999)
	require.Len(ancestralFiles, 0)
}

func TestDetectProcessingMode(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	// Test hypersync mode detection
	files := []*FileInfo{
		{Path: "hypersync_chunk_50_1_1000.bin", Type: FileTypeHypersyncChunk, BlockHeight: 50, ChunkId: func() *uint64 { v := uint64(1); return &v }(), Timestamp: 1000},
	}

	state := &ProgressState{
		HypersyncComplete:         false,
		LastCommittedBlockHeight:  0,
		CurrentMempoolBlockHeight: 0,
		LastMempoolTimestamp:      0,
	}

	mode := fm.DetectProcessingMode(files, state)
	require.Equal(ModeHypersync, mode)

	// Test committed blocks mode detection
	state.HypersyncComplete = true
	files = append(files, &FileInfo{
		Path:        "state_changes_100.bin",
		Type:        FileTypeCommittedBlock,
		BlockHeight: 100,
	})

	mode = fm.DetectProcessingMode(files, state)
	require.Equal(ModeCommittedBlocks, mode)

	// Test mempool mode detection
	state.LastCommittedBlockHeight = 100
	state.CurrentMempoolBlockHeight = 100
	files = append(files, &FileInfo{
		Path:        "mempool_100_2000.bin",
		Type:        FileTypeMempoolDiff,
		BlockHeight: 100,
		Timestamp:   2000,
	})

	mode = fm.DetectProcessingMode(files, state)
	require.Equal(ModeMempool, mode)
}

func TestHasLegacyFiles(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	// Initially no legacy files
	require.False(fm.HasLegacyFiles())

	// Create a legacy file
	err = os.WriteFile(filepath.Join(tempDir, "state-changes.bin"), []byte("test"), 0644)
	require.NoError(err)

	require.True(fm.HasLegacyFiles())
}

func TestHasNewFormatFiles(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	// Initially no new format files
	require.False(fm.HasNewFormatFiles())

	// Create a new format file
	err = os.WriteFile(filepath.Join(tempDir, "state_changes_100.bin"), []byte("test"), 0644)
	require.NoError(err)

	require.True(fm.HasNewFormatFiles())
}

func TestValidateFileIntegrity(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	// Test non-existent file
	fileInfo := &FileInfo{Path: filepath.Join(tempDir, "nonexistent.bin")}
	err = fm.ValidateFileIntegrity(fileInfo)
	require.Error(err)

	// Test empty file
	emptyFile := filepath.Join(tempDir, "empty.bin")
	err = os.WriteFile(emptyFile, []byte{}, 0644)
	require.NoError(err)

	fileInfo = &FileInfo{Path: emptyFile}
	err = fm.ValidateFileIntegrity(fileInfo)
	require.Error(err)

	// Test valid file
	validFile := filepath.Join(tempDir, "valid.bin")
	err = os.WriteFile(validFile, []byte("content"), 0644)
	require.NoError(err)

	fileInfo = &FileInfo{Path: validFile}
	err = fm.ValidateFileIntegrity(fileInfo)
	require.NoError(err)
}

func TestGetFileDisplayName(t *testing.T) {
	require := require.New(t)

	tempDir, err := os.MkdirTemp("", "file-manager-test")
	require.NoError(err)
	defer os.RemoveAll(tempDir)

	fm, err := NewFileManager(tempDir)
	require.NoError(err)

	tests := []struct {
		fileInfo *FileInfo
		expected string
	}{
		{
			fileInfo: &FileInfo{
				Path:        "/path/to/state_changes_100.bin",
				Type:        FileTypeCommittedBlock,
				BlockHeight: 100,
			},
			expected: "committed block 100 (state_changes_100.bin)",
		},
		{
			fileInfo: &FileInfo{
				Path:        "/path/to/mempool_100_1640995200000000000.bin",
				Type:        FileTypeMempoolDiff,
				BlockHeight: 100,
				Timestamp:   1640995200000000000,
			},
			expected: "mempool diff 100@1640995200000000000 (mempool_100_1640995200000000000.bin)",
		},
		{
			fileInfo: &FileInfo{
				Path:        "/path/to/hypersync_chunk_50_5_1640995200000000000.bin",
				Type:        FileTypeHypersyncChunk,
				BlockHeight: 50,
				ChunkId:     func() *uint64 { v := uint64(5); return &v }(),
				Timestamp:   1640995200000000000,
			},
			expected: "hypersync chunk 50:5@1640995200000000000 (hypersync_chunk_50_5_1640995200000000000.bin)",
		},
		{
			fileInfo: &FileInfo{
				Path: "/path/to/state-changes.bin",
				Type: FileTypeLegacy,
			},
			expected: "legacy file (state-changes.bin)",
		},
	}

	for _, test := range tests {
		displayName := fm.GetFileDisplayName(test.fileInfo)
		require.Equal(test.expected, displayName)
	}
}
