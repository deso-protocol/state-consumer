package consumer

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// ProcessingMode represents the current processing state of the consumer
type ProcessingMode int

const (
	ModeHypersync ProcessingMode = iota
	ModeCommittedBlocks
	ModeMempool
)

func (m ProcessingMode) String() string {
	switch m {
	case ModeHypersync:
		return "hypersync"
	case ModeCommittedBlocks:
		return "committed_blocks"
	case ModeMempool:
		return "mempool"
	default:
		return "unknown"
	}
}

// FileType represents the type of state change file
type FileType int

const (
	FileTypeCommittedBlock FileType = iota
	FileTypeMempoolDiff
	FileTypeMempoolAncestral
	FileTypeHypersyncChunk
	FileTypeLegacy
)

func (ft FileType) String() string {
	switch ft {
	case FileTypeCommittedBlock:
		return "committed_block"
	case FileTypeMempoolDiff:
		return "mempool_diff"
	case FileTypeMempoolAncestral:
		return "mempool_ancestral"
	case FileTypeHypersyncChunk:
		return "hypersync_chunk"
	case FileTypeLegacy:
		return "legacy"
	default:
		return "unknown"
	}
}

// FileInfo contains metadata about a state change file
type FileInfo struct {
	Path        string
	Type        FileType
	BlockHeight uint64
	Timestamp   int64
	ChunkId     *uint64 // For hypersync chunks only
	ModTime     time.Time
}

// FileManager handles discovery and management of state change files
type FileManager struct {
	stateChangeDir string
	currentMode    ProcessingMode

	// Compiled regex patterns for file parsing
	committedBlockPattern   *regexp.Regexp
	mempoolDiffPattern      *regexp.Regexp
	mempoolAncestralPattern *regexp.Regexp
	hypersyncChunkPattern   *regexp.Regexp
}

// NewFileManager creates a new file manager for the given directory
func NewFileManager(stateChangeDir string) (*FileManager, error) {
	fm := &FileManager{
		stateChangeDir: stateChangeDir,
		currentMode:    ModeHypersync, // Start with hypersync detection
	}

	// Compile regex patterns for file parsing
	var err error
	fm.committedBlockPattern, err = regexp.Compile(`^state_changes_(\d+)\.bin$`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compile committed block pattern")
	}

	fm.mempoolDiffPattern, err = regexp.Compile(`^mempool_(\d+)_(\d+)\.bin$`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compile mempool diff pattern")
	}

	fm.mempoolAncestralPattern, err = regexp.Compile(`^mempool_ancestral_(\d+)_(\d+)\.bin$`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compile mempool ancestral pattern")
	}

	fm.hypersyncChunkPattern, err = regexp.Compile(`^hypersync_chunk_(\d+)_(\d+)_(\d+)\.bin$`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compile hypersync chunk pattern")
	}

	return fm, nil
}

// DiscoverAllFiles scans the state change directory and returns all discovered files
func (fm *FileManager) DiscoverAllFiles() ([]*FileInfo, error) {
	entries, err := os.ReadDir(fm.stateChangeDir)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read state change directory: %s", fm.stateChangeDir)
	}

	var files []*FileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		fullPath := filepath.Join(fm.stateChangeDir, filename)

		fileInfo, err := fm.parseFileName(filename, fullPath)
		if err != nil {
			// Skip files that don't match our patterns
			continue
		}

		// Get file modification time
		stat, err := entry.Info()
		if err == nil {
			fileInfo.ModTime = stat.ModTime()
		}

		files = append(files, fileInfo)
	}

	return files, nil
}

// parseFileName attempts to parse a filename and determine its type and metadata
func (fm *FileManager) parseFileName(filename, fullPath string) (*FileInfo, error) {
	// Try committed block pattern: state_changes_<height>.bin
	if matches := fm.committedBlockPattern.FindStringSubmatch(filename); matches != nil {
		height, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid block height in filename: %s", filename)
		}

		return &FileInfo{
			Path:        fullPath,
			Type:        FileTypeCommittedBlock,
			BlockHeight: height,
		}, nil
	}

	// Try mempool diff pattern: mempool_<height>_<timestamp>.bin
	if matches := fm.mempoolDiffPattern.FindStringSubmatch(filename); matches != nil {
		height, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid block height in mempool filename: %s", filename)
		}

		timestamp, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid timestamp in mempool filename: %s", filename)
		}

		return &FileInfo{
			Path:        fullPath,
			Type:        FileTypeMempoolDiff,
			BlockHeight: height,
			Timestamp:   timestamp,
		}, nil
	}

	// Try mempool ancestral pattern: mempool_ancestral_<height>_<timestamp>.bin
	if matches := fm.mempoolAncestralPattern.FindStringSubmatch(filename); matches != nil {
		height, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid block height in ancestral filename: %s", filename)
		}

		timestamp, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid timestamp in ancestral filename: %s", filename)
		}

		return &FileInfo{
			Path:        fullPath,
			Type:        FileTypeMempoolAncestral,
			BlockHeight: height,
			Timestamp:   timestamp,
		}, nil
	}

	// Try hypersync chunk pattern: hypersync_chunk_<height>_<chunk_id>_<timestamp>.bin
	if matches := fm.hypersyncChunkPattern.FindStringSubmatch(filename); matches != nil {
		height, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid block height in hypersync filename: %s", filename)
		}

		chunkId, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid chunk ID in hypersync filename: %s", filename)
		}

		timestamp, err := strconv.ParseInt(matches[3], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid timestamp in hypersync filename: %s", filename)
		}

		return &FileInfo{
			Path:        fullPath,
			Type:        FileTypeHypersyncChunk,
			BlockHeight: height,
			Timestamp:   timestamp,
			ChunkId:     &chunkId,
		}, nil
	}

	// Check for legacy files
	if filename == "state-changes.bin" || filename == "mempool.bin" {
		return &FileInfo{
			Path: fullPath,
			Type: FileTypeLegacy,
		}, nil
	}

	return nil, fmt.Errorf("unrecognized file pattern: %s", filename)
}

// GetFilesByType returns files of a specific type, sorted appropriately
func (fm *FileManager) GetFilesByType(files []*FileInfo, fileType FileType) []*FileInfo {
	var filtered []*FileInfo
	for _, file := range files {
		if file.Type == fileType {
			filtered = append(filtered, file)
		}
	}

	// Sort based on file type
	switch fileType {
	case FileTypeCommittedBlock:
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].BlockHeight < filtered[j].BlockHeight
		})
	case FileTypeMempoolDiff, FileTypeMempoolAncestral:
		sort.Slice(filtered, func(i, j int) bool {
			if filtered[i].BlockHeight != filtered[j].BlockHeight {
				return filtered[i].BlockHeight < filtered[j].BlockHeight
			}
			return filtered[i].Timestamp < filtered[j].Timestamp
		})
	case FileTypeHypersyncChunk:
		sort.Slice(filtered, func(i, j int) bool {
			if filtered[i].BlockHeight != filtered[j].BlockHeight {
				return filtered[i].BlockHeight < filtered[j].BlockHeight
			}
			if filtered[i].ChunkId != nil && filtered[j].ChunkId != nil {
				if *filtered[i].ChunkId != *filtered[j].ChunkId {
					return *filtered[i].ChunkId < *filtered[j].ChunkId
				}
			}
			return filtered[i].Timestamp < filtered[j].Timestamp
		})
	}

	return filtered
}

// GetNextHypersyncFiles returns the next batch of hypersync files to process
func (fm *FileManager) GetNextHypersyncFiles(allFiles []*FileInfo, lastProcessedChunk uint64) ([]*FileInfo, error) {
	hypersyncFiles := fm.GetFilesByType(allFiles, FileTypeHypersyncChunk)

	if len(hypersyncFiles) == 0 {
		return nil, nil // No hypersync files found
	}

	// Filter to unprocessed chunks
	var nextFiles []*FileInfo
	for _, file := range hypersyncFiles {
		if file.ChunkId != nil && *file.ChunkId > lastProcessedChunk {
			nextFiles = append(nextFiles, file)
		}
	}

	return nextFiles, nil
}

// GetNextCommittedBlockFile returns the next committed block file to process
func (fm *FileManager) GetNextCommittedBlockFile(allFiles []*FileInfo, lastProcessedHeight uint64) (*FileInfo, error) {
	committedFiles := fm.GetFilesByType(allFiles, FileTypeCommittedBlock)

	for _, file := range committedFiles {
		if file.BlockHeight > lastProcessedHeight {
			return file, nil
		}
	}

	return nil, nil // No new committed block files
}

// GetNextMempoolFile returns the next mempool file to process for a given block height
func (fm *FileManager) GetNextMempoolFile(allFiles []*FileInfo, blockHeight uint64, lastProcessedTimestamp int64) (*FileInfo, error) {
	mempoolFiles := fm.GetFilesByType(allFiles, FileTypeMempoolDiff)

	for _, file := range mempoolFiles {
		if file.BlockHeight == blockHeight && file.Timestamp > lastProcessedTimestamp {
			return file, nil
		}
	}

	return nil, nil // No new mempool files for this block height
}

// GetAncestralFilesForBlock returns all ancestral files for a given block height, sorted by timestamp (descending for reversion)
func (fm *FileManager) GetAncestralFilesForBlock(allFiles []*FileInfo, blockHeight uint64) []*FileInfo {
	ancestralFiles := fm.GetFilesByType(allFiles, FileTypeMempoolAncestral)

	var blockAncestralFiles []*FileInfo
	for _, file := range ancestralFiles {
		if file.BlockHeight == blockHeight {
			blockAncestralFiles = append(blockAncestralFiles, file)
		}
	}

	// Sort by timestamp descending for proper reversion order
	sort.Slice(blockAncestralFiles, func(i, j int) bool {
		return blockAncestralFiles[i].Timestamp > blockAncestralFiles[j].Timestamp
	})

	return blockAncestralFiles
}

// HasLegacyFiles checks if legacy format files exist
func (fm *FileManager) HasLegacyFiles() bool {
	legacyFiles := []string{"state-changes.bin", "mempool.bin"}

	for _, filename := range legacyFiles {
		if _, err := os.Stat(filepath.Join(fm.stateChangeDir, filename)); err == nil {
			return true
		}
	}

	return false
}

// HasNewFormatFiles checks if any new format files exist
func (fm *FileManager) HasNewFormatFiles() bool {
	files, err := fm.DiscoverAllFiles()
	if err != nil {
		return false
	}

	for _, file := range files {
		if file.Type != FileTypeLegacy {
			return true
		}
	}

	return false
}

// DetectProcessingMode determines the appropriate processing mode based on available files
func (fm *FileManager) DetectProcessingMode(allFiles []*FileInfo, currentState *ProgressState) ProcessingMode {
	// If hypersync is not complete and hypersync files exist, use hypersync mode
	if !currentState.HypersyncComplete {
		hypersyncFiles := fm.GetFilesByType(allFiles, FileTypeHypersyncChunk)
		if len(hypersyncFiles) > 0 {
			return ModeHypersync
		}
		// No hypersync files, mark as complete
		currentState.HypersyncComplete = true
	}

	// Check for new committed block files
	committedFiles := fm.GetFilesByType(allFiles, FileTypeCommittedBlock)
	for _, file := range committedFiles {
		if file.BlockHeight > currentState.LastCommittedBlockHeight {
			return ModeCommittedBlocks
		}
	}

	// Check for mempool files for current block height
	mempoolFiles := fm.GetFilesByType(allFiles, FileTypeMempoolDiff)
	for _, file := range mempoolFiles {
		if file.BlockHeight == currentState.CurrentMempoolBlockHeight &&
			file.Timestamp > currentState.LastMempoolTimestamp {
			return ModeMempool
		}
	}

	// Default to mempool mode in most cases:
	// 1. If we have processed at least one committed block
	// 2. If there are actual mempool files present
	// 3. If no files exist at all (assume hypersync auto-completed)
	if currentState.LastCommittedBlockHeight > 0 || len(mempoolFiles) > 0 || len(allFiles) == 0 {
		return ModeMempool
	}

	// Only stay in committed blocks mode if we have some files but no committed blocks processed yet
	return ModeCommittedBlocks
}

// ValidateFileIntegrity performs basic validation on a file
func (fm *FileManager) ValidateFileIntegrity(file *FileInfo) error {
	stat, err := os.Stat(file.Path)
	if err != nil {
		return errors.Wrapf(err, "file does not exist: %s", file.Path)
	}

	if stat.Size() == 0 {
		return fmt.Errorf("file is empty: %s", file.Path)
	}

	return nil
}

// GetFileDisplayName returns a human-readable name for a file
func (fm *FileManager) GetFileDisplayName(file *FileInfo) string {
	filename := filepath.Base(file.Path)
	switch file.Type {
	case FileTypeCommittedBlock:
		return fmt.Sprintf("committed block %d (%s)", file.BlockHeight, filename)
	case FileTypeMempoolDiff:
		return fmt.Sprintf("mempool diff %d@%d (%s)", file.BlockHeight, file.Timestamp, filename)
	case FileTypeMempoolAncestral:
		return fmt.Sprintf("ancestral %d@%d (%s)", file.BlockHeight, file.Timestamp, filename)
	case FileTypeHypersyncChunk:
		if file.ChunkId != nil {
			return fmt.Sprintf("hypersync chunk %d:%d@%d (%s)", file.BlockHeight, *file.ChunkId, file.Timestamp, filename)
		}
		return fmt.Sprintf("hypersync chunk %d@%d (%s)", file.BlockHeight, file.Timestamp, filename)
	case FileTypeLegacy:
		return fmt.Sprintf("legacy file (%s)", filename)
	default:
		return filename
	}
}
