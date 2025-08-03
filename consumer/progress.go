package consumer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

const (
	ProgressStateFilename  = "consumer-progress-v2.json"
	LegacyProgressFilename = "consumer-progress.bin"
)

// ProgressState tracks the comprehensive state of the consumer across all processing modes
type ProgressState struct {
	// Current processing mode
	Mode ProcessingMode `json:"mode"`

	// File-level tracking
	CurrentFilePath   string `json:"current_file_path"`
	CurrentFileOffset int64  `json:"current_file_offset"`
	CurrentEntryIndex uint64 `json:"current_entry_index"`

	// Block height tracking
	LastCommittedBlockHeight  uint64 `json:"last_committed_block_height"`
	CurrentMempoolBlockHeight uint64 `json:"current_mempool_block_height"`

	// Hypersync tracking
	LastHypersyncChunk uint64 `json:"last_hypersync_chunk"`
	HypersyncComplete  bool   `json:"hypersync_complete"`

	// Mempool state
	AppliedMempoolFiles  []string `json:"applied_mempool_files"`
	LastMempoolTimestamp int64    `json:"last_mempool_timestamp"`

	// Legacy compatibility
	LegacyLastScannedIndex uint64 `json:"legacy_last_scanned_index"`

	// Metadata
	LastUpdated time.Time `json:"last_updated"`
	Version     int       `json:"version"`
}

// ProgressManager handles loading, saving, and managing progress state
type ProgressManager struct {
	progressDir  string
	currentState *ProgressState
}

// NewProgressManager creates a new progress manager
func NewProgressManager(progressDir string) (*ProgressManager, error) {
	pm := &ProgressManager{
		progressDir: progressDir,
	}

	// Try to load existing progress
	state, err := pm.LoadProgressState()
	if err != nil {
		// If we can't load new format, try legacy format
		if legacyState, legacyErr := pm.loadLegacyProgress(); legacyErr == nil {
			state = pm.migrateLegacyProgress(legacyState)
		} else {
			// Create new progress state
			state = pm.createNewProgressState()
		}
	}

	pm.currentState = state
	return pm, nil
}

// GetCurrentState returns the current progress state
func (pm *ProgressManager) GetCurrentState() *ProgressState {
	return pm.currentState
}

// UpdateProgress updates the progress state and saves it
func (pm *ProgressManager) UpdateProgress(updateFunc func(*ProgressState)) error {
	updateFunc(pm.currentState)
	pm.currentState.LastUpdated = time.Now()
	return pm.SaveProgressState()
}

// LoadProgressState loads the progress state from disk
func (pm *ProgressManager) LoadProgressState() (*ProgressState, error) {
	progressPath := filepath.Join(pm.progressDir, ProgressStateFilename)

	data, err := os.ReadFile(progressPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read progress file: %s", progressPath)
	}

	var state ProgressState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal progress state")
	}

	// Validate and set defaults
	if state.Version == 0 {
		state.Version = 1
	}

	if state.AppliedMempoolFiles == nil {
		state.AppliedMempoolFiles = make([]string, 0)
	}

	return &state, nil
}

// SaveProgressState saves the current progress state to disk
func (pm *ProgressManager) SaveProgressState() error {
	if pm.currentState == nil {
		return errors.New("no current state to save")
	}

	pm.currentState.LastUpdated = time.Now()
	pm.currentState.Version = 1

	data, err := json.MarshalIndent(pm.currentState, "", "  ")
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress state")
	}

	progressPath := filepath.Join(pm.progressDir, ProgressStateFilename)
	tempPath := progressPath + ".tmp"

	// Ensure directory exists
	if err := os.MkdirAll(pm.progressDir, 0755); err != nil {
		return errors.Wrapf(err, "failed to create progress directory: %s", pm.progressDir)
	}

	// Write to temp file first
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return errors.Wrapf(err, "failed to write temp progress file: %s", tempPath)
	}

	// Atomic rename
	if err := os.Rename(tempPath, progressPath); err != nil {
		return errors.Wrapf(err, "failed to rename progress file")
	}

	return nil
}

// loadLegacyProgress loads progress from the legacy format
func (pm *ProgressManager) loadLegacyProgress() (uint64, error) {
	legacyPath := filepath.Join(pm.progressDir, LegacyProgressFilename)

	file, err := os.Open(legacyPath)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open legacy progress file: %s", legacyPath)
	}
	defer file.Close()

	var lastIndex uint64
	if err := binary.Read(file, binary.LittleEndian, &lastIndex); err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, errors.Wrap(err, "failed to read legacy progress")
	}

	return lastIndex, nil
}

// migrateLegacyProgress converts legacy progress to new format
func (pm *ProgressManager) migrateLegacyProgress(legacyIndex uint64) *ProgressState {
	state := pm.createNewProgressState()
	state.LegacyLastScannedIndex = legacyIndex
	state.CurrentEntryIndex = legacyIndex

	// Assume hypersync is complete if we have legacy progress
	state.HypersyncComplete = true
	state.Mode = ModeCommittedBlocks

	return state
}

// createNewProgressState creates a new default progress state
func (pm *ProgressManager) createNewProgressState() *ProgressState {
	return &ProgressState{
		Mode:                      ModeHypersync,
		CurrentFilePath:           "",
		CurrentFileOffset:         0,
		CurrentEntryIndex:         0,
		LastCommittedBlockHeight:  0,
		CurrentMempoolBlockHeight: 0,
		LastHypersyncChunk:        0,
		HypersyncComplete:         false,
		AppliedMempoolFiles:       make([]string, 0),
		LastMempoolTimestamp:      0,
		LegacyLastScannedIndex:    0,
		LastUpdated:               time.Now(),
		Version:                   1,
	}
}

// SetMode updates the processing mode
func (pm *ProgressManager) SetMode(mode ProcessingMode) error {
	return pm.UpdateProgress(func(state *ProgressState) {
		state.Mode = mode
	})
}

// SetCurrentFile updates the current file being processed
func (pm *ProgressManager) SetCurrentFile(filePath string, offset int64) error {
	return pm.UpdateProgress(func(state *ProgressState) {
		state.CurrentFilePath = filePath
		state.CurrentFileOffset = offset
	})
}

// IncrementEntryIndex increments the entry index counter
func (pm *ProgressManager) IncrementEntryIndex() error {
	return pm.UpdateProgress(func(state *ProgressState) {
		state.CurrentEntryIndex++
	})
}

// UpdateCommittedBlockHeight updates the last processed committed block height
func (pm *ProgressManager) UpdateCommittedBlockHeight(height uint64) error {
	return pm.UpdateProgress(func(state *ProgressState) {
		state.LastCommittedBlockHeight = height
		// Update mempool block height to match
		if height > state.CurrentMempoolBlockHeight {
			state.CurrentMempoolBlockHeight = height
			// Reset mempool timestamp when moving to new block
			state.LastMempoolTimestamp = 0
			// Clear applied mempool files for new block
			state.AppliedMempoolFiles = make([]string, 0)
		}
	})
}

// UpdateMempoolProgress updates mempool-related progress
func (pm *ProgressManager) UpdateMempoolProgress(filePath string, timestamp int64) error {
	return pm.UpdateProgress(func(state *ProgressState) {
		state.LastMempoolTimestamp = timestamp

		// Add to applied mempool files if not already present
		for _, appliedFile := range state.AppliedMempoolFiles {
			if appliedFile == filePath {
				return // Already recorded
			}
		}
		state.AppliedMempoolFiles = append(state.AppliedMempoolFiles, filePath)
	})
}

// UpdateHypersyncProgress updates hypersync-related progress
func (pm *ProgressManager) UpdateHypersyncProgress(chunkId uint64) error {
	return pm.UpdateProgress(func(state *ProgressState) {
		if chunkId > state.LastHypersyncChunk {
			state.LastHypersyncChunk = chunkId
		}
	})
}

// UpdateCommittedBlockProgress updates the last processed committed block height
func (pm *ProgressManager) UpdateCommittedBlockProgress(blockHeight uint64) error {
	return pm.UpdateProgress(func(state *ProgressState) {
		if blockHeight > state.LastCommittedBlockHeight {
			state.LastCommittedBlockHeight = blockHeight
			// Also update current mempool block height for mempool processing
			state.CurrentMempoolBlockHeight = blockHeight
		}
	})
}

// MarkHypersyncComplete marks hypersync as complete and transitions to committed blocks
func (pm *ProgressManager) MarkHypersyncComplete() error {
	return pm.UpdateProgress(func(state *ProgressState) {
		state.HypersyncComplete = true
		state.Mode = ModeCommittedBlocks
	})
}

// RevertMempoolFiles reverts applied mempool files (used when new block is committed)
func (pm *ProgressManager) RevertMempoolFiles() error {
	return pm.UpdateProgress(func(state *ProgressState) {
		// Clear applied mempool files - they'll be reverted
		state.AppliedMempoolFiles = make([]string, 0)
		state.LastMempoolTimestamp = 0
	})
}

// GetFileProgress returns progress information for the current file
func (pm *ProgressManager) GetFileProgress() (filePath string, offset int64, entryIndex uint64) {
	state := pm.GetCurrentState()
	return state.CurrentFilePath, state.CurrentFileOffset, state.CurrentEntryIndex
}

// GetProcessingModeInfo returns detailed information about the current processing mode
func (pm *ProgressManager) GetProcessingModeInfo() (ProcessingMode, string) {
	state := pm.GetCurrentState()

	var info string
	switch state.Mode {
	case ModeHypersync:
		info = fmt.Sprintf("chunk %d", state.LastHypersyncChunk)
	case ModeCommittedBlocks:
		info = fmt.Sprintf("block %d", state.LastCommittedBlockHeight)
	case ModeMempool:
		info = fmt.Sprintf("block %d, timestamp %d", state.CurrentMempoolBlockHeight, state.LastMempoolTimestamp)
	}

	return state.Mode, info
}

// IsLegacyMode returns true if we're still operating in legacy compatibility mode
func (pm *ProgressManager) IsLegacyMode() bool {
	state := pm.GetCurrentState()
	return state.LegacyLastScannedIndex > 0 && !state.HypersyncComplete
}

// GetLegacyProgress returns the legacy progress index for backward compatibility
func (pm *ProgressManager) GetLegacyProgress() uint64 {
	return pm.GetCurrentState().LegacyLastScannedIndex
}

// CleanupLegacyFiles removes legacy progress files after successful migration
func (pm *ProgressManager) CleanupLegacyFiles() error {
	legacyPath := filepath.Join(pm.progressDir, LegacyProgressFilename)

	if _, err := os.Stat(legacyPath); err == nil {
		if err := os.Remove(legacyPath); err != nil {
			return errors.Wrapf(err, "failed to remove legacy progress file: %s", legacyPath)
		}
	}

	return nil
}

// Reset resets the progress state to initial values
func (pm *ProgressManager) Reset() error {
	pm.currentState = pm.createNewProgressState()
	return pm.SaveProgressState()
}

// String returns a string representation of the current progress
func (pm *ProgressManager) String() string {
	state := pm.GetCurrentState()
	mode, info := pm.GetProcessingModeInfo()

	return fmt.Sprintf("Mode: %s (%s), Entry: %d, File: %s@%d",
		mode, info, state.CurrentEntryIndex,
		filepath.Base(state.CurrentFilePath), state.CurrentFileOffset)
}
