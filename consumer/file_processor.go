package consumer

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// FileProcessor coordinates between file management, hypersync processing, and sequential file processing
type FileProcessor struct {
	fileManager             *FileManager
	progressManager         *ProgressManager
	ancestralManager        *AncestralRecordManager
	hypersyncProcessor      *HypersyncProcessor
	committedBlockProcessor *CommittedBlockProcessor
	mempoolProcessor        *MempoolProcessor
	dataHandler             StateSyncerDataHandler

	// Configuration
	maxConcurrentChunks int
	batchSize           uint64

	// State
	isRunning bool
	stopChan  chan bool
}

// FileProcessorConfig holds configuration for the file processor
type FileProcessorConfig struct {
	StateChangeDir      string
	ProgressDir         string
	MaxConcurrentChunks int
	BatchSize           uint64
}

// NewFileProcessor creates a new file processor with the given configuration
func NewFileProcessor(config FileProcessorConfig, dataHandler StateSyncerDataHandler) (*FileProcessor, error) {
	// Initialize file manager
	fileManager, err := NewFileManager(config.StateChangeDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file manager")
	}

	// Initialize progress manager
	progressManager, err := NewProgressManager(config.ProgressDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create progress manager")
	}

	// Initialize ancestral record manager
	ancestralManager := NewAncestralRecordManager(config.ProgressDir, dataHandler)

	// Initialize hypersync processor
	hypersyncProcessor := NewHypersyncProcessor(
		fileManager,
		progressManager,
		dataHandler,
		config.MaxConcurrentChunks,
		config.BatchSize,
	)

	// Initialize committed block processor
	committedBlockProcessor := NewCommittedBlockProcessor(
		fileManager,
		progressManager,
		ancestralManager,
		dataHandler,
		config.BatchSize,
	)

	// Initialize mempool processor
	mempoolProcessor := NewMempoolProcessor(
		fileManager,
		progressManager,
		ancestralManager,
		dataHandler,
		config.BatchSize,
	)

	return &FileProcessor{
		fileManager:             fileManager,
		progressManager:         progressManager,
		ancestralManager:        ancestralManager,
		hypersyncProcessor:      hypersyncProcessor,
		committedBlockProcessor: committedBlockProcessor,
		mempoolProcessor:        mempoolProcessor,
		dataHandler:             dataHandler,
		maxConcurrentChunks:     config.MaxConcurrentChunks,
		batchSize:               config.BatchSize,
		stopChan:                make(chan bool),
	}, nil
}

// Start begins processing files based on the current state and available files
func (fp *FileProcessor) Start() error {
	if fp.isRunning {
		return fmt.Errorf("file processor is already running")
	}

	fp.isRunning = true
	glog.Infof("Starting file processor")

	// Determine the processing mode based on current state and available files
	mode, err := fp.determineProcessingMode()
	if err != nil {
		return errors.Wrap(err, "failed to determine processing mode")
	}

	glog.Infof("File processor starting in mode: %s", mode)

	// Start processing based on mode
	switch mode {
	case ModeHypersync:
		return fp.startHypersyncProcessing()
	case ModeCommittedBlocks:
		return fp.startCommittedBlockProcessing()
	case ModeMempool:
		return fp.startMempoolProcessing()
	default:
		return fmt.Errorf("unknown processing mode: %s", mode)
	}
}

// Stop stops the file processor
func (fp *FileProcessor) Stop() error {
	if !fp.isRunning {
		return nil
	}

	glog.Infof("Stopping file processor")
	fp.isRunning = false

	// Stop all processors if running
	fp.hypersyncProcessor.StopProcessing()
	fp.committedBlockProcessor.StopProcessing()
	fp.mempoolProcessor.StopProcessing()

	// Signal stop
	fp.stopChan <- true

	glog.Infof("File processor stopped")
	return nil
}

// determineProcessingMode determines the appropriate processing mode based on current state and available files
func (fp *FileProcessor) determineProcessingMode() (ProcessingMode, error) {
	// Discover all available files
	allFiles, err := fp.fileManager.DiscoverAllFiles()
	if err != nil {
		return ModeHypersync, errors.Wrap(err, "failed to discover files")
	}

	// Get current state
	currentState := fp.progressManager.GetCurrentState()

	// Use file manager's logic to detect processing mode
	mode := fp.fileManager.DetectProcessingMode(allFiles, currentState)

	glog.V(2).Infof("Detected processing mode: %s (hypersync_complete=%v, last_committed_height=%d)",
		mode, currentState.HypersyncComplete, currentState.LastCommittedBlockHeight)

	return mode, nil
}

// startHypersyncProcessing starts hypersync chunk processing
func (fp *FileProcessor) startHypersyncProcessing() error {
	glog.Infof("Starting hypersync processing")

	// Start hypersync processor
	if err := fp.hypersyncProcessor.StartHypersyncProcessing(); err != nil {
		return errors.Wrap(err, "failed to start hypersync processing")
	}

	// Monitor for completion or stop signal
	go fp.monitorHypersyncProgress()

	return nil
}

// startCommittedBlockProcessing starts committed block file processing
func (fp *FileProcessor) startCommittedBlockProcessing() error {
	glog.Infof("Starting committed block processing")

	// Start committed block processing
	go fp.processCommittedBlockLoop()

	return nil
}

// startMempoolProcessing starts mempool file processing
func (fp *FileProcessor) startMempoolProcessing() error {
	glog.Infof("Starting mempool processing")

	// Start mempool processing
	go fp.processMempoolLoop()

	return nil
}

// monitorHypersyncProgress monitors hypersync progress and handles transition
func (fp *FileProcessor) monitorHypersyncProgress() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fp.stopChan:
			return
		case <-ticker.C:
			// Check if hypersync is complete
			state := fp.progressManager.GetCurrentState()
			if state.HypersyncComplete {
				glog.Infof("Hypersync complete, transitioning to committed block processing")

				// Start committed block processing
				if err := fp.startCommittedBlockProcessing(); err != nil {
					glog.Errorf("Failed to start committed block processing: %v", err)
				}
				return
			}

			// Log progress
			stats := fp.hypersyncProcessor.GetProcessingStats()
			glog.V(2).Infof("Hypersync progress: %v", stats)
		}
	}
}

// monitorCommittedBlockFiles monitors for new committed block files
func (fp *FileProcessor) monitorCommittedBlockFiles() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fp.stopChan:
			return
		case <-ticker.C:
			// Discover files and check for new committed blocks
			allFiles, err := fp.fileManager.DiscoverAllFiles()
			if err != nil {
				glog.Errorf("Failed to discover files: %v", err)
				continue
			}

			state := fp.progressManager.GetCurrentState()
			nextFile, err := fp.fileManager.GetNextCommittedBlockFile(allFiles, state.LastCommittedBlockHeight)
			if err != nil {
				glog.Errorf("Failed to get next committed block file: %v", err)
				continue
			}

			if nextFile != nil {
				glog.V(2).Infof("Found new committed block file: %s", fp.fileManager.GetFileDisplayName(nextFile))

				// TODO: Process the committed block file
				// This will be implemented in Phase 1C

				// For now, just check if we should transition to mempool mode
				mempoolFiles := fp.fileManager.GetFilesByType(allFiles, FileTypeMempoolDiff)
				if len(mempoolFiles) > 0 {
					glog.Infof("Transitioning to mempool processing")
					go fp.startMempoolProcessing()
					return
				}
			}
		}
	}
}

// monitorMempoolFiles monitors for new mempool files
func (fp *FileProcessor) monitorMempoolFiles() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-fp.stopChan:
			return
		case <-ticker.C:
			// Discover files and check for new mempool files
			allFiles, err := fp.fileManager.DiscoverAllFiles()
			if err != nil {
				glog.Errorf("Failed to discover files: %v", err)
				continue
			}

			state := fp.progressManager.GetCurrentState()
			nextFile, err := fp.fileManager.GetNextMempoolFile(allFiles, state.CurrentMempoolBlockHeight, state.LastMempoolTimestamp)
			if err != nil {
				glog.Errorf("Failed to get next mempool file: %v", err)
				continue
			}

			if nextFile != nil {
				glog.V(2).Infof("Found new mempool file: %s", fp.fileManager.GetFileDisplayName(nextFile))

				// TODO: Process the mempool file
				// This will be implemented in Phase 1C
			}

			// Check if a new committed block appeared
			nextCommittedFile, err := fp.fileManager.GetNextCommittedBlockFile(allFiles, state.LastCommittedBlockHeight)
			if err != nil {
				glog.Errorf("Failed to check for new committed block: %v", err)
				continue
			}

			if nextCommittedFile != nil {
				glog.Infof("New committed block detected, reverting mempool and transitioning")

				// TODO: Revert mempool using ancestral records
				// This will be implemented in Phase 1C

				go fp.startCommittedBlockProcessing()
				return
			}
		}
	}
}

// GetCurrentState returns the current processing state
func (fp *FileProcessor) GetCurrentState() *ProgressState {
	return fp.progressManager.GetCurrentState()
}

// GetProcessingStats returns comprehensive processing statistics
func (fp *FileProcessor) GetProcessingStats() map[string]interface{} {
	state := fp.progressManager.GetCurrentState()

	stats := map[string]interface{}{
		"is_running": fp.isRunning,
		"mode":       state.Mode.String(),
		"progress":   fp.progressManager.String(),
	}

	// Add mode-specific stats
	if state.Mode == ModeHypersync {
		hypersyncStats := fp.hypersyncProcessor.GetProcessingStats()
		stats["hypersync"] = hypersyncStats
	}

	return stats
}

// IsRunning returns whether the file processor is currently running
func (fp *FileProcessor) IsRunning() bool {
	return fp.isRunning
}

// GetFileManager returns the file manager (for testing/debugging)
func (fp *FileProcessor) GetFileManager() *FileManager {
	return fp.fileManager
}

// GetProgressManager returns the progress manager (for testing/debugging)
func (fp *FileProcessor) GetProgressManager() *ProgressManager {
	return fp.progressManager
}

// processCommittedBlockLoop processes committed block files using the CommittedBlockProcessor
func (fp *FileProcessor) processCommittedBlockLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fp.stopChan:
			return
		case <-ticker.C:
			// Process next committed block
			result, err := fp.committedBlockProcessor.ProcessNextCommittedBlock()
			if err != nil {
				glog.Errorf("Error processing committed block: %v", err)
				continue
			}

			if result == nil {
				// No more committed blocks to process, check for mempool transition
				if fp.committedBlockProcessor.shouldTransitionToMempool() {
					glog.Infof("Transitioning from committed blocks to mempool processing")
					go fp.startMempoolProcessing()
					return
				}
				continue
			}

			if result.Error != nil {
				glog.Errorf("Committed block processing error: %v", result.Error)
				continue
			}

			glog.V(2).Infof("Processed committed block %d: %d entries in %v",
				result.BlockHeight, result.EntriesProcessed, result.ProcessingTime)
		}
	}
}

// processMempoolLoop processes mempool files using the MempoolProcessor
func (fp *FileProcessor) processMempoolLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-fp.stopChan:
			return
		case <-ticker.C:
			// Check if we should transition back to committed blocks
			if fp.mempoolProcessor.shouldTransitionToCommittedBlocks() {
				glog.Infof("New committed block detected, transitioning from mempool processing")
				go fp.startCommittedBlockProcessing()
				return
			}

			// Process next mempool file
			result, err := fp.mempoolProcessor.ProcessNextMempoolFile()
			if err != nil {
				glog.Errorf("Error processing mempool file: %v", err)
				continue
			}

			if result == nil {
				// No more mempool files to process for now
				continue
			}

			if result.Error != nil {
				glog.Errorf("Mempool processing error: %v", result.Error)
				continue
			}

			glog.V(2).Infof("Processed mempool file: %s (%d entries in %v)",
				result.FilePath, result.EntriesProcessed, result.ProcessingTime)
		}
	}
}
