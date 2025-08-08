package consumer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/deso-protocol/core/lib"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// MempoolProcessor manages the processing of sequential mempool diff files
type MempoolProcessor struct {
	fileManager      *FileManager
	progressManager  *ProgressManager
	ancestralManager *AncestralRecordManager
	dataHandler      StateSyncerDataHandler

	// Configuration
	batchSize uint64

	// State tracking
	processedFiles     map[string]bool
	currentBlockHeight uint64

	// Processing control
	stopProcessing chan bool

	currentBatchType lib.EncoderType
}

// MempoolResult represents the result of processing a mempool file
type MempoolResult struct {
	FilePath         string
	BlockHeight      uint64
	Timestamp        int64
	EntriesProcessed int
	ProcessingTime   time.Duration
	Error            error
}

// NewMempoolProcessor creates a new mempool processor
func NewMempoolProcessor(
	fileManager *FileManager,
	progressManager *ProgressManager,
	ancestralManager *AncestralRecordManager,
	dataHandler StateSyncerDataHandler,
	batchSize uint64,
) *MempoolProcessor {
	return &MempoolProcessor{
		fileManager:      fileManager,
		progressManager:  progressManager,
		ancestralManager: ancestralManager,
		dataHandler:      dataHandler,
		batchSize:        batchSize,
		processedFiles:   make(map[string]bool),
		stopProcessing:   make(chan bool, 1),
	}
}

// ProcessAllAvailableMempoolFiles discovers files once and processes all available mempool files for the current block height only
func (mp *MempoolProcessor) ProcessAllAvailableMempoolFiles() ([]*MempoolResult, error) {
	// Discover all files once
	allFiles, err := mp.fileManager.DiscoverAllFiles()
	if err != nil {
		return nil, errors.Wrap(err, "failed to discover files")
	}

	// Get current progress
	state := mp.progressManager.GetCurrentState()

	// Get all mempool files sorted by timestamp
	mempoolFiles := mp.fileManager.GetFilesByType(allFiles, FileTypeMempoolDiff)

	var results []*MempoolResult
	var processedCount int

	// Process files for current block height only
	for _, file := range mempoolFiles {
		// Skip files that are already processed
		if file.Timestamp <= state.LastMempoolTimestamp {
			continue
		}

		// If this file is from the current block height, process it
		if file.BlockHeight == state.CurrentMempoolBlockHeight {
			glog.V(2).Infof("Processing mempool file: %s (block %d, timestamp %d)",
				file.Path, file.BlockHeight, file.Timestamp)

			// Log progress every 25 files
			processedCount++
			if processedCount%25 == 0 {
				fmt.Printf("📊 MEMPOOL PROGRESS: Processed %d mempool files (current: block %d, timestamp %d)\n",
					processedCount, file.BlockHeight, file.Timestamp)
			}

			result, err := mp.processMempoolFile(file)
			if err != nil {
				return results, errors.Wrapf(err, "failed to process mempool file %s", file.Path)
			}

			if result.Error != nil {
				// Stop processing on error but return what we've processed so far
				results = append(results, result)
				return results, nil
			}

			results = append(results, result)

			// Update our local state reference for next iteration
			state = mp.progressManager.GetCurrentState()

		} else if file.BlockHeight > state.CurrentMempoolBlockHeight {
			// If we encounter a file from a higher block height, stop processing
			// This indicates a block transition should happen
			glog.V(2).Infof("Encountered mempool file from higher block height %d (current: %d), stopping batch processing",
				file.BlockHeight, state.CurrentMempoolBlockHeight)
			break
		}
		// Files from lower block heights are ignored (shouldn't happen with proper sorting)
	}

	if processedCount > 0 {
		fmt.Printf("📊 MEMPOOL BATCH: Processed %d mempool files for block %d in this batch\n",
			processedCount, state.CurrentMempoolBlockHeight)
	}

	return results, nil
}

// ProcessNextMempoolFile finds and processes the next single available mempool diff file
func (mp *MempoolProcessor) ProcessNextMempoolFile() (*MempoolResult, error) {
	// Discover all files
	allFiles, err := mp.fileManager.DiscoverAllFiles()
	if err != nil {
		return nil, errors.Wrap(err, "failed to discover files")
	}

	// Get current progress
	state := mp.progressManager.GetCurrentState()

	// Get all mempool files sorted by timestamp
	mempoolFiles := mp.fileManager.GetFilesByType(allFiles, FileTypeMempoolDiff)

	// Find the next unprocessed file for the current block height
	for _, file := range mempoolFiles {
		// Skip files that are already processed
		if file.Timestamp <= state.LastMempoolTimestamp {
			continue
		}

		// If this file is from the current block height, process it
		if file.BlockHeight == state.CurrentMempoolBlockHeight {
			glog.V(2).Infof("Processing next mempool file: %s (block %d, timestamp %d)",
				file.Path, file.BlockHeight, file.Timestamp)

			result, err := mp.processMempoolFile(file)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to process mempool file %s", file.Path)
			}

			return result, nil
		} else if file.BlockHeight > state.CurrentMempoolBlockHeight {
			// If we encounter a file from a higher block height, stop processing
			glog.V(2).Infof("Encountered mempool file from higher block height %d (current: %d), no more files to process",
				file.BlockHeight, state.CurrentMempoolBlockHeight)
			break
		}
		// Files from lower block heights are ignored (shouldn't happen with proper sorting)
	}

	return nil, nil // No files to process
}

// processMempoolFile processes a single mempool diff file
func (mp *MempoolProcessor) processMempoolFile(mempoolFile *FileInfo) (*MempoolResult, error) {
	startTime := time.Now()

	// Process the mempool file
	entriesProcessed, err := mp.readAndProcessMempoolFile(mempoolFile)

	processingTime := time.Since(startTime)

	result := &MempoolResult{
		FilePath:         mempoolFile.Path,
		BlockHeight:      mempoolFile.BlockHeight,
		Timestamp:        mempoolFile.Timestamp,
		EntriesProcessed: entriesProcessed,
		ProcessingTime:   processingTime,
		Error:            err,
	}

	if err != nil {
		return result, nil
	}

	// Update progress
	if err := mp.progressManager.UpdateMempoolProgress(mempoolFile.Path, mempoolFile.Timestamp); err != nil {
		glog.Errorf("Failed to update mempool progress: %v", err)
	}

	// Mark file as processed
	mp.processedFiles[mempoolFile.Path] = true
	mp.currentBlockHeight = mempoolFile.BlockHeight

	glog.V(2).Infof("Mempool file processed successfully: %d entries in %v",
		entriesProcessed, processingTime)

	return result, nil
}

// readAndProcessMempoolFile reads a mempool diff file and processes its entries
func (mp *MempoolProcessor) readAndProcessMempoolFile(mempoolFile *FileInfo) (int, error) {
	file, err := os.Open(mempoolFile.Path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open mempool file: %s", mempoolFile.Path)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	entriesProcessed := 0
	var batchEntries []*lib.StateChangeEntry

	// Read entries from badger backup format (same as committed blocks and hypersync)
	for {
		// Read length (4 bytes)
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return entriesProcessed, errors.Wrap(err, "failed to read entry length")
		}

		// Read CRC (4 bytes) - we don't validate it for now
		var crc uint32
		if err := binary.Read(reader, binary.LittleEndian, &crc); err != nil {
			return entriesProcessed, errors.Wrap(err, "failed to read entry CRC")
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return entriesProcessed, errors.Wrap(err, "failed to read entry data")
		}

		// Parse KVList
		var kvList pb.KVList
		if err := proto.Unmarshal(data, &kvList); err != nil {
			return entriesProcessed, errors.Wrap(err, "failed to unmarshal KVList")
		}

		// Convert KV pairs to StateChangeEntries
		for _, kv := range kvList.Kv {
			stateChangeEntry, err := mp.kvToStateChangeEntry(kv, mempoolFile.BlockHeight, mempoolFile.Timestamp)
			if err != nil {
				glog.Warningf("Failed to convert KV to StateChangeEntry: %v", err)
				continue
			}

			// Process batch if it reaches the batch size or if the encoder type changes
			if (len(batchEntries) > 0 && stateChangeEntry.EncoderType != mp.currentBatchType) || uint64(len(batchEntries)) >= mp.batchSize {
				// Process the current batch with the correct type
				if err := mp.dataHandler.HandleEntryBatch(batchEntries, true); err != nil {
					return entriesProcessed, errors.Wrap(err, "failed to handle entry batch")
				}
				batchEntries = []*lib.StateChangeEntry{}
			}

			// Update current batch type and add entry to current batch
			mp.currentBatchType = stateChangeEntry.EncoderType
			batchEntries = append(batchEntries, stateChangeEntry)
			entriesProcessed++
		}
	}

	// Process remaining entries in batch
	if len(batchEntries) > 0 {
		mp.currentBatchType = batchEntries[0].EncoderType
		if err := mp.dataHandler.HandleEntryBatch(batchEntries, true); err != nil {
			return entriesProcessed, errors.Wrap(err, "failed to handle final entry batch")
		}
	}

	return entriesProcessed, nil
}

// kvToStateChangeEntry converts a protobuf KV to a StateChangeEntry for mempool entries
func (mp *MempoolProcessor) kvToStateChangeEntry(kv *pb.KV, blockHeight uint64, timestamp int64) (*lib.StateChangeEntry, error) {
	stateChangeEntry := &lib.StateChangeEntry{
		OperationType: lib.DbOperationTypeUpsert, // Mempool entries are usually upserts
		KeyBytes:      kv.Key,
		EncoderBytes:  kv.Value,
		BlockHeight:   blockHeight,
	}

	// Try to decode the encoder type if possible
	if isEncoder, encoder := lib.StateKeyToDeSoEncoder(kv.Key); isEncoder && encoder != nil {
		stateChangeEntry.EncoderType = encoder.GetEncoderType()

		// Handle special encoding for blocks and block nodes (like state syncer does)
		switch encoder.GetEncoderType() {
		case lib.EncoderTypeBlock:
			stateChangeEntry.EncoderBytes = lib.AddEncoderMetadataToMsgDeSoBlockBytes(kv.Value, blockHeight)
		case lib.EncoderTypeBlockNode:
			stateChangeEntry.EncoderBytes = lib.AddEncoderMetadataToBlockNodeBytes(kv.Value, blockHeight)
		default:
			stateChangeEntry.EncoderBytes = kv.Value
		}

		if len(stateChangeEntry.EncoderBytes) > 0 {
			dst := encoder.GetEncoderType().New()
			reader := bytes.NewReader(stateChangeEntry.EncoderBytes)
			if ok, err := lib.DecodeFromBytes(dst, reader); ok && err == nil {
				stateChangeEntry.Encoder = dst
			}
		}
	} else {
		// Try to decode from state key
		keyEncoder, err := lib.DecodeStateKey(kv.Key, kv.Value)
		if err == nil {
			stateChangeEntry.EncoderType = keyEncoder.GetEncoderType()
			stateChangeEntry.Encoder = keyEncoder
			stateChangeEntry.EncoderBytes = nil
		}
	}

	return stateChangeEntry, nil
}

// ProcessMempoolSequence processes mempool files in sequence for the current block
func (mp *MempoolProcessor) ProcessMempoolSequence() error {
	for {
		select {
		case <-mp.stopProcessing:
			return nil
		default:
			result, err := mp.ProcessNextMempoolFile()
			if err != nil {
				return errors.Wrap(err, "failed to process mempool file")
			}

			if result == nil {
				// No more mempool files to process for current block
				return nil
			}

			if result.Error != nil {
				glog.Errorf("Error processing mempool file %s: %v", result.FilePath, result.Error)
				return result.Error
			}

			// Check if a new committed block appeared (requires transition)
			if mp.shouldTransitionToCommittedBlocks() {
				glog.Infof("New committed block detected, transitioning from mempool processing")
				return nil
			}
		}
	}
}

// shouldTransitionToCommittedBlocks determines if we should transition to committed block processing
func (mp *MempoolProcessor) shouldTransitionToCommittedBlocks() bool {
	// Check if there are new committed block files
	allFiles, err := mp.fileManager.DiscoverAllFiles()
	if err != nil {
		glog.Errorf("Failed to discover files for transition check: %v", err)
		return false
	}

	state := mp.progressManager.GetCurrentState()
	committedFiles := mp.fileManager.GetFilesByType(allFiles, FileTypeCommittedBlock)

	for _, file := range committedFiles {
		if file.BlockHeight > state.LastCommittedBlockHeight {
			return true
		}
	}

	return false
}

// ProcessMempoolFilesForBlock processes all mempool files for a specific block height
func (mp *MempoolProcessor) ProcessMempoolFilesForBlock(blockHeight uint64) error {
	// Discover all files
	allFiles, err := mp.fileManager.DiscoverAllFiles()
	if err != nil {
		return errors.Wrap(err, "failed to discover files")
	}

	// Get mempool files for this block height
	mempoolFiles := mp.fileManager.GetFilesByType(allFiles, FileTypeMempoolDiff)

	// Filter to files for this block height and sort by timestamp
	var blockMempoolFiles []*FileInfo
	for _, file := range mempoolFiles {
		if file.BlockHeight == blockHeight {
			blockMempoolFiles = append(blockMempoolFiles, file)
		}
	}

	if len(blockMempoolFiles) == 0 {
		return nil // No mempool files for this block
	}

	// Sort by timestamp (chronological order)
	sort.Slice(blockMempoolFiles, func(i, j int) bool {
		return blockMempoolFiles[i].Timestamp < blockMempoolFiles[j].Timestamp
	})

	glog.V(2).Infof("Processing %d mempool files for block %d", len(blockMempoolFiles), blockHeight)

	// Process files in sequence
	for _, file := range blockMempoolFiles {
		if mp.processedFiles[file.Path] {
			continue // Already processed
		}

		result, err := mp.processMempoolFile(file)
		if err != nil {
			return errors.Wrapf(err, "failed to process mempool file: %s", file.Path)
		}

		if result.Error != nil {
			return errors.Wrapf(result.Error, "error in mempool file: %s", file.Path)
		}
	}

	return nil
}

// StopProcessing stops the mempool processing
func (mp *MempoolProcessor) StopProcessing() {
	glog.Infof("Stopping mempool processing...")

	// Use select with default to prevent blocking if channel is full or no receiver
	select {
	case mp.stopProcessing <- true:
		// Signal sent successfully
	default:
		// Channel is full or no receiver, which is fine
		glog.V(2).Infof("Stop signal already sent or no receiver")
	}

	glog.Infof("Mempool processing stopped")
}

// GetProcessingStats returns statistics about mempool processing
func (mp *MempoolProcessor) GetProcessingStats() map[string]interface{} {
	state := mp.progressManager.GetCurrentState()

	return map[string]interface{}{
		"mode":                  "mempool",
		"current_block_height":  state.CurrentMempoolBlockHeight,
		"last_timestamp":        state.LastMempoolTimestamp,
		"processed_files_count": len(mp.processedFiles),
		"applied_files":         len(state.AppliedMempoolFiles),
		"batch_size":            mp.batchSize,
	}
}

// IsFileProcessed returns whether a specific file has been processed
func (mp *MempoolProcessor) IsFileProcessed(filePath string) bool {
	return mp.processedFiles[filePath]
}

// Reset clears the processing state
func (mp *MempoolProcessor) Reset() {
	mp.processedFiles = make(map[string]bool)
	mp.currentBlockHeight = 0
}

// GetCurrentBlockHeight returns the current block height being processed
func (mp *MempoolProcessor) GetCurrentBlockHeight() uint64 {
	return mp.currentBlockHeight
}

// GetProcessedFilesCount returns the number of processed files
func (mp *MempoolProcessor) GetProcessedFilesCount() int {
	return len(mp.processedFiles)
}
