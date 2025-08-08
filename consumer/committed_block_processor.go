package consumer

import (
	"bufio"
	"bytes"
	"encoding/binary"
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

// CommittedBlockProcessor manages the processing of committed block diff files
type CommittedBlockProcessor struct {
	fileManager      *FileManager
	progressManager  *ProgressManager
	ancestralManager *AncestralRecordManager
	dataHandler      StateSyncerDataHandler

	// Configuration
	batchSize uint64

	// State tracking
	processedBlocks map[uint64]bool

	// Processing control
	stopProcessing chan bool

	currentBatchType lib.EncoderType
}

// CommittedBlockResult represents the result of processing a committed block
type CommittedBlockResult struct {
	BlockHeight      uint64
	EntriesProcessed int
	ProcessingTime   time.Duration
	Error            error
}

// NewCommittedBlockProcessor creates a new committed block processor
func NewCommittedBlockProcessor(
	fileManager *FileManager,
	progressManager *ProgressManager,
	ancestralManager *AncestralRecordManager,
	dataHandler StateSyncerDataHandler,
	batchSize uint64,
) *CommittedBlockProcessor {
	return &CommittedBlockProcessor{
		fileManager:      fileManager,
		progressManager:  progressManager,
		ancestralManager: ancestralManager,
		dataHandler:      dataHandler,
		batchSize:        batchSize,
		processedBlocks:  make(map[uint64]bool),
		stopProcessing:   make(chan bool, 1),
	}
}

// ProcessAllAvailableCommittedBlocks discovers files once and processes all available committed blocks
func (cbp *CommittedBlockProcessor) ProcessAllAvailableCommittedBlocks() ([]*CommittedBlockResult, error) {
	// Discover all files once
	allFiles, err := cbp.fileManager.DiscoverAllFiles()
	if err != nil {
		return nil, errors.Wrap(err, "failed to discover files")
	}

	// Get current progress
	state := cbp.progressManager.GetCurrentState()

	// Get all committed block files sorted by height
	committedFiles := cbp.fileManager.GetFilesByType(allFiles, FileTypeCommittedBlock)

	var results []*CommittedBlockResult

	// Process all available files in sequence
	for _, file := range committedFiles {
		if file.BlockHeight <= state.LastCommittedBlockHeight {
			continue // Already processed
		}

		glog.V(2).Infof("Processing committed block %d: %s", file.BlockHeight, file.Path)

		result, err := cbp.processCommittedBlockFile(file)
		if err != nil {
			return results, errors.Wrapf(err, "failed to process block %d", file.BlockHeight)
		}

		if result.Error != nil {
			// Stop processing on error but return what we've processed so far
			results = append(results, result)
			return results, nil
		}

		results = append(results, result)

		// Update our local state reference for next iteration
		state = cbp.progressManager.GetCurrentState()
	}

	return results, nil
}

// ProcessNextCommittedBlock finds and processes the next available committed block file (legacy method for compatibility)
func (cbp *CommittedBlockProcessor) ProcessNextCommittedBlock() (*CommittedBlockResult, error) {
	results, err := cbp.ProcessAllAvailableCommittedBlocks()
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, nil // No files to process
	}

	// Return the first result for compatibility
	return results[0], nil
}

// processCommittedBlockFile processes a single committed block diff file
func (cbp *CommittedBlockProcessor) processCommittedBlockFile(blockFile *FileInfo) (*CommittedBlockResult, error) {
	startTime := time.Now()
	blockHeight := blockFile.BlockHeight

	// Check if we need to revert mempool first
	if err := cbp.revertMempoolForNewBlock(blockHeight); err != nil {
		return &CommittedBlockResult{
			BlockHeight: blockHeight,
			Error:       errors.Wrap(err, "failed to revert mempool"),
		}, nil
	}

	// Process the block file
	entriesProcessed, err := cbp.readAndProcessBlockFile(blockFile)

	processingTime := time.Since(startTime)

	result := &CommittedBlockResult{
		BlockHeight:      blockHeight,
		EntriesProcessed: entriesProcessed,
		ProcessingTime:   processingTime,
		Error:            err,
	}

	if err != nil {
		return result, nil
	}

	// Update progress
	if err := cbp.progressManager.UpdateCommittedBlockProgress(blockHeight); err != nil {
		glog.Errorf("Failed to update committed block progress: %v", err)
	}

	// Mark block as processed
	cbp.processedBlocks[blockHeight] = true

	glog.V(2).Infof("Committed block %d processed successfully: %d entries in %v",
		blockHeight, entriesProcessed, processingTime)

	return result, nil
}

// revertMempoolForNewBlock reverts any applied mempool changes before processing a new block
func (cbp *CommittedBlockProcessor) revertMempoolForNewBlock(blockHeight uint64) error {
	// Get current state
	state := cbp.progressManager.GetCurrentState()

	// If we're processing the same block height as current mempool, no revert needed
	if state.CurrentMempoolBlockHeight == blockHeight {
		// Find ancestral files for this block height to revert mempool
		allFiles, err := cbp.fileManager.DiscoverAllFiles()
		if err != nil {
			return errors.Wrap(err, "failed to discover files for revert")
		}

		ancestralFiles := cbp.fileManager.GetAncestralFilesForBlock(allFiles, blockHeight)

		if len(ancestralFiles) > 0 {
			glog.V(2).Infof("Reverting mempool changes for block %d using %d ancestral files",
				blockHeight, len(ancestralFiles))

			// Process ancestral files in reverse order (most recent first)
			sort.Slice(ancestralFiles, func(i, j int) bool {
				return ancestralFiles[i].Timestamp > ancestralFiles[j].Timestamp
			})

			if err := cbp.ancestralManager.RevertMempoolChangesFromFiles(ancestralFiles); err != nil {
				return errors.Wrap(err, "failed to revert mempool using ancestral files")
			}
		}
	}

	return nil
}

// readAndProcessBlockFile reads a committed block file and processes its entries
func (cbp *CommittedBlockProcessor) readAndProcessBlockFile(blockFile *FileInfo) (int, error) {
	file, err := os.Open(blockFile.Path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open block file: %s", blockFile.Path)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	entriesProcessed := 0
	var batchEntries []*lib.StateChangeEntry

	// Read entries from badger backup format
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

			stateChangeEntry, err := cbp.kvToStateChangeEntry(kv, blockFile.BlockHeight)
			if err != nil {
				glog.Warningf("Failed to convert KV to StateChangeEntry: %v", err)
				continue
			}

			// Process batch if it reaches the batch size or if the encoder type changes
			if (len(batchEntries) > 0 && stateChangeEntry.EncoderType != cbp.currentBatchType) || uint64(len(batchEntries)) >= cbp.batchSize {
				// Process the current batch with the correct type
				if err := cbp.dataHandler.HandleEntryBatch(batchEntries, false); err != nil {
					return entriesProcessed, errors.Wrapf(err, "failed to handle entry batch for encoder type %d", cbp.currentBatchType)
				}
				batchEntries = []*lib.StateChangeEntry{}
			}

			// Update current batch type and add entry to current batch
			cbp.currentBatchType = stateChangeEntry.EncoderType
			batchEntries = append(batchEntries, stateChangeEntry)
			entriesProcessed++
		}
	}

	// Process any remaining entries in the final batch
	if len(batchEntries) > 0 {
		cbp.currentBatchType = batchEntries[0].EncoderType
		if err := cbp.dataHandler.HandleEntryBatch(batchEntries, false); err != nil {
			return entriesProcessed, errors.Wrapf(err, "failed to handle final entry batch for encoder type %d", cbp.currentBatchType)
		}
	}

	return entriesProcessed, nil
}

// kvToStateChangeEntry converts a protobuf KV to a StateChangeEntry for committed blocks
func (cbp *CommittedBlockProcessor) kvToStateChangeEntry(kv *pb.KV, blockHeight uint64) (*lib.StateChangeEntry, error) {
	stateChangeEntry := &lib.StateChangeEntry{
		OperationType: lib.DbOperationTypeUpsert, // Committed block entries are always upserts
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
			} else {
				glog.V(3).Infof("Failed to decode value for encoder type %d: ok=%v, err=%v",
					encoder.GetEncoderType(), ok, err)
			}
		}
	} else {
		// Try to decode from state key
		keyEncoder, err := lib.DecodeStateKey(kv.Key, kv.Value)
		if err == nil {
			stateChangeEntry.EncoderType = keyEncoder.GetEncoderType()
			stateChangeEntry.Encoder = keyEncoder
			stateChangeEntry.EncoderBytes = nil
		} else {
			glog.V(3).Infof("Both decode methods failed for key prefix %d: %v", kv.Key[0], err)
		}
	}

	return stateChangeEntry, nil
}

// ProcessCommittedBlockSequence processes committed block files in sequence
func (cbp *CommittedBlockProcessor) ProcessCommittedBlockSequence() error {
	for {
		select {
		case <-cbp.stopProcessing:
			return nil
		default:
			result, err := cbp.ProcessNextCommittedBlock()
			if err != nil {
				return errors.Wrap(err, "failed to process committed block")
			}

			if result == nil {
				// No more blocks to process
				return nil
			}

			if result.Error != nil {
				glog.Errorf("Error processing committed block %d: %v", result.BlockHeight, result.Error)
				return result.Error
			}

			// Check if we should transition to mempool mode
			if cbp.shouldTransitionToMempool() {
				glog.Infof("Transitioning from committed blocks to mempool processing")
				return nil
			}
		}
	}
}

// shouldTransitionToMempool determines if we should transition to mempool processing
func (cbp *CommittedBlockProcessor) shouldTransitionToMempool() bool {
	// Check if there are mempool files for the current block height
	allFiles, err := cbp.fileManager.DiscoverAllFiles()
	if err != nil {
		glog.Errorf("Failed to discover files for transition check: %v", err)
		return false
	}

	state := cbp.progressManager.GetCurrentState()
	mempoolFiles := cbp.fileManager.GetFilesByType(allFiles, FileTypeMempoolDiff)

	for _, file := range mempoolFiles {
		if file.BlockHeight == state.LastCommittedBlockHeight {
			return true
		}
	}

	return false
}

// StopProcessing stops the committed block processing
func (cbp *CommittedBlockProcessor) StopProcessing() {
	glog.Infof("Stopping committed block processing...")

	// Use select with default to prevent blocking if channel is full or no receiver
	select {
	case cbp.stopProcessing <- true:
		// Signal sent successfully
	default:
		// Channel is full or no receiver, which is fine
		glog.V(2).Infof("Stop signal already sent or no receiver")
	}

	glog.Infof("Committed block processing stopped")
}

// GetProcessingStats returns statistics about committed block processing
func (cbp *CommittedBlockProcessor) GetProcessingStats() map[string]interface{} {
	state := cbp.progressManager.GetCurrentState()

	return map[string]interface{}{
		"mode":                   "committed_blocks",
		"last_processed_block":   state.LastCommittedBlockHeight,
		"processed_blocks_count": len(cbp.processedBlocks),
		"batch_size":             cbp.batchSize,
	}
}

// IsBlockProcessed returns whether a specific block has been processed
func (cbp *CommittedBlockProcessor) IsBlockProcessed(blockHeight uint64) bool {
	return cbp.processedBlocks[blockHeight]
}

// Reset clears the processing state
func (cbp *CommittedBlockProcessor) Reset() {
	cbp.processedBlocks = make(map[uint64]bool)
}
