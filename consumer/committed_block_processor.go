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
		stopProcessing:   make(chan bool),
	}
}

// ProcessNextCommittedBlock finds and processes the next available committed block file
func (cbp *CommittedBlockProcessor) ProcessNextCommittedBlock() (*CommittedBlockResult, error) {
	// Discover all files
	allFiles, err := cbp.fileManager.DiscoverAllFiles()
	if err != nil {
		return nil, errors.Wrap(err, "failed to discover files")
	}

	// Get current progress
	state := cbp.progressManager.GetCurrentState()

	// Find next committed block file to process
	nextFile, err := cbp.fileManager.GetNextCommittedBlockFile(allFiles, state.LastCommittedBlockHeight)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get next committed block file")
	}

	if nextFile == nil {
		return nil, nil // No file to process
	}

	glog.V(2).Infof("Processing committed block %d: %s", nextFile.BlockHeight, nextFile.Path)

	return cbp.processCommittedBlockFile(nextFile)
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
			if !isCoreStateKey(kv.Key) {
				continue // Skip non-core state keys
			}

			stateChangeEntry, err := cbp.kvToStateChangeEntry(kv, blockFile.BlockHeight)
			if err != nil {
				glog.Warningf("Failed to convert KV to StateChangeEntry: %v", err)
				continue
			}

			batchEntries = append(batchEntries, stateChangeEntry)
			entriesProcessed++

			// Process batch if it reaches the batch size
			if uint64(len(batchEntries)) >= cbp.batchSize {
				if err := cbp.dataHandler.HandleEntryBatch(batchEntries, false); err != nil {
					return entriesProcessed, errors.Wrap(err, "failed to handle entry batch")
				}
				batchEntries = []*lib.StateChangeEntry{}
			}
		}
	}

	// Process remaining entries in batch
	if len(batchEntries) > 0 {
		if err := cbp.dataHandler.HandleEntryBatch(batchEntries, false); err != nil {
			return entriesProcessed, errors.Wrap(err, "failed to handle final entry batch")
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
		if len(kv.Value) > 0 {
			dst := encoder.GetEncoderType().New()
			reader := bytes.NewReader(kv.Value)
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
	cbp.stopProcessing <- true
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
