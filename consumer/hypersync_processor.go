package consumer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/deso-protocol/core/lib"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// HypersyncProcessor manages the processing of hypersync chunk files
type HypersyncProcessor struct {
	fileManager     *FileManager
	progressManager *ProgressManager
	dataHandler     StateSyncerDataHandler

	// Configuration
	maxConcurrentChunks int
	batchSize           uint64

	// State tracking
	processedChunks map[uint64]bool
	chunkMutex      sync.RWMutex

	// Processing control
	stopProcessing chan bool
	processingWG   sync.WaitGroup

	currentBatchType lib.EncoderType
}

// HypersyncChunkResult represents the result of processing a hypersync chunk
type HypersyncChunkResult struct {
	ChunkId          uint64
	EntriesProcessed int
	ProcessingTime   time.Duration
	Error            error
}

// NewHypersyncProcessor creates a new hypersync processor
func NewHypersyncProcessor(
	fileManager *FileManager,
	progressManager *ProgressManager,
	dataHandler StateSyncerDataHandler,
	maxConcurrentChunks int,
	batchSize uint64,
) *HypersyncProcessor {
	return &HypersyncProcessor{
		fileManager:         fileManager,
		progressManager:     progressManager,
		dataHandler:         dataHandler,
		maxConcurrentChunks: maxConcurrentChunks,
		batchSize:           batchSize,
		processedChunks:     make(map[uint64]bool),
		stopProcessing:      make(chan bool, 1),
	}
}

// StartHypersyncProcessing begins processing hypersync chunks asynchronously
func (hp *HypersyncProcessor) StartHypersyncProcessing() error {
	glog.Infof("Starting hypersync chunk processing with max %d concurrent chunks", hp.maxConcurrentChunks)

	// Emit hypersync start event
	if err := hp.dataHandler.HandleSyncEvent(SyncEventHypersyncStart); err != nil {
		return errors.Wrap(err, "failed to handle hypersync start event")
	}

	// Set processing mode to hypersync
	if err := hp.progressManager.SetMode(ModeHypersync); err != nil {
		return errors.Wrap(err, "failed to set hypersync mode")
	}

	// Start the main processing loop
	go hp.processHypersyncLoop()

	return nil
}

// processHypersyncLoop continuously processes available hypersync chunks
func (hp *HypersyncProcessor) processHypersyncLoop() {
	defer hp.processingWG.Wait() // Wait for all chunks to complete

	ticker := time.NewTicker(100 * time.Millisecond) // Much more reasonable interval
	defer ticker.Stop()

	semaphore := make(chan bool, hp.maxConcurrentChunks)
	results := make(chan HypersyncChunkResult, hp.maxConcurrentChunks)

	// Start result processor
	go hp.processChunkResults(results)

	for {
		select {
		case <-hp.stopProcessing:
			glog.Infof("Stopping hypersync processing")
			return
		case <-ticker.C:
			if err := hp.processAllAvailableHypersyncChunks(semaphore, results); err != nil {
				glog.Errorf("Error processing available chunks: %v", err)
			}
		}
	}
}

// processAllAvailableHypersyncChunks discovers files once and processes all available hypersync chunks
func (hp *HypersyncProcessor) processAllAvailableHypersyncChunks(semaphore chan bool, results chan HypersyncChunkResult) error {
	// Discover all files once
	allFiles, err := hp.fileManager.DiscoverAllFiles()
	if err != nil {
		return errors.Wrap(err, "failed to discover files")
	}

	// Get current progress
	state := hp.progressManager.GetCurrentState()

	// Find next hypersync chunks to process
	nextChunks, err := hp.fileManager.GetNextHypersyncFiles(allFiles, state.LastHypersyncChunk)
	if err != nil {
		return errors.Wrap(err, "failed to get next hypersync files")
	}

	if len(nextChunks) == 0 {
		// No more hypersync chunks - check if we should transition
		if hp.shouldTransitionFromHypersync(allFiles, state) {
			return hp.transitionFromHypersync()
		}
		return nil
	}

	// Sort chunks by chunk ID to ensure sequential processing
	sort.Slice(nextChunks, func(i, j int) bool {
		if nextChunks[i].ChunkId == nil || nextChunks[j].ChunkId == nil {
			return false
		}
		return *nextChunks[i].ChunkId < *nextChunks[j].ChunkId
	})

	var chunksStarted int

	// Process ALL available chunks up to our concurrency limit
	for _, chunk := range nextChunks {
		if chunk.ChunkId == nil {
			continue
		}

		// Check if already processed
		hp.chunkMutex.RLock()
		processed := hp.processedChunks[*chunk.ChunkId]
		hp.chunkMutex.RUnlock()

		if processed {
			continue
		}

		// Try to acquire semaphore (non-blocking)
		select {
		case semaphore <- true:
			// Start processing this chunk
			chunksStarted++

			// Log progress every 25 chunks
			if chunksStarted%25 == 0 {
				fmt.Printf("📊 HYPERSYNC PROGRESS: Started processing %d chunks (current: chunk %d)\n",
					chunksStarted, *chunk.ChunkId)
			}
			hp.processingWG.Add(1)
			go hp.processHypersyncChunk(chunk, semaphore, results)
		default:
			// No available slots, will try again next tick
			return nil
		}
	}

	if chunksStarted > 0 {
		fmt.Printf("📊 HYPERSYNC BATCH: Started processing %d chunks in this batch\n", chunksStarted)
	}

	return nil
}

// processAvailableChunks finds and processes available hypersync chunks (legacy method for compatibility)
func (hp *HypersyncProcessor) processAvailableChunks(semaphore chan bool, results chan HypersyncChunkResult) error {
	return hp.processAllAvailableHypersyncChunks(semaphore, results)
}

// processHypersyncChunk processes a single hypersync chunk file
func (hp *HypersyncProcessor) processHypersyncChunk(
	chunkFile *FileInfo,
	semaphore chan bool,
	results chan HypersyncChunkResult,
) {
	defer hp.processingWG.Done()
	defer func() { <-semaphore }() // Release semaphore

	startTime := time.Now()
	chunkId := uint64(0)
	if chunkFile.ChunkId != nil {
		chunkId = *chunkFile.ChunkId
	}

	glog.V(2).Infof("Processing hypersync chunk %d: %s", chunkId, chunkFile.Path)

	// Mark as being processed
	hp.chunkMutex.Lock()
	hp.processedChunks[chunkId] = true
	hp.chunkMutex.Unlock()

	// Read and process the chunk file
	entriesProcessed, err := hp.readAndProcessChunkFile(chunkFile)

	processingTime := time.Since(startTime)

	// Send result
	results <- HypersyncChunkResult{
		ChunkId:          chunkId,
		EntriesProcessed: entriesProcessed,
		ProcessingTime:   processingTime,
		Error:            err,
	}
}

// readAndProcessChunkFile reads a hypersync chunk file and processes its entries
func (hp *HypersyncProcessor) readAndProcessChunkFile(chunkFile *FileInfo) (int, error) {
	file, err := os.Open(chunkFile.Path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open chunk file: %s", chunkFile.Path)
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

			stateChangeEntry, err := hp.kvToStateChangeEntry(kv, chunkFile.BlockHeight)
			if err != nil {
				glog.Warningf("Failed to convert KV to StateChangeEntry: %v", err)
				continue
			}

			// Process batch if it reaches the batch size or if the encoder type changes
			if (len(batchEntries) > 0 && stateChangeEntry.EncoderType != hp.currentBatchType) || uint64(len(batchEntries)) >= hp.batchSize {
				// Process the current batch with the correct type
				if err := hp.dataHandler.HandleEntryBatch(batchEntries, false); err != nil {
					return entriesProcessed, errors.Wrap(err, "failed to handle entry batch")
				}
				batchEntries = []*lib.StateChangeEntry{}
			}

			// Update current batch type and add entry to current batch
			hp.currentBatchType = stateChangeEntry.EncoderType
			batchEntries = append(batchEntries, stateChangeEntry)
			entriesProcessed++
		}
	}

	// Process remaining entries in batch
	if len(batchEntries) > 0 {
		hp.currentBatchType = batchEntries[0].EncoderType
		if err := hp.dataHandler.HandleEntryBatch(batchEntries, false); err != nil {
			return entriesProcessed, errors.Wrap(err, "failed to handle final entry batch")
		}
	}

	return entriesProcessed, nil
}

// kvToStateChangeEntry converts a protobuf KV to a StateChangeEntry
func (hp *HypersyncProcessor) kvToStateChangeEntry(kv *pb.KV, blockHeight uint64) (*lib.StateChangeEntry, error) {
	stateChangeEntry := &lib.StateChangeEntry{
		OperationType: lib.DbOperationTypeUpsert, // Hypersync entries are always upserts
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
			if ok, err := lib.DecodeFromBytes(dst, bytes.NewReader(stateChangeEntry.EncoderBytes)); ok && err == nil {
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

// processChunkResults handles the results from chunk processing
func (hp *HypersyncProcessor) processChunkResults(results chan HypersyncChunkResult) {
	for result := range results {
		if result.Error != nil {
			glog.Errorf("Hypersync chunk %d processing failed: %v", result.ChunkId, result.Error)

			// Mark as not processed so it can be retried
			hp.chunkMutex.Lock()
			delete(hp.processedChunks, result.ChunkId)
			hp.chunkMutex.Unlock()
			continue
		}

		glog.V(2).Infof("Hypersync chunk %d processed successfully: %d entries in %v",
			result.ChunkId, result.EntriesProcessed, result.ProcessingTime)

		// Update progress
		if err := hp.progressManager.UpdateHypersyncProgress(result.ChunkId); err != nil {
			glog.Errorf("Failed to update hypersync progress: %v", err)
		}
	}
}

// shouldTransitionFromHypersync determines if we should transition from hypersync mode
func (hp *HypersyncProcessor) shouldTransitionFromHypersync(allFiles []*FileInfo, state *ProgressState) bool {
	// Check if there are any more hypersync files
	hypersyncFiles := hp.fileManager.GetFilesByType(allFiles, FileTypeHypersyncChunk)

	// If no hypersync files exist, or all have been processed
	if len(hypersyncFiles) == 0 {
		return true
	}

	// Check if all hypersync files have been processed
	for _, file := range hypersyncFiles {
		if file.ChunkId != nil && *file.ChunkId > state.LastHypersyncChunk {
			return false // Still have unprocessed chunks
		}
	}

	return true
}

// transitionFromHypersync transitions from hypersync mode to committed blocks mode
func (hp *HypersyncProcessor) transitionFromHypersync() error {
	glog.Infof("Transitioning from hypersync to committed blocks mode")

	// Wait for all processing to complete
	hp.processingWG.Wait()

	// Mark hypersync as complete
	if err := hp.progressManager.MarkHypersyncComplete(); err != nil {
		return errors.Wrap(err, "failed to mark hypersync complete")
	}

	// Emit hypersync complete event
	if err := hp.dataHandler.HandleSyncEvent(SyncEventHypersyncComplete); err != nil {
		return errors.Wrap(err, "failed to handle hypersync complete event")
	}

	// Emit block sync start event
	if err := hp.dataHandler.HandleSyncEvent(SyncEventBlocksyncStart); err != nil {
		return errors.Wrap(err, "failed to handle block sync start event")
	}

	// Stop the processing loop
	hp.stopProcessing <- true

	glog.Infof("Successfully transitioned from hypersync to committed blocks mode")
	return nil
}

// StopProcessing stops the hypersync processing
func (hp *HypersyncProcessor) StopProcessing() {
	glog.Infof("Stopping hypersync processing...")

	// Use select with default to prevent blocking if channel is full or no receiver
	select {
	case hp.stopProcessing <- true:
		// Signal sent successfully
	default:
		// Channel is full or no receiver, which is fine
		glog.V(2).Infof("Stop signal already sent or no receiver")
	}

	hp.processingWG.Wait()
	glog.Infof("Hypersync processing stopped")
}

// GetProcessingStats returns statistics about hypersync processing
func (hp *HypersyncProcessor) GetProcessingStats() map[string]interface{} {
	hp.chunkMutex.RLock()
	defer hp.chunkMutex.RUnlock()

	state := hp.progressManager.GetCurrentState()

	return map[string]interface{}{
		"mode":                 "hypersync",
		"last_processed_chunk": state.LastHypersyncChunk,
		"processed_chunks":     len(hp.processedChunks),
		"max_concurrent":       hp.maxConcurrentChunks,
		"batch_size":           hp.batchSize,
	}
}
