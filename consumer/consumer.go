package consumer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/deso-protocol/core/lib"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	ConsumerProgressFilename = "consumer-progress.bin"
)

// StateSyncerConsumer is a struct that contains the persisted state that is needed to consume state changes from a file.
// This includes file readers, statuses, batch caches, and channels to facilitate multi-threaded processing.
type StateSyncerConsumer struct {
	// File that contains the state changes.
	StateChangeFile       *os.File
	StateChangeFileReader *bufio.Reader

	StateChangeMempoolFile           *os.File
	StateChangeMempoolFirstEntryFile *os.File
	StateChangeMempoolFileReader     *bufio.Reader

	// An ordered slice containing every mempool entry that has been applied to the database.
	AppliedMempoolEntries []*lib.StateChangeEntry

	CurrentConfirmedEntryFlushId uuid.UUID
	CurrentMempoolEntryFlushId   uuid.UUID

	// File that contains the byte indexes of the state change file that corresponds to db operations.
	StateChangeIndexFile *os.File
	// Index of the entry in the state change file that the consumer should start parsing at.
	LastScannedIndex uint64
	// File that contains the entry index of the last saved state change.
	ConsumerProgressFile *os.File
	ConsumerProgressDir  string

	// The data handler that will be used to process the state changes that the consumer parses.
	DataHandler StateSyncerDataHandler

	// An object that contains the state changes that have been parsed but not yet processed. Used for batching.
	BatchedEntries []*lib.StateChangeEntry
	// Whether the batched entries are from a committed block or are from mempool transactions.
	IsBatchMempool bool
	BytesInBatch   uint64

	// The maximum number of bytes to batch before inserting into the database.
	MaxBatchBytes uint64
	ThreadLimit   int
	ThreadMutex   sync.Mutex

	// Track whether we're currently hypersyncing.
	IsHypersyncing bool

	// Whether to wrap each batch in a db transaction.
	ExecuteTransactions bool

	// Track whether we're currently syncing from the beginning.
	SyncingFromBeginning bool

	// Whether to sync mempool entries, or only committed entries.
	SyncMempoolEntires bool

	// A counter to keep track of how many batches have been inserted.
	BatchCount uint64
	EntryCount uint64

	// Channel to enforce a max thread limit on the listener.
	DBBlockingChannel chan bool
	DBBlockingWG      sync.WaitGroup

	// Indexes to track asynchronous batch handling progress during hypersync.
	BatchIndexes []*BatchIndexInfo

	// Whether to stop the consumer.
	StopConsumer bool
}

func (consumer *StateSyncerConsumer) InitializeAndRun(
	stateChangeDir string, consumerProgressFilename string, batchBytes uint64,
	threadLimit int, syncMempool bool, handler StateSyncerDataHandler) error {
	// initialize the consumer
	err := consumer.initialize(stateChangeDir, consumerProgressFilename, batchBytes, threadLimit, syncMempool, handler)
	if err != nil && err.Error() != "EOF" {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error initializing consumer")
	}
	// If there are entries to read, processNewEntriesInFile an initial scan of the state change file.
	if err == nil || err.Error() != "EOF" {
		if _, _, err = consumer.processNewEntriesInFile(false); err != nil {
			return errors.Wrapf(err, "consumer.InitializeAndRun: Error running consumer")
		}
	}
	// After we've done an initial scan, create a watcher to handle any new writes to the state change file.
	if err = consumer.watchFileAndScanOnWrite(); err != nil {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error watching file")
	}
	return nil
}

// Open the state change file and the index file, and determine the byte index that the state syncer should start
// parsing at.
func (consumer *StateSyncerConsumer) initialize(stateChangeDir string, consumerProgressDir string, batchBytes uint64, threadLimit int, syncMempool bool, handler StateSyncerDataHandler) error {
	// Set up the data handler initial values.
	consumer.IsHypersyncing = false
	consumer.ExecuteTransactions = false
	consumer.SyncMempoolEntires = syncMempool
	consumer.BatchCount = 0
	consumer.EntryCount = 0
	consumer.MaxBatchBytes = batchBytes
	consumer.ThreadLimit = threadLimit
	consumer.DataHandler = handler
	lib.GlobalDeSoParams = *handler.GetParams()
	consumer.DBBlockingChannel = make(chan bool, threadLimit)
	consumer.AppliedMempoolEntries = make([]*lib.StateChangeEntry, 0)
	consumer.CurrentMempoolEntryFlushId = uuid.Nil
	consumer.CurrentConfirmedEntryFlushId = uuid.Nil

	stateChangeFilePath := filepath.Join(stateChangeDir, lib.StateChangeFileName)
	stateChangeIndexFilePath := filepath.Join(stateChangeDir, lib.StateChangeIndexFileName)
	stateChangeMempoolFilePath := filepath.Join(stateChangeDir, lib.StateChangeMempoolFileName)

	// Wait for the state changes file to be created. Once it has been created, open it.
	consumer.waitForStateChangesFile(stateChangeFilePath)

	// Create a new reader for the state change file.
	consumer.StateChangeFileReader = bufio.NewReader(consumer.StateChangeFile)

	// Create a new reader for the mempool file.
	if stateChangeMempoolFile, err := os.Open(stateChangeMempoolFilePath); err == nil {
		consumer.StateChangeMempoolFile = stateChangeMempoolFile
		consumer.StateChangeMempoolFileReader = bufio.NewReader(consumer.StateChangeMempoolFile)
	} else {
		return errors.Wrapf(err, "consumer.initialize: Error opening mempool state change file")
	}

	if stateChangeMempoolFile, err := os.Open(stateChangeMempoolFilePath); err == nil {
		consumer.StateChangeMempoolFirstEntryFile = stateChangeMempoolFile
	} else {
		return errors.Wrapf(err, "consumer.initialize: Error opening mempool state change file")
	}

	// Open the file that contains byte indexes for each entry in the state changes file.
	indexFile, err := os.Open(stateChangeIndexFilePath)
	if err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error opening indexFile")
	}
	consumer.StateChangeIndexFile = indexFile

	// Open the file that contains the entry index of the last saved state change.
	consumer.ConsumerProgressDir = consumerProgressDir
	consumerProgressFilePath := filepath.Join(consumerProgressDir, ConsumerProgressFilename)

	if consumerProgressFile, err := os.Open(consumerProgressFilePath); err == nil {
		consumer.ConsumerProgressFile = consumerProgressFile
	}

	// Get last entry index that was synced.
	lastEntrySyncedIdx, err := consumer.retrieveLastSyncedStateChangeEntryIndex()
	if err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error retrieving last synced state change entry index")
	}

	// If the last entry synced index is not 0, we are resuming a previous sync.
	// Revert the mempool transactions that were applied during the previous sync.
	if lastEntrySyncedIdx != 0 {
		err = consumer.revertStoredMempoolTransactions()
		if err != nil {
			return errors.Wrapf(err, "consumer.initialize: Error reverting mempool transactions")
		}
	}

	// Discover where we should start parsing the state change file.
	stateChangeFileByteIndex, err := consumer.retrieveFileIndexForDbOperation(lastEntrySyncedIdx)
	if err != nil {
		return errors.Wrapf(err, "consumer.intialize: Error retrieving file index for db operation")
	}

	// Set the batch count to the current batch on resume.
	currentBatch := stateChangeFileByteIndex / batchBytes
	consumer.BatchCount = currentBatch

	// Seek to the byte index that we should start parsing at.
	if _, err = consumer.StateChangeFile.Seek(int64(stateChangeFileByteIndex), 0); err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error seeking to byte index")
	}

	// If the byte index is 0, we are starting a fresh sync.
	if stateChangeFileByteIndex == 0 {
		consumer.SyncingFromBeginning = true
		if err = consumer.DataHandler.HandleSyncEvent(SyncEventStart); err != nil {
			return errors.Wrapf(err, "consumer.initialize: Error handling sync start event")
		}
	}

	// Check if we are starting a block sync, emit an event if so.
	err = consumer.checkBlockSyncStart()
	if err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error checking block sync start")
	}

	return nil
}

// processNewEntriesInFile reads the state change file and passes each entry to the data handler.
func (consumer *StateSyncerConsumer) processNewEntriesInFile(isMempool bool) (bool, bool, error) {
	revertTriggered := false
	entriesProcessed := false

	fileEOF := false
	// Read from the state change file until we reach the end.
	for !fileEOF {
		var err error
		var stateChangeEntry *lib.StateChangeEntry
		// Get the next state change entry from the state change file.
		stateChangeEntry, fileEOF, err = consumer.retrieveNextEntry(isMempool)
		if err != nil {
			// If the error is from the mempool file, don't kill the process, just log the error.
			if isMempool {
				glog.Errorf("consumer.processNewEntriesInFile: Error reading next mempool entry from file: %s", err.Error())
				break
			}
			return revertTriggered, entriesProcessed, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error reading next entry from file")
		}
		if fileEOF {
			break
		}
		entriesProcessed = true
		var entryRevertTriggered bool
		if !isMempool {
			entryRevertTriggered, err = consumer.SyncCommittedEntry(stateChangeEntry)
		} else {
			entryRevertTriggered, err = consumer.SyncMempoolEntry(stateChangeEntry)
		}

		// Update the overall revertTriggered flag if this entry triggered a revert
		revertTriggered = revertTriggered || entryRevertTriggered
		if err != nil {
			return revertTriggered, entriesProcessed, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error syncing committed entry")
		}
	}

	// Once we've reached the file EOF, process any remaining batched entries and cleanup.
	if err := consumer.cleanup(); err != nil {
		return revertTriggered, entriesProcessed, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error cleaning up")
	}

	// If we are syncing from the beginning, emit a sync end event.
	if consumer.SyncingFromBeginning && !isMempool && !consumer.IsHypersyncing {
		consumer.SyncingFromBeginning = false
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventComplete); err != nil {
			return revertTriggered, entriesProcessed, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error handling sync end event")
		}
	}
	return revertTriggered, entriesProcessed, nil
}

func (consumer *StateSyncerConsumer) SyncCommittedEntry(stateChangeEntry *lib.StateChangeEntry) (bool, error) {
	revertTriggered := false
	// If the entry is from a new flush (i.e. a new block), revert the current mempool entries before applying.
	if stateChangeEntry.FlushId != consumer.CurrentConfirmedEntryFlushId {

		if err := consumer.RevertMempoolEntries(); err != nil {
			return false, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error reverting mempool entries")
		}
		revertTriggered = true
		// Update the current block sync flush ID.
		consumer.CurrentConfirmedEntryFlushId = stateChangeEntry.FlushId
		if !consumer.IsHypersyncing {
			// Log the handling of the flush.
			fmt.Println("Now handling flush ", stateChangeEntry.FlushId.String())
		}
	}
	// Detect if this entry represets a sync state change and emit
	if err := consumer.detectAndHandleSyncEvent(stateChangeEntry); err != nil {
		return revertTriggered, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error detecting sync event")
	}
	// Handle the state change entry.
	if err := consumer.handleStateChangeEntry(stateChangeEntry, false); err != nil {
		return revertTriggered, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error handling state change entry")
	}
	return revertTriggered, nil
}

func (consumer *StateSyncerConsumer) SyncMempoolEntry(stateChangeEntry *lib.StateChangeEntry) (bool, error) {
	revertTriggered := false

	// If the entry is from a new flush (i.e. a new block), revert the current mempool entries before applying.
	if stateChangeEntry.FlushId != consumer.CurrentMempoolEntryFlushId {
		if err := consumer.RevertMempoolEntries(); err != nil {
			return false, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error reverting mempool entries")
		}
		revertTriggered = true
		consumer.CurrentMempoolEntryFlushId = stateChangeEntry.FlushId
	}

	// Handle the state change entry.
	if err := consumer.handleStateChangeEntry(stateChangeEntry, true); err != nil {
		return false, errors.Wrapf(err, "consumer.processNewEntriesInFile: Error handling state change entry")
	}

	// Add this entry to the list of applied mempool entries.
	consumer.AppliedMempoolEntries = append(consumer.AppliedMempoolEntries, stateChangeEntry)

	// Add this entry to the file log of applied mempool entries.
	consumer.saveMempoolProgressToFile(stateChangeEntry)
	return revertTriggered, nil
}

func (consumer *StateSyncerConsumer) RevertMempoolEntry(stateChangeEntry *lib.StateChangeEntry) error {
	// Create a copy of the stateChangeEntry.
	revertEntry := *stateChangeEntry

	// If the ancestral record is nil, we need to delete the entry.
	if revertEntry.AncestralRecord == nil {
		revertEntry.OperationType = lib.DbOperationTypeDelete
		revertEntry.Encoder = nil
	} else {
		// If the ancestral record exists, update the db record to that value.
		revertEntry.OperationType = lib.DbOperationTypeUpsert
		revertEntry.Encoder = revertEntry.AncestralRecord
		revertEntry.EncoderBytes = revertEntry.AncestralRecordBytes
	}

	// Handle the reverted state change entry.
	if err := consumer.handleStateChangeEntry(&revertEntry, true); err != nil {
		return errors.Wrapf(err, "consumer.processNewEntriesInFile: Error handling state change entry")
	}

	if len(consumer.AppliedMempoolEntries) == 0 {
		return nil
	}

	// Remove this entry from the list of applied mempool entries.
	consumer.AppliedMempoolEntries = consumer.AppliedMempoolEntries[:len(consumer.AppliedMempoolEntries)-1]
	return nil
}

func (consumer *StateSyncerConsumer) RevertMempoolEntries() error {
	// Execute any remaining batched transactions before executing the revert.
	if err := consumer.executeBatch(); err != nil {
		return errors.Wrapf(err, "consumer.revertMempoolEntries: Error executing batch")
	}

	// Revert all applied mempool entries in reverse order.
	for ii := len(consumer.AppliedMempoolEntries) - 1; ii >= 0; ii-- {
		if err := consumer.RevertMempoolEntry(consumer.AppliedMempoolEntries[ii]); err != nil {
			return errors.Wrapf(err, "consumer.revertMempoolEntries: Error reverting mempool entry")
		}
	}
	// Execute any remaining batched transactions before finalizing the revert.
	if err := consumer.executeBatch(); err != nil {
		return errors.Wrapf(err, "consumer.revertMempoolEntries: Error executing batch")
	}
	return nil
}

// readAndDecodeNextEntry reads the next state change entry from the state change file and decodes it as a deso encoder.
func (consumer *StateSyncerConsumer) readAndDecodeNextEntry(reader *bufio.Reader, file *os.File) (sce *lib.StateChangeEntry, eof bool, err error) {
	// Get the current position in the file
	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error getting current position in file")
	}
	// Get the size of the next state change entry.
	entryByteSize, err := lib.ReadUvarint(reader)
	if err != nil && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
		// If it's an unexpected EOF, log it and return true to signify EOF.
		glog.V(2).Infof("consumer.readAndDecodeNextEntry: Error reading from state change file: %v", err)

		// Reset the reader to the position before the unexpected EOF.
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, true, nil
	} else if err != nil {
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error reading from state change file")
	}

	if err = CheckSliceSize(int(entryByteSize)); err != nil {
		// Reset the reader.
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error checking slice size")
	}

	// Create a buffer to hold the entry.
	buffer := make([]byte, entryByteSize)
	bytesRead, err := io.ReadFull(reader, buffer)
	// If there are no bytes to read, return true to signify EOF.
	if bytesRead == 0 {
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, true, nil
	} else if err != nil && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
		// If it's an unexpected EOF, log it and return true to signify EOF.
		glog.V(2).Infof("consumer.readAndDecodeNextEntry: Error reading from state change file: %v", err)
		// Reset the reader to the position before the unexpected EOF.
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, true, nil
	} else if err != nil {
		// Reset the reader to the position before the unexpected EOF.
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error reading from state change file")
	} else if bytesRead < int(entryByteSize) {
		// Reset the reader to the position before the unexpected EOF.
		if _, err = file.Seek(currentPos, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error seeking to current position in file")
		}
		return nil, false, fmt.Errorf("consumer.readAndDecodeNextEntry: Not enough bytes read from state change file. Expected %d, got %d", entryByteSize, bytesRead)
	}

	// Decode the state change entry.
	stateChangeEntry := &lib.StateChangeEntry{}

	// Create deferred function to handle any panics that occur during decoding.
	defer func() {
		if r := recover(); r != nil {
			file.Seek(currentPos, io.SeekStart)
			err = fmt.Errorf("consumer.readAndDecodeNextEntry: Panic decoding entry: %v", r)
			eof = false
			sce = nil
		}

	}()
	if err = DecodeEntry(stateChangeEntry, buffer); err != nil {
		file.Seek(currentPos, io.SeekStart)
		return nil, false, errors.Wrapf(err, "consumer.readAndDecodeNextEntry: Error decoding entry")
	}

	return stateChangeEntry, false, err
}

// retrieveNextEntry reads the next StateChangeEntry bytes from the state change file and decode them.
func (consumer *StateSyncerConsumer) retrieveNextEntry(isMempool bool) (*lib.StateChangeEntry, bool, error) {
	var reader *bufio.Reader
	var file *os.File
	if isMempool {
		reader = consumer.StateChangeMempoolFileReader
		file = consumer.StateChangeMempoolFile
	} else {
		reader = consumer.StateChangeFileReader
		file = consumer.StateChangeFile
	}

	// If mempool, check first entry to see if the flush ID has changed.
	if isMempool {
		// Scan the first entry in the mempool file to see if the flush ID has changed.
		if _, err := consumer.StateChangeMempoolFirstEntryFile.Seek(0, io.SeekStart); err != nil {
			return nil, false, errors.Wrapf(err, "consumer.retrieveNextEntry: Error seeking to start of mempool file")
		}
		// Read the first mempool entry to see if the flush ID has changed.
		firstEntryReader := bufio.NewReader(consumer.StateChangeMempoolFirstEntryFile)

		mempoolFirstEntry, eof, err := consumer.readAndDecodeNextEntry(firstEntryReader, consumer.StateChangeMempoolFirstEntryFile)
		if eof {
			return nil, true, nil
		} else if err != nil {
			return nil, false, errors.Wrapf(err, "consumer.retrieveNextEntry: Error reading and decoding first mempool entry")
		}

		// If the flush ID has changed, revert the current mempool entries and reset the mempool reader.
		if mempoolFirstEntry.FlushId != consumer.CurrentMempoolEntryFlushId {
			if err = consumer.RevertMempoolEntries(); err != nil {
				return nil, false, errors.Wrapf(err, "consumer.retrieveNextEntry: Error reverting mempool entries")
			}
			// Set the flush ID to the new flush ID.
			consumer.CurrentMempoolEntryFlushId = mempoolFirstEntry.FlushId
			// Reset the mempool reader, so that the next entry read will be the first entry in the new flush.
			consumer.StateChangeMempoolFile.Seek(0, io.SeekStart)
			consumer.StateChangeMempoolFileReader = bufio.NewReader(consumer.StateChangeMempoolFile)
			// Set the reader to the newly reset mempool file reader.
			reader = consumer.StateChangeMempoolFileReader
		}
	}
	stateChangeEntry, eof, err := consumer.readAndDecodeNextEntry(reader, file)
	if eof {
		return nil, true, nil
	} else if err != nil {
		return nil, false, errors.Wrapf(err, "consumer.retrieveNextEntry: Error reading and decoding entry")
	}

	return stateChangeEntry, false, nil
}

// detectAndHandleSyncEvent determines if the state change entry represents a sync event and emits it to the data handler.
func (consumer *StateSyncerConsumer) detectAndHandleSyncEvent(stateChangeEntry *lib.StateChangeEntry) error {
	// Determine if hypersync is beginning or ending.
	if stateChangeEntry.OperationType == lib.DbOperationTypeInsert && !consumer.IsHypersyncing {
		consumer.IsHypersyncing = true
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventHypersyncStart); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync start event")
		}
	} else if stateChangeEntry.OperationType != lib.DbOperationTypeInsert && consumer.IsHypersyncing {
		// If the operation type is not an insert, we must have finished hypersyncing.
		// First, wait for any remaining batch threads to finish.
		consumer.DBBlockingWG.Wait()
		// Set the hypersyncing flag to false and close the channels.
		consumer.IsHypersyncing = false
		consumer.ExecuteTransactions = true
		close(consumer.DBBlockingChannel)
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventHypersyncComplete); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync complete event")
		}
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventBlocksyncStart); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync complete event")
		}
	} else if consumer.LastScannedIndex == 0 && stateChangeEntry.OperationType != lib.DbOperationTypeInsert {
		consumer.ExecuteTransactions = true
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventHypersyncComplete); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync complete event")
		}
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventBlocksyncStart); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync complete event")
		}
	}

	// Determine if we've reached a new transaction type during hypersync, log it if so.
	if consumer.IsHypersyncing && len(consumer.BatchedEntries) > 0 && stateChangeEntry.EncoderType != consumer.BatchedEntries[0].EncoderType {
		fmt.Printf("Now hypersyncing encoder type %d\n", stateChangeEntry.EncoderType)
	}

	return nil
}

// watchFileAndScanOnWrite continually triggers a new processNewEntriesInFile of the consumer. If there are any new changes that have been
// written, they will be captured by the processNewEntriesInFile, otherwise the processNewEntriesInFile will exit.
func (consumer *StateSyncerConsumer) watchFileAndScanOnWrite() (err error) {
	for !consumer.StopConsumer {
		err = func() error {
			// Short sleep to prevent busy-waiting.
			time.Sleep(25 * time.Millisecond)

			// If we are executing transactions, initiate a new transaction.
			// This should occur after hypersync is complete.
			if consumer.ExecuteTransactions {
				err = consumer.DataHandler.InitiateTransaction()
				if err != nil {
					return errors.Wrapf(err, "consumer.processNewEntriesInFile: Error initiating transaction")
				}
				defer func() {
					// Call CommitTransaction and handle any potential error.
					if commitErr := consumer.DataHandler.CommitTransaction(); commitErr != nil {
						// If there's an error, wrap it with additional context and assign it to the named return variable.
						err = fmt.Errorf("consumer.processNewEntriesInFile: error committing transaction: %w", commitErr)
					}
				}()
			}
			// Process any new committed entries.
			revertTriggered, _, err := consumer.processNewEntriesInFile(false)
			if err != nil {
				return errors.Wrapf(err, "consumer.watchFileAndScanOnWrite: Error scanning committed entries")
			}

			// Process any new mempool entries
			if consumer.SyncMempoolEntires {
				// If a revert was triggered during the committed entries, and we are executing transactions,
				// we need to process mempool entries UNTIL we see new entries get applied.
				// This is to ensure that any entries that were reverted but not included in the new block are
				// re-applied before the transaction is committed.
				if revertTriggered && consumer.ExecuteTransactions {
					for {
						_, entriesProcessed, err := consumer.processNewEntriesInFile(true)
						if err != nil {
							return errors.Wrapf(err, "consumer.watchFileAndScanOnWrite: Error scanning mempool entries")
						}
						// Break if we processed entries, since we've now synced the new mempool state
						if entriesProcessed {
							break
						}
						// Small sleep to prevent busy waiting
						time.Sleep(25 * time.Millisecond)
					}
				} else {
					// Just process once if no revert or not executing transactions
					_, _, err := consumer.processNewEntriesInFile(true)
					if err != nil {
						return errors.Wrapf(err, "consumer.watchFileAndScanOnWrite: Error scanning mempool entries")
					}
				}
			}

			return nil
		}()
		if err != nil {
			return errors.Wrapf(err, "consumer.watchFileAndScanOnWrite: Error processing new entries")
		}
	}
	return nil
}

// waitForStateChangesFile blocks execution until the state changes file is created, and then assigns it to the consumer.
// It blocks until the file is non-empty. This prevents the consumer from starting before the state changes file has been
// fully initialized, causing an EOF read error.
func (consumer *StateSyncerConsumer) waitForStateChangesFile(stateChangeFileName string) {
	for {
		// Attempt to open the state changes file. If it doesn't exist, wait 5 seconds and try again.
		if stateChangeFile, err := os.Open(stateChangeFileName); err == nil {
			consumer.StateChangeFile = stateChangeFile
			// Once the file successfully is open, check if it is empty. If it is, wait 5 seconds and try again.
			stateChangeFileInfo, err := stateChangeFile.Stat()
			if err == nil {
				// If the file is non-empty, break out of the loop and stop blocking the thread.
				if stateChangeFileInfo.Size() > 0 {
					break
				}
			}
		}
		fmt.Println("Waiting for state changes file to be created...")
		time.Sleep(5 * time.Second)
	}
}

// retrieveLastSyncedStateChangeEntryIndex looks up the last synced state change entry index from the consumer progress file.
// This is used to determine where to start scanning the state changes file from after a restart.
func (consumer *StateSyncerConsumer) retrieveLastSyncedStateChangeEntryIndex() (uint64, error) {
	// Attempt to open the consumer progress file. If it exists, it should have a single uint32 representing the
	// last StateChangeEntry index that was processed.
	if consumer.ConsumerProgressFile != nil {
		return getUint64FromFile(consumer.ConsumerProgressFile)
	}
	return 0, nil
}

// retrieveFileIndexForDbOperation retrieves the byte index in the state change file for the next db operation.
// It does this by reading the last saved entry index from the entry index file and multiplying it by 4 to get the
// byte index in the state change index file.
func (consumer *StateSyncerConsumer) retrieveFileIndexForDbOperation(startEntryIndex uint64) (uint64, error) {
	consumer.EntryCount = startEntryIndex
	consumer.LastScannedIndex = startEntryIndex
	// Find the byte index in the state change file for the next db operation. Each entry byte index is represented
	// in the index file as a uint64. This means the entry byte index exists at its consumer progress index * 8.
	entryIndexBytes := make([]byte, 8)
	fileBytesPosition := int64(startEntryIndex * 8)
	bytesRead, err := consumer.StateChangeIndexFile.ReadAt(entryIndexBytes, fileBytesPosition)
	if bytesRead == 0 {
		return consumer.retrieveFileIndexForDbOperation(startEntryIndex - 1)
	} else if err != nil {
		return 0, errors.Wrapf(err, "consumer.retrieveFileIndexForDbOperation: Error reading from state change index file")
	}
	// If we read no bytes, we're at EOF.
	if bytesRead == 0 {
		return 0, errors.New("consumer.retrieveFileIndexForDbOperation: EOF reached")
	}
	// If we read a non uint64 number of bytes, something is wrong.
	if bytesRead < 8 {
		return 0, errors.New("consumer.retrieveFileIndexForDbOperation: Too few bytes read")
	}

	// Use binary package to read a uint64 index from the byte slice representing the index of the db operation.
	dbIndex := binary.LittleEndian.Uint64(entryIndexBytes)
	return dbIndex, nil
}

// peekNextStateChangeEntry reads the next entry from the state change file without advancing the file pointer.
func (consumer *StateSyncerConsumer) peekNextStateChangeEntry(reader *bufio.Reader, file *os.File) (*lib.StateChangeEntry, error) {
	// Get the current byte position in the state change file.
	currentPos, err := file.Seek(0, io.SeekCurrent)

	// Read the next entry from the state change file.
	stateChangeEntry, _, err := consumer.readAndDecodeNextEntry(reader, file)
	if err != nil {
		return nil, errors.Wrapf(err, "consumer.peekNextStateChangeEntry: Error reading next entry")
	}

	// Seek back to the original position in the file.
	if _, err = consumer.StateChangeMempoolFile.Seek(currentPos, io.SeekStart); err != nil {
		return nil, errors.Wrapf(err, "consumer.retrieveNextEntry: Error seeking to current position in mempool file")
	}

	return stateChangeEntry, nil
}

// checkBlockSyncStart checks if the next entry in the state change file is a blocksync event. If it is, emit a
// SyncEventBlocksyncStart event to the data handler.
func (consumer *StateSyncerConsumer) checkBlockSyncStart() error {
	// Peek at the next state change entry
	nextStateChangeEntry, err := consumer.peekNextStateChangeEntry(consumer.StateChangeFileReader, consumer.StateChangeFile)
	if err != nil {
		return errors.Wrapf(err, "consumer.checkBlockSyncStart: Error peeking at next state change entry")
	}
	if nextStateChangeEntry == nil {
		return nil
	}
	if nextStateChangeEntry.OperationType != lib.DbOperationTypeInsert {
		consumer.ExecuteTransactions = true
		if err = consumer.DataHandler.HandleSyncEvent(SyncEventBlocksyncStart); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling blocksync start event")
		}
	}
	return nil
}

// saveConsumerProgressToFile saves the last StateChangeEntry index that was processed to the consumer progress file.
// This is represented as a single uint32 encoded to bytes.
func (consumer *StateSyncerConsumer) saveConsumerProgressToFile(entryIndex uint64) error {
	consumerProgressFilepath := filepath.Join(consumer.ConsumerProgressDir, ConsumerProgressFilename)
	// Create the file if it doesn't exist.
	file, err := createDirAndFile(consumerProgressFilepath)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error creating consumer progress file: %s", consumer.ConsumerProgressDir)
	}
	defer file.Close()

	// Write the entry index to the file.
	err = binary.Write(file, binary.LittleEndian, entryIndex)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error writing entry index to consumer progress file: %s", consumer.ConsumerProgressDir)
	}
	consumer.LastScannedIndex = entryIndex
	return nil
}

// saveMempoolProgressToFile appends the last applied mempool entry to the mempool progress file.
func (consumer *StateSyncerConsumer) saveMempoolProgressToFile(mempoolStateChangeEntry *lib.StateChangeEntry) error {
	mempoolStatusFilepath := filepath.Join(consumer.ConsumerProgressDir, lib.StateChangeMempoolFileName)

	// Create the file if it doesn't exist.
	file, err := createDirAndFile(mempoolStatusFilepath)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error creating applied mempool entries file: %s", consumer.ConsumerProgressDir)
	}
	defer file.Close()

	mempoolEntryBytes := lib.EncodeByteArray(lib.EncodeToBytes(mempoolStateChangeEntry.BlockHeight, mempoolStateChangeEntry))

	if _, err := file.Write(mempoolEntryBytes); err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error writing to applied mempool entries: %s", consumer.ConsumerProgressDir)
	}
	return nil
}

// revertStoredMempoolTransactions extracts all applied mempool entries from the mempool progress file and reverts them.
// This is used when re-starting the state syncer, so that the database is able to revert back to the last known chain-state.
func (consumer *StateSyncerConsumer) revertStoredMempoolTransactions() error {
	mempoolStatusFilepath := filepath.Join(consumer.ConsumerProgressDir, lib.StateChangeMempoolFileName)
	// Create the file if it doesn't exist.
	file, err := os.Open(mempoolStatusFilepath)
	if os.IsNotExist(err) {
		// If the file doesn't exist, we can assume there were no mempool transactions to revert.
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "consumer.revertStoredMempoolTransactions: Error opening applied mempool entries file: %s", consumer.ConsumerProgressDir)
	}
	defer file.Close()

	var mempoolEntries []*lib.StateChangeEntry
	fileEof := false

	reader := bufio.NewReader(file)

	for !fileEof {
		var mempoolEntry *lib.StateChangeEntry
		mempoolEntry, fileEof, err = consumer.readAndDecodeNextEntry(reader, file)
		if fileEof {
			break
		} else if err != nil {
			return errors.Wrapf(err, "consumer.revertStoredMempoolTransactions: Error reading from applied mempool entries file: %s", consumer.ConsumerProgressDir)
		}
		mempoolEntries = append(mempoolEntries, mempoolEntry)
	}

	// Revert the mempool entries in reverse order.
	for i := len(mempoolEntries) - 1; i >= 0; i-- {
		mempoolEntry := mempoolEntries[i]
		if err := consumer.RevertMempoolEntry(mempoolEntry); err != nil {
			return errors.Wrapf(err, "consumer.revertStoredMempoolTransactions: Error reverting mempool entry: %s", consumer.ConsumerProgressDir)
		}
	}
	return nil
}

// truncateMempoolProgressFile truncates the mempool progress file to 0 bytes.
func (consumer *StateSyncerConsumer) truncateMempoolProgressFile() error {
	mempoolStatusFilepath := filepath.Join(consumer.ConsumerProgressDir, lib.StateChangeMempoolFileName)
	// Create the file if it doesn't exist.
	file, err := createDirAndFile(mempoolStatusFilepath)
	if os.IsNotExist(err) {
		// If the file doesn't exist, we can assume there were no mempool transactions to revert.
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "consumer.truncateMempoolProgressFile: Error creating applied mempool entries file: %s", consumer.ConsumerProgressDir)
	}
	defer file.Close()

	if err := file.Truncate(0); err != nil {
		return errors.Wrapf(err, "consumer.truncateMempoolProgressFile: Error truncating applied mempool entries file: %s", consumer.ConsumerProgressDir)
	}
	return nil
}

// cleanup performs any final operations before the consumer exits. This mainly consists of handling any remaining
// batched entries that haven't been processed yet.
func (consumer *StateSyncerConsumer) cleanup() error {
	// If there are still bulk operations to perform, perform them now.
	if err := consumer.executeBatch(); err != nil {
		return errors.Wrapf(err, "consumer.cleanup: Error executing final batch")
	}
	return nil
}

func (consumer *StateSyncerConsumer) Stop() {
	consumer.StopConsumer = true
}
