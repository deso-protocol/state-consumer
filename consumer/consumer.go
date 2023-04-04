package consumer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/deso-protocol/core/lib"
	"github.com/pkg/errors"
	"io"
	"os"
	"time"
)

// StateSyncerConsumer is a struct that contains the persisted state that is needed to consume state changes from a file.
// This includes file readers, statuses, batch caches, and channels to facilitate multi-threaded processing.
type StateSyncerConsumer struct {
	// File that contains the state changes.
	StateChangeFile       *os.File
	StateChangeFileReader *bufio.Reader
	// File that contains the byte indexes of the state change file that corresponds to db operations.
	StateChangeIndexFile *os.File
	// Index of the entry in the state change file that the consumer should start parsing at.
	LastScannedIndex uint32
	// File that contains the entry index of the last saved state change.
	ConsumerProgressFile     *os.File
	ConsumerProgressFileName string
	// The data handler that will be used to process the state changes that the consumer parses.
	DataHandler StateSyncerDataHandler
	// An object that contains the state changes that have been parsed but not yet processed. Used for batching.
	BatchedEntries []*lib.StateChangeEntry
	// The maximum number of entries to batch before inserting into the database.
	MaxBatchSize int

	// Track whether we're currently hypersyncing
	IsHypersyncing bool
	// A counter to keep track of how many batches have been inserted.
	BatchCount int
	EntryCount uint32

	// Multi-threading channels
	// Channel to pass state change entries to the listener.
	DBEntryChannel chan []*lib.StateChangeEntry
	// Channel to enforce a max thread limit on the listener.
	DBBlockingChannel chan bool
}

func (consumer *StateSyncerConsumer) InitializeAndRun(
	stateChangeFileName string, stateChangeIndexFileName string, consumerProgressFilename string, batchSize int,
	threadLimit int, handler StateSyncerDataHandler) error {
	// initialize the consumer
	err := consumer.initialize(stateChangeFileName, stateChangeIndexFileName, consumerProgressFilename,
		batchSize, threadLimit, handler)
	if err != nil && err.Error() != "EOF" {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error initializing consumer")
	}
	// If there are entries to read, run an initial scan of the state change file.
	if err == nil || err.Error() != "EOF" {
		if err = consumer.run(); err != nil {
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
func (consumer *StateSyncerConsumer) initialize(stateChangeFileName string, stateChangeIndexFileName string,
	consumerProgressFilename string, batchSize int, threadLimit int,
	handler StateSyncerDataHandler) error {
	// Set up the data handler initial values.
	consumer.IsHypersyncing = false
	consumer.BatchCount = 0
	consumer.EntryCount = 0
	consumer.MaxBatchSize = batchSize
	consumer.DataHandler = handler
	consumer.DBEntryChannel = make(chan []*lib.StateChangeEntry, threadLimit)
	consumer.DBBlockingChannel = make(chan bool, threadLimit)

	// Wait for the state changes file to be created. Once it has been created, open it.
	consumer.waitForStateChangesFile(stateChangeFileName)
	// Create a new reader for the state change file.
	consumer.StateChangeFileReader = bufio.NewReader(consumer.StateChangeFile)

	// Open the file that contains byte indexes for each entry in the state changes file.
	indexFile, err := os.Open(stateChangeIndexFileName)
	if err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error opening indexFile")
	}
	consumer.StateChangeIndexFile = indexFile

	// Open the file that contains the entry index of the last saved state change.
	consumer.ConsumerProgressFileName = consumerProgressFilename
	if startEntryIndexFile, err := os.Open(consumerProgressFilename); err == nil {
		consumer.ConsumerProgressFile = startEntryIndexFile
	}

	// Discover where we should start parsing the state change file.
	stateChangeFileByteIndex, err := consumer.retrieveFileIndexForDbOperation()
	if err != nil {
		return errors.Wrapf(err, "consumer.intialize: Error retrieving file index for db operation")
	}

	// Seek to the byte index that we should start parsing at.
	if _, err = consumer.StateChangeFile.Seek(int64(stateChangeFileByteIndex), 0); err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error seeking to byte index")
	}

	// If the byte index is 0, we are starting a fresh sync.
	if stateChangeFileByteIndex == 0 {
		if err = consumer.DataHandler.HandleSyncEvent(SyncEventStart); err != nil {
			return errors.Wrapf(err, "consumer.initialize: Error handling sync start event")
		}
	}

	return nil
}

// run reads the state change file and passes each entry to the data handler.
func (consumer *StateSyncerConsumer) run() error {
	fileEOF := false
	// Read from the state change file until we reach the end.
	for !fileEOF {
		var err error
		var stateChangeEntry *lib.StateChangeEntry
		// Get the next state change entry from the state change file.
		stateChangeEntry, fileEOF, err = consumer.readNextEntryFromFile()
		if err != nil {
			return errors.Wrapf(err, "consumer.run: Error reading next entry from file")
		}
		if !fileEOF {
			// Detect if this entry represets a sync state change and emit
			if err = consumer.detectAndHandleSyncEvent(stateChangeEntry); err != nil {
				return errors.Wrapf(err, "consumer.run: Error detecting sync event")
			}
			// Handle the state change entry.
			if err = consumer.handleStateChangeEntry(stateChangeEntry); err != nil {
				return errors.Wrapf(err, "consumer.run: Error handling state change entry")
			}
		}
	}
	// Once we've reached the file EOF, process any remaining batched entries and cleanup.
	return consumer.cleanup()
}

// readNextEntryFromFile reads the next StateChangeEntry bytes from the state change file and decode them.
func (consumer *StateSyncerConsumer) readNextEntryFromFile() (*lib.StateChangeEntry, bool, error) {
	// Get the size of the next state change entry.
	entryByteSize, err := lib.ReadUvarint(consumer.StateChangeFileReader)
	// Create a buffer to hold the entry.
	buffer := make([]byte, entryByteSize)
	bytesRead, err := io.ReadFull(consumer.StateChangeFileReader, buffer)
	// If there are no bytes to read, return true to signify EOF.
	if bytesRead == 0 {
		return nil, true, nil
	} else if err != nil {
		return nil, false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error reading from state change file")
	} else if bytesRead < int(entryByteSize) {
		return nil, false, fmt.Errorf("consumer.readNextEntryFromFile: Not enough bytes read from state change file. Expected %d, got %d", entryByteSize, bytesRead)
	}

	// Decode the state change entry.
	stateChangeEntry := &lib.StateChangeEntry{}
	if err = DecodeEntry(stateChangeEntry, buffer); err != nil {
		return nil, false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error decoding entry")
	}
	consumer.EntryCount++
	return stateChangeEntry, false, nil
}

// detectAndHandleSyncEvent determines if the state change entry represents a sync event and emits it to the data handler.
func (consumer *StateSyncerConsumer) detectAndHandleSyncEvent(stateChangeEntry *lib.StateChangeEntry) error {
	// Determine if hypersync is beginning or ending.
	if stateChangeEntry.OperationType == lib.DbOperationTypeInsert && !consumer.IsHypersyncing {
		consumer.IsHypersyncing = true
		go consumer.listenForBatchEvents()
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventHypersyncStart); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync start event")
		}
	} else if stateChangeEntry.OperationType != lib.DbOperationTypeInsert && consumer.IsHypersyncing {
		// If the operation type is not an insert, we must have finished hypersyncing.
		// First, wait for any remaining batch threads to finish.
		consumer.waitForAllInsertsToComplete()
		// Set the hypersyncing flag to false and close the channels.
		consumer.IsHypersyncing = false
		close(consumer.DBEntryChannel)
		close(consumer.DBBlockingChannel)
		if err := consumer.DataHandler.HandleSyncEvent(SyncEventHypersyncComplete); err != nil {
			return errors.Wrapf(err, "consumer.detectAndHandleSyncEvent: Error handling hypersync complete event")
		}
	}
	return nil
}

// handleStateChangeEntry handles a state change entry by passing it to the appropriate data handler function.
func (consumer *StateSyncerConsumer) handleStateChangeEntry(stateChangeEntry *lib.StateChangeEntry) error {
	// If the entry is a utxo op, pass it to the utxo op handler.
	// We don't need to pass the entry to the data handler because the utxo op handler will handle the appropriate
	// disconnect logic for this transaction.
	if len(stateChangeEntry.UtxoOps) > 0 {
		if err := consumer.handleUtxoOps(stateChangeEntry); err != nil {
			return errors.Wrapf(err, "consumer.handleStateChangeEntry: Error handling utxo ops")
		}
	} else {
		if err := consumer.HandleEntryOperationBatch(stateChangeEntry); err != nil {
			return errors.Wrapf(err, "consumer.handleStateChangeEntry: Error handling entry operation batch")
		}
	}
	return nil
}

// watchFileAndScanOnWrite continually triggers a new run of the consumer. If there are any new changes that have been
// written, they will be captured by the run, otherwise the run will exit.
func (consumer *StateSyncerConsumer) watchFileAndScanOnWrite() error {
	for {
		time.Sleep(50 * time.Millisecond)
		err := consumer.run()
		if err != nil {
			return errors.Wrapf(err, "consumer.watchFileAndScanOnWrite: Error running consumer")
		}
	}
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

// waitForAllInsertsToComplete blocks execution until all inserts have been completed. This is necessary because
// the consumer will exit before all inserts have been completed. This marks the end of hypersync, where inserts
// can happen asynchronously, and the beginning of blocksync, where inserts must be completed in order due to
// transaction dependencies.
func (consumer *StateSyncerConsumer) waitForAllInsertsToComplete() {
	for {
		select {
		case <-consumer.DBBlockingChannel:
			// Channel has a value, continue waiting for it to be empty
		default:
			// Channel is empty, exit the loop
			return
		}
	}
}

// handleUtxoOps handles a state change entry that contains utxo ops. It will translate each utxo op
// into the appropriate disconnect operation and pass it to be handled by the batch data handler.
func (consumer *StateSyncerConsumer) handleUtxoOps(stateChangeEntry *lib.StateChangeEntry) error {
	for _, utxoOp := range stateChangeEntry.UtxoOps {
		disconnectEncoder, disconnectOperationType, disconnectEncoderType := utxoOpToEncoderAndOperationType(utxoOp)
		if disconnectOperationType == lib.DbOperationTypeSkip {
			continue
		}
		stateChangeEntry.EncoderType = disconnectEncoderType
		stateChangeEntry.OperationType = disconnectOperationType
		stateChangeEntry.Encoder = disconnectEncoder
		// Handle disconnect db operations via the standard batch handler.
		err := consumer.HandleEntryOperationBatch(stateChangeEntry)
		if err != nil {
			return fmt.Errorf("consumer.handleUtxoOps: Error handling entry batch: %w", err)
		}
	}
	return nil
}

// retrieveFileIndexForDbOperation retrieves the byte index in the state change file for the next db operation.
// It does this by reading the last saved entry index from the entry index file and multiplying it by 4 to get the
// byte index in the state change index file.
func (consumer *StateSyncerConsumer) retrieveFileIndexForDbOperation() (uint32, error) {
	startEntryIndex := uint32(0)
	var err error
	// Attempt to open the consumer progress file. If it exists, it should have a single uint32 representing the
	// last StateChangeEntry index that was processed.
	if consumer.ConsumerProgressFile != nil {
		startEntryIndex, err = getUint32FromFile(consumer.ConsumerProgressFile)
		if err != nil {
			return 0, err
		}
	}
	consumer.EntryCount = startEntryIndex
	consumer.LastScannedIndex = startEntryIndex
	fmt.Printf("Last scanned index: %d\n", startEntryIndex)
	// Find the byte index in the state change file for the next db operation. Each entry byte index is represented
	// in the index file as a uint32. This means the entry byte index exists at its consumer progress index * 4.
	entryIndexBytes := make([]byte, 4)
	fileBytesPosition := int64(startEntryIndex * 4)
	bytesRead, err := consumer.StateChangeIndexFile.ReadAt(entryIndexBytes, fileBytesPosition)
	if err != nil {
		return 0, errors.Wrapf(err, "consumer.retrieveFileIndexForDbOperation: Error reading from state change index file")
	}
	// If we read no bytes, we're at EOF.
	if bytesRead == 0 {
		return 0, fmt.Errorf("consumer.retrieveFileIndexForDbOperation: EOF reached")
	}
	// If we read a non uint32 number of bytes, something is wrong.
	if bytesRead < 4 {
		return 0, fmt.Errorf("consumer.retrieveFileIndexForDbOperation: Too few bytes read")
	}

	// Use binary package to read a uint32 index from the byte slice representing the index of the db operation.
	dbIndex := binary.LittleEndian.Uint32(entryIndexBytes)
	return dbIndex, nil
}

// saveConsumerProgressToFile saves the last StateChangeEntry index that was processed to the consumer progress file.
// This is represented as a single uint32 encoded to bytes.
func (consumer *StateSyncerConsumer) saveConsumerProgressToFile(entryIndex uint32) error {
	// Don't save progress if we're in hypersync mode.
	if consumer.IsHypersyncing {
		return nil
	}

	// Create the file if it doesn't exist.
	file, err := os.Create(consumer.ConsumerProgressFileName)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error creating consumer progress file: %s", consumer.ConsumerProgressFileName)
	}
	defer file.Close()

	// Write the entry index to the file.
	err = binary.Write(file, binary.LittleEndian, entryIndex)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error writing entry index to consumer progress file: %s", consumer.ConsumerProgressFileName)
	}
	consumer.LastScannedIndex = entryIndex
	return nil
}

// cleanup performs any final operations before the consumer exits. This mainly consists of handling any remaining
// batched entries that haven't been processed yet.
func (consumer *StateSyncerConsumer) cleanup() error {
	// If there are still bulk operations to perform, perform them now.
	if consumer.BatchedEntries != nil && len(consumer.BatchedEntries) > 0 {
		err := consumer.DataHandler.HandleEntryBatch(consumer.BatchedEntries)
		if err != nil {
			return errors.Wrapf(err, "consumer.cleanup: Error handling entry batch")
		}
		handledEntries := len(UniqueEntries(consumer.BatchedEntries))
		fmt.Printf("consumer.cleanup: Handled batch %d\n", consumer.BatchCount)
		consumer.BatchCount += 1
		consumer.BatchedEntries = nil
		return consumer.saveConsumerProgressToFile(consumer.LastScannedIndex + uint32(handledEntries))
	}
	return nil
}
