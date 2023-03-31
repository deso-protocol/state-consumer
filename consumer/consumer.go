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

type StateSyncerConsumer struct {
	// File that contains the state changes.
	StateChangeFile       *os.File
	StateChangeFileName   string
	StateChangeFileReader *bufio.Reader
	// File that contains the byte indexes of the state change file that corresponds to db operations.
	StateChangeIndexFile     *os.File
	StateChangeIndexFileName string
	// Index of the entry in the state change file that the consumer should start parsing at.
	LastScannedIndex uint32
	// File that contains the entry index of the last saved state change.
	ConsumerProgressFile     *os.File
	ConsumerProgressFileName string
	// The data handler that will be used to process the state changes that the consumer parses.
	DataHandler             StateSyncerDataHandler
	ProcessEntriesInBatches bool
	// An object that contains the state changes that have been parsed but not yet processed. Used for batching.
	BatchedEntries []*lib.StateChangeEntry
	// The maximum number of entries to batch before inserting into the database.
	MaxBatchSize int

	// Track whether we're currently hypersyncing
	IsHypersyncing bool
	// A counter to keep track of how many batches have been inserted.
	BatchCount int
	EntryCount uint32
}

func (consumer *StateSyncerConsumer) InitializeAndRun(stateChangeFileName string, stateChangeIndexFileName string, consumerProgressFilename string, processInBatches bool, batchSize int, handler StateSyncerDataHandler) error {
	// initialize the consumer
	err := consumer.initialize(stateChangeFileName, stateChangeIndexFileName, consumerProgressFilename, processInBatches, batchSize, handler)
	if err != nil && err.Error() != "EOF" {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error initializing consumer")
	}
	// If there are entries to read, run an initial scan of the index file.
	if err == nil || err.Error() != "EOF" {
		if err = consumer.run(); err != nil {
			return errors.Wrapf(err, "consumer.InitializeAndRun: Error running consumer")
		}
	}
	// Create a watcher to handle any new writes to the state change file.
	if err = consumer.watchFileAndScanOnWrite(); err != nil {
		return errors.Wrapf(err, "consumer.InitializeAndRun: Error watching file")
	}
	return nil
}

// Open the state change file and the index file, and determine the byte index that the state syncer should start
// parsing at.
func (consumer *StateSyncerConsumer) initialize(stateChangeFileName string, stateChangeIndexFileName string, consumerProgressFilename string, processInBatches bool, batchSize int, handler StateSyncerDataHandler) error {
	// Set up the data handler initial values.
	consumer.IsHypersyncing = false
	consumer.ProcessEntriesInBatches = processInBatches
	consumer.BatchCount = 0
	consumer.EntryCount = 0
	consumer.MaxBatchSize = batchSize
	consumer.DataHandler = handler

	// Open the state changes file
	consumer.StateChangeFileName = stateChangeFileName
	stateChangeFile, err := os.Open(stateChangeFileName)
	if err != nil {
		return errors.Wrapf(err, "consumer.initialize: Error opening stateChangeFile")
	}
	consumer.StateChangeFile = stateChangeFile
	consumer.StateChangeFileReader = bufio.NewReader(stateChangeFile)

	// Open the file that contains byte indexes for each entry in the state changes file.
	consumer.StateChangeIndexFileName = stateChangeIndexFileName
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

	stateChangeFileByteIndex, err := consumer.retrieveFileIndexForDbOperation()
	if err != nil {
		return errors.Wrapf(err, "consumer.intialize: Error retrieving file index for db operation")
	}

	consumer.StateChangeFile.Seek(int64(stateChangeFileByteIndex), 0)

	// If the byte index is 0, we are starting a fresh sync.
	if stateChangeFileByteIndex == 0 {
		consumer.DataHandler.HandleSyncEvent(SyncEventStart)
	}

	return nil
}

func (consumer *StateSyncerConsumer) watchFileAndScanOnWrite() error {
	for {
		time.Sleep(50 * time.Millisecond)
		err := consumer.run()
		if err != nil {
			return errors.Wrapf(err, "consumer.watchFileAndScanOnWrite: Error running consumer")
		}
	}
}

// Read the next state change entry from the state change file and pass it to the data handler.
// If the entry is a utxo op, pass it to the utxo op handler.
// If there are no bytes to read, return true to signify EOF.
func (consumer *StateSyncerConsumer) readNextEntryFromFile() (bool, error) {
	// Get the size of the next state change entry.
	entryByteSize, err := lib.ReadUvarint(consumer.StateChangeFileReader)
	// Create a buffer to hold the entry.
	buffer := make([]byte, entryByteSize)
	bytesRead, err := io.ReadFull(consumer.StateChangeFileReader, buffer)
	// If there are no bytes to read, return true to signify EOF.
	if bytesRead == 0 {
		return true, nil
	} else if err != nil {
		return false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error reading from state change file")
	} else if bytesRead < int(entryByteSize) {
		return false, fmt.Errorf("consumer.readNextEntryFromFile: Not enough bytes read from state change file. Expected %d, got %d", entryByteSize, bytesRead)
	}
	// Decode the state change entry.
	stateChangeEntry := &lib.StateChangeEntry{}
	err = DecodeEntry(stateChangeEntry, buffer)
	if err != nil {
		return false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error decoding entry")
	}

	// If the entry is a utxo op, pass it to the utxo op handler.
	if len(stateChangeEntry.UtxoOps) > 0 {
		if err = consumer.handleUtxoOps(stateChangeEntry); err != nil {
			return false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error handling utxo ops")
		}
		return false, nil
	} else {
		// Pass the parsed values to the appropriate data handler function.
		if consumer.ProcessEntriesInBatches {
			if err = consumer.HandleEntryOperationBatch(stateChangeEntry); err != nil {
				return false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error handling entry operation batch")
			} else {
				return false, nil
			}
		} else {
			if err = consumer.DataHandler.HandleEntry(stateChangeEntry); err != nil {
				return false, errors.Wrapf(err, "consumer.readNextEntryFromFile: Error handling entry")
			} else {
				return false, nil
			}
		}
	}
}

func (consumer *StateSyncerConsumer) handleUtxoOps(stateChangeEntry *lib.StateChangeEntry) error {
	for _, utxoOp := range stateChangeEntry.UtxoOps {
		//fmt.Printf("\nHere is the utxoOp: %+v\n\n", utxoOp)
		disconnectEncoder, disconnectOperationType, disconnectEncoderType := utxoOpToEncoderAndOperationType(utxoOp)
		if disconnectOperationType == lib.DbOperationTypeSkip {
			continue
		}
		stateChangeEntry.EncoderType = disconnectEncoderType
		stateChangeEntry.OperationType = disconnectOperationType
		stateChangeEntry.Encoder = disconnectEncoder
		// Handle disconnect db operations based on how the consumer is configured to process entries.
		if consumer.ProcessEntriesInBatches {
			err := consumer.HandleEntryOperationBatch(stateChangeEntry)
			if err != nil {
				return fmt.Errorf("consumer.handleUtxoOps: Error handling entry batch: %w", err)
			}
		} else {
			err := consumer.DataHandler.HandleEntry(stateChangeEntry)
			if err != nil {
				return fmt.Errorf("consumer.handleUtxoOps: Error handling entry: %w", err)
			}
		}
	}
	return nil
}

func (consumer *StateSyncerConsumer) run() error {
	fileEOF := false
	for !fileEOF {
		var err error
		fileEOF, err = consumer.readNextEntryFromFile()
		if err != nil {
			return fmt.Errorf("consumer.run: Error reading next entry from file: %w", err)
		}
	}
	return consumer.cleanup()
}

func (consumer *StateSyncerConsumer) end() error {
	consumer.cleanup()
	if err := consumer.StateChangeFile.Close(); err != nil {
		return errors.Wrapf(err, "consumer.end: Error closing state change file")
	}
	if err := consumer.StateChangeIndexFile.Close(); err != nil {
		return errors.Wrapf(err, "consumer.end: Error closing state change index file")
	}
	if consumer.ConsumerProgressFile != nil {
		if err := consumer.ConsumerProgressFile.Close(); err != nil {
			return errors.Wrapf(err, "consumer.end: Error closing consumer progress file")
		}
	}
	return nil
}

// RetrieveFileIndexForDbOperation retrieves the byte index in the state change file for the next db operation.
// It does this by reading the last saved entry index from the entry index file and multiplying it by 4 to get the
// byte index in the state change index file.
func (consumer *StateSyncerConsumer) retrieveFileIndexForDbOperation() (uint32, error) {
	startEntryIndex := uint32(0)
	var err error
	if consumer.ConsumerProgressFile != nil {
		startEntryIndex, err = getUint32FromFile(consumer.ConsumerProgressFile)
		if err != nil {
			return 0, err
		}
	}
	consumer.LastScannedIndex = startEntryIndex
	fmt.Printf("Last scanned index: %d\n", startEntryIndex)
	// Each entry byte index is represented as a uint32. This means the entry byte index exists at its consumer
	// index * 4.
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
	// If we read a weird number of bytes, something is wrong.
	if bytesRead < 4 {
		return 0, fmt.Errorf("consumer.retrieveFileIndexForDbOperation: Too few bytes read")
	}

	// Use binary package to read a uint32 index from the byte slice representing the index of the db operation.
	dbIndex := binary.LittleEndian.Uint32(entryIndexBytes)
	return dbIndex, nil
}

func (consumer *StateSyncerConsumer) saveConsumerProgressToFile(entryIndex uint32) error {
	file, err := os.Create(consumer.ConsumerProgressFileName)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error creating consumer progress file: %s", consumer.ConsumerProgressFileName)
	}
	defer file.Close()

	err = binary.Write(file, binary.LittleEndian, entryIndex)
	if err != nil {
		return errors.Wrapf(err, "consumer.saveConsumerProgressToFile: Error writing entry index to consumer progress file: %s", consumer.ConsumerProgressFileName)
	}
	consumer.LastScannedIndex = entryIndex
	return nil
}

func (consumer *StateSyncerConsumer) cleanup() error {
	// If there are still bulk operations to perform, perform them now.
	if consumer.ProcessEntriesInBatches && consumer.BatchedEntries != nil && len(consumer.BatchedEntries) > 0 {
		// TODO: Reduce re-used code here.
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
