package consumer

import (
	"encoding/binary"
	"fmt"
	"github.com/deso-protocol/core/lib"
	"github.com/fsnotify/fsnotify"
	"github.com/golang/glog"
	"log"
	"os"
)

type StateSyncerConsumer struct {
	// File that contains the state changes.
	StateChangeFile     *os.File
	StateChangeFileName string
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
	BatchedEntries *BatchedEntries
	// The maximum number of entries to batch before inserting into the database.
	MaxBatchSize int

	// Track whether we're actively consuming or not.
	IsScanning bool
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
		return err
	}
	// If there are entries to read, run an initial scan of the index file.
	if err == nil || err.Error() != "EOF" {
		err = consumer.run()
	} else {
		consumer.IsScanning = false
	}
	// Create a watcher to handle any new writes to the state change file.
	err = consumer.watchFileAndScanOnWrite()
	if err != nil {
		return err
	}
	return nil
}

// Open the state change file and the index file, and determine the byte index that the state syncer should start
// parsing at.
func (consumer *StateSyncerConsumer) initialize(stateChangeFileName string, stateChangeIndexFileName string, consumerProgressFilename string, processInBatches bool, batchSize int, handler StateSyncerDataHandler) error {
	// Set up the data handler initial values.
	consumer.IsScanning = true
	consumer.IsHypersyncing = false
	consumer.ProcessEntriesInBatches = processInBatches
	consumer.BatchCount = 0
	consumer.EntryCount = 0
	consumer.MaxBatchSize = batchSize

	// Open the state changes file
	consumer.StateChangeFileName = stateChangeFileName
	stateChangeFile, err := os.Open(stateChangeFileName)
	if err != nil {
		return fmt.Errorf("Error opening stateChangeFile: %w", err)
	}
	consumer.StateChangeFile = stateChangeFile

	// Open the file that contains byte indexes for each entry in the state changes file.
	consumer.StateChangeIndexFileName = stateChangeIndexFileName
	indexFile, err := os.Open(stateChangeIndexFileName)
	if err != nil {
		return fmt.Errorf("Error opening indexFile: %w", err)
	}
	consumer.StateChangeIndexFile = indexFile

	// Open the file that contains the entry index of the last saved state change.
	consumer.ConsumerProgressFileName = consumerProgressFilename
	startEntryIndexFile, err := os.Open(consumerProgressFilename)
	if err == nil {
		consumer.ConsumerProgressFile = startEntryIndexFile
	}

	stateChangeFileByteIndex, err := consumer.retrieveFileIndexForDbOperation()
	if err != nil {
		if err.Error() == "EOF" {
			consumer.end()
			return err
		}
		return err
	}

	consumer.StateChangeFile.Seek(int64(stateChangeFileByteIndex), 0)

	consumer.DataHandler = handler

	// If the byte index is 0, we are starting a fresh sync.
	if stateChangeFileByteIndex == 0 {
		consumer.DataHandler.HandleSyncEvent(SyncEventStart)
	}

	return nil
}

func (consumer *StateSyncerConsumer) watchFileAndScanOnWrite() error {
	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					fmt.Println("File modified. Scanning for state changes.")
					fmt.Printf("IsScanning: %v\n", consumer.IsScanning)
					// Don't start scanning if we're already scanning.
					if !consumer.IsScanning {
						fmt.Println("Starting scan.")
						consumer.run()
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				glog.Fatalf("Error watching file: %v", err)
			}
		}
	}()

	// Add a path.
	err = watcher.Add(consumer.StateChangeFileName)
	if err != nil {
		return err
	}

	// Block main goroutine forever.
	<-make(chan struct{})
	fmt.Printf("Done watching file.\n")
	return nil
}

// ReadNextEntryFromFile reads the next entry from the state change file and runs the appropriate data handler function.
// The format of the file is:
// [operation type (1 byte)][encoder type (2 bytes)][key length (2 bytes)][key bytes][value length (2 bytes)][value bytes]
func (consumer *StateSyncerConsumer) readNextEntryFromFile() (bool, error) {
	// Extract the operation type.
	// Operation type is 0 for insert, 1 for delete, 2 for update, and 3 for upsert.
	operationTypeInt, err := getUint8FromFile(consumer.StateChangeFile)
	operationType := lib.StateSyncerOperationType(operationTypeInt)
	if err != nil {
		return true, nil
		//return false, err
	}

	// If the operation type is an insert, we must be hypersyncing.
	if operationType == lib.DbOperationTypeInsert && !consumer.IsHypersyncing {
		consumer.IsHypersyncing = true
	} else if operationType != lib.DbOperationTypeInsert && consumer.IsHypersyncing {
		// If the operation type is not an insert, we must have finished hypersyncing.
		consumer.IsHypersyncing = false
		if err = consumer.DataHandler.HandleSyncEvent(SyncEventHypersyncComplete); err != nil {
			return false, err
		}
	}

	//fmt.Printf("\nHere is the operation type: %v", operationType)

	// Extract which encoder the entry is encoded with.
	encoderType, err := getUint16FromFile(consumer.StateChangeFile)
	if err != nil {
		return false, err
	}
	//fmt.Printf("\nHere is the encoder type: %v", encoderType)

	// Determine how large the key is, in bytes.
	keyByteSize, err := getUint16FromFile(consumer.StateChangeFile)
	if err != nil {
		return false, err
	}
	//fmt.Printf("\nHere is the keyByteSize: %v", keyByteSize)

	// Read the contents of the first uint16 from the stateChangeFile into a byte slice.
	keyBytes, err := getBytesFromFile(int(keyByteSize), consumer.StateChangeFile)
	if err != nil {
		return false, err
	}
	//fmt.Printf("\nHere is the keyBytes: %v", keyBytes)

	// Get encoder for the key.
	isEncoder, encoder := lib.StateKeyToDeSoEncoder(keyBytes)
	if !isEncoder || encoder == nil {
		return false, fmt.Errorf("No encoder found for encoder type: %d", encoderType)
	}

	// Determine how large the entry is, in bytes.
	entryByteSize, err := getUint32FromFile(consumer.StateChangeFile)
	if err != nil {
		return false, err
	}
	//fmt.Printf("\nHere is the entryByteSize: %v", entryByteSize)

	if entryByteSize > 0 {
		// Read the contents of the first uint16 from the stateChangeFile into a byte slice.
		entryBytes, err := getBytesFromFile(int(entryByteSize), consumer.StateChangeFile)
		if err != nil {
			return false, err
		}
		//fmt.Printf("\nHere is the entryBytes: %v", entryBytes)

		// Decode value to DeSo Encoder.
		DecodeEntry(encoder, entryBytes)
	}

	consumer.EntryCount += 1
	//fmt.Printf("\nHere is the entryCounter: %v", consumer.EntryCounter)
	//fmt.Printf("\nHere is the encoder: %+v", encoder)

	// Pass the parsed values to the appropriate data handler function.
	if consumer.ProcessEntriesInBatches {
		return false, consumer.HandleEntryOperationBatch(keyBytes, &encoder, lib.EncoderType(encoderType), operationType)
	} else {
		return false, consumer.DataHandler.HandleEntry(keyBytes, encoder, lib.EncoderType(encoderType), operationType)
	}
}

func (consumer *StateSyncerConsumer) run() error {
	fileEOF := false
	fmt.Println("Before File EOF")
	for !fileEOF {
		var err error
		fileEOF, err = consumer.readNextEntryFromFile()
		if err != nil {
			fmt.Println("Err != nil")
			consumer.IsScanning = false
			return err
		}
	}
	fmt.Println("Is Scanning = false")
	consumer.IsScanning = false
	return consumer.end()
}

func (consumer *StateSyncerConsumer) end() error {
	consumer.cleanup()
	if err := consumer.StateChangeFile.Close(); err != nil {
		return err
	}
	if err := consumer.StateChangeIndexFile.Close(); err != nil {
		return err
	}
	if consumer.ConsumerProgressFile != nil {
		if err := consumer.ConsumerProgressFile.Close(); err != nil {
			return err
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
	// Each entry byte index is represented as a uint32. This means the entry byte index exists at it'consumer
	// index * 4.
	entryIndexBytes := make([]byte, 4)
	fileBytesPosition := int64(startEntryIndex * 4)
	bytesRead, err := consumer.StateChangeIndexFile.ReadAt(entryIndexBytes, fileBytesPosition)
	if err != nil {
		return 0, err
	}
	// If we read no bytes, we're at EOF.
	if bytesRead == 0 {
		return 0, fmt.Errorf("EOF reached")
	}
	// If we read a weird number of bytes, something is wrong.
	if bytesRead < 4 {
		return 0, fmt.Errorf("Too few bytes read")
	}

	// Use binary package to read a uint32 index from the byte slice representing the index of the db operation.
	dbIndex := binary.LittleEndian.Uint32(entryIndexBytes)
	return dbIndex, nil
}

func (consumer *StateSyncerConsumer) saveConsumerProgressToFile(entryIndex uint32) error {
	file, err := os.Create(consumer.ConsumerProgressFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	err = binary.Write(file, binary.LittleEndian, entryIndex)
	if err != nil {
		return err
	}
	consumer.LastScannedIndex = entryIndex
	return nil
}

func (consumer *StateSyncerConsumer) cleanup() error {
	// If there are still bulk operations to perform, perform them now.
	if consumer.ProcessEntriesInBatches && consumer.BatchedEntries != nil && len(consumer.BatchedEntries.Entries) > 0 {
		err := consumer.DataHandler.HandleEntryBatch(consumer.BatchedEntries)
		if err != nil {
			return err
		}
		handledEntries := len(UniqueEntries(consumer.BatchedEntries.Entries))
		return consumer.saveConsumerProgressToFile(consumer.LastScannedIndex + uint32(handledEntries))
	}
	return nil
}
