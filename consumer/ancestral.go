package consumer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/deso-protocol/core/lib"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// AncestralOperation represents the type of operation that needs to be reverted
type AncestralOperation uint8

const (
	AncestralOperationInsert AncestralOperation = 0
	AncestralOperationUpdate AncestralOperation = 1
	AncestralOperationDelete AncestralOperation = 2
)

func (ao AncestralOperation) String() string {
	switch ao {
	case AncestralOperationInsert:
		return "insert"
	case AncestralOperationUpdate:
		return "update"
	case AncestralOperationDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// AncestralRecord stores the information needed to revert a mempool change
type AncestralRecord struct {
	Key           []byte
	PreviousValue []byte
	Operation     AncestralOperation
}

// AncestralRecordManager handles reading ancestral records and applying reverts
type AncestralRecordManager struct {
	progressDir string
	dataHandler StateSyncerDataHandler
}

// NewAncestralRecordManager creates a new ancestral record manager
func NewAncestralRecordManager(progressDir string, dataHandler StateSyncerDataHandler) *AncestralRecordManager {
	return &AncestralRecordManager{
		progressDir: progressDir,
		dataHandler: dataHandler,
	}
}

// ReadAncestralRecordsFromFile reads ancestral records from a file
func (arm *AncestralRecordManager) ReadAncestralRecordsFromFile(filePath string) ([]AncestralRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open ancestral file: %s", filePath)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to stat ancestral file: %s", filePath)
	}

	if stat.Size() == 0 {
		return []AncestralRecord{}, nil // Empty file is valid
	}

	reader := bufio.NewReader(file)
	var records []AncestralRecord

	for {
		record, err := arm.readSingleAncestralRecord(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read ancestral record from file: %s", filePath)
		}
		records = append(records, *record)
	}

	return records, nil
}

// readSingleAncestralRecord reads a single ancestral record from a reader
func (arm *AncestralRecordManager) readSingleAncestralRecord(reader *bufio.Reader) (*AncestralRecord, error) {
	// Read operation type (1 byte)
	opByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	operation := AncestralOperation(opByte)
	if operation > AncestralOperationDelete {
		return nil, fmt.Errorf("invalid ancestral operation: %d", opByte)
	}

	// Read key length (4 bytes)
	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return nil, errors.Wrap(err, "failed to read key length")
	}

	if keyLen > 1024*1024 { // 1MB sanity check
		return nil, fmt.Errorf("key length too large: %d", keyLen)
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, errors.Wrap(err, "failed to read key")
	}

	// Read value length (4 bytes)
	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return nil, errors.Wrap(err, "failed to read value length")
	}

	if valueLen > 10*1024*1024 { // 10MB sanity check
		return nil, fmt.Errorf("value length too large: %d", valueLen)
	}

	// Read value
	var previousValue []byte
	if valueLen > 0 {
		previousValue = make([]byte, valueLen)
		if _, err := io.ReadFull(reader, previousValue); err != nil {
			return nil, errors.Wrap(err, "failed to read value")
		}
	}

	return &AncestralRecord{
		Key:           key,
		PreviousValue: previousValue,
		Operation:     operation,
	}, nil
}

// ApplyRevertOperation applies a single revert operation based on an ancestral record
func (arm *AncestralRecordManager) ApplyRevertOperation(record AncestralRecord) error {
	// Convert ancestral record to StateChangeEntry for processing
	var stateChangeEntry *lib.StateChangeEntry

	switch record.Operation {
	case AncestralOperationInsert:
		// If this was an insert, we need to delete the entry
		stateChangeEntry = &lib.StateChangeEntry{
			OperationType: lib.DbOperationTypeDelete,
			KeyBytes:      record.Key,
			Encoder:       nil,
			EncoderBytes:  nil,
		}

	case AncestralOperationUpdate:
		// If this was an update, we need to restore the previous value
		stateChangeEntry = &lib.StateChangeEntry{
			OperationType: lib.DbOperationTypeUpsert,
			KeyBytes:      record.Key,
			EncoderBytes:  record.PreviousValue,
		}

		// Decode the encoder if possible
		if isEncoder, encoder := lib.StateKeyToDeSoEncoder(record.Key); isEncoder && encoder != nil {
			stateChangeEntry.EncoderType = encoder.GetEncoderType()
			if len(record.PreviousValue) > 0 {
				dst := encoder.GetEncoderType().New()
				if ok, err := lib.DecodeFromBytes(dst, bytes.NewReader(record.PreviousValue)); ok && err == nil {
					stateChangeEntry.Encoder = dst
				}
			}
		} else {
			// Value encoded in key
			keyEncoder, err := lib.DecodeStateKey(record.Key, record.PreviousValue)
			if err == nil {
				stateChangeEntry.EncoderType = keyEncoder.GetEncoderType()
				stateChangeEntry.Encoder = keyEncoder
				stateChangeEntry.EncoderBytes = nil
			}
		}

	case AncestralOperationDelete:
		// If this was a delete, we need to restore the previous value
		stateChangeEntry = &lib.StateChangeEntry{
			OperationType: lib.DbOperationTypeUpsert,
			KeyBytes:      record.Key,
			EncoderBytes:  record.PreviousValue,
		}

		// Decode the encoder if possible
		if isEncoder, encoder := lib.StateKeyToDeSoEncoder(record.Key); isEncoder && encoder != nil {
			stateChangeEntry.EncoderType = encoder.GetEncoderType()
			if len(record.PreviousValue) > 0 {
				dst := encoder.GetEncoderType().New()
				if ok, err := lib.DecodeFromBytes(dst, bytes.NewReader(record.PreviousValue)); ok && err == nil {
					stateChangeEntry.Encoder = dst
				}
			}
		} else {
			// Value encoded in key
			keyEncoder, err := lib.DecodeStateKey(record.Key, record.PreviousValue)
			if err == nil {
				stateChangeEntry.EncoderType = keyEncoder.GetEncoderType()
				stateChangeEntry.Encoder = keyEncoder
				stateChangeEntry.EncoderBytes = nil
			}
		}

	default:
		return fmt.Errorf("unknown ancestral operation: %d", record.Operation)
	}

	// Apply the revert operation through the data handler
	batch := []*lib.StateChangeEntry{stateChangeEntry}
	if err := arm.dataHandler.HandleEntryBatch(batch, true); err != nil {
		return errors.Wrapf(err, "failed to apply revert operation for key: %x", record.Key)
	}

	return nil
}

// RevertMempoolChangesFromFiles reverts mempool changes using ancestral files
func (arm *AncestralRecordManager) RevertMempoolChangesFromFiles(ancestralFiles []*FileInfo) error {
	if len(ancestralFiles) == 0 {
		return nil // Nothing to revert
	}

	glog.V(2).Infof("Reverting mempool changes from %d ancestral files", len(ancestralFiles))

	// Process ancestral files in reverse timestamp order (newest first)
	for _, fileInfo := range ancestralFiles {
		glog.V(2).Infof("Processing ancestral file: %s", fileInfo.Path)

		records, err := arm.ReadAncestralRecordsFromFile(fileInfo.Path)
		if err != nil {
			return errors.Wrapf(err, "failed to read ancestral records from file: %s", fileInfo.Path)
		}

		// Apply revert operations in reverse order within the file
		for i := len(records) - 1; i >= 0; i-- {
			record := records[i]
			if err := arm.ApplyRevertOperation(record); err != nil {
				return errors.Wrapf(err, "failed to apply revert operation from file: %s", fileInfo.Path)
			}
		}

		glog.V(2).Infof("Successfully reverted %d operations from file: %s", len(records), fileInfo.Path)
	}

	return nil
}

// ValidateAncestralFile performs basic validation on an ancestral file
func (arm *AncestralRecordManager) ValidateAncestralFile(filePath string) error {
	records, err := arm.ReadAncestralRecordsFromFile(filePath)
	if err != nil {
		return errors.Wrapf(err, "failed to validate ancestral file: %s", filePath)
	}

	// Basic validation - ensure all records have valid operations and non-empty keys
	for i, record := range records {
		if len(record.Key) == 0 {
			return fmt.Errorf("ancestral record %d has empty key in file: %s", i, filePath)
		}

		if record.Operation > AncestralOperationDelete {
			return fmt.Errorf("ancestral record %d has invalid operation %d in file: %s", i, record.Operation, filePath)
		}

		// For updates and deletes, we expect to have previous values (though they can be empty for some entries)
		if record.Operation == AncestralOperationUpdate || record.Operation == AncestralOperationDelete {
			// Previous value can be empty for some types of entries (like follows, likes, etc.)
			// So we don't enforce this as a hard requirement
		}
	}

	return nil
}

// GetAncestralFileStats returns statistics about an ancestral file
func (arm *AncestralRecordManager) GetAncestralFileStats(filePath string) (map[AncestralOperation]int, error) {
	records, err := arm.ReadAncestralRecordsFromFile(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get stats for ancestral file: %s", filePath)
	}

	stats := map[AncestralOperation]int{
		AncestralOperationInsert: 0,
		AncestralOperationUpdate: 0,
		AncestralOperationDelete: 0,
	}

	for _, record := range records {
		stats[record.Operation]++
	}

	return stats, nil
}

// CleanupProcessedAncestralFiles removes ancestral files that have been processed
// This should be called after successful revert operations
func (arm *AncestralRecordManager) CleanupProcessedAncestralFiles(processedFiles []*FileInfo) error {
	for _, fileInfo := range processedFiles {
		if err := os.Remove(fileInfo.Path); err != nil {
			glog.Warningf("Failed to remove processed ancestral file %s: %v", fileInfo.Path, err)
			// Don't return error - cleanup failures shouldn't block processing
		} else {
			glog.V(2).Infof("Cleaned up processed ancestral file: %s", fileInfo.Path)
		}
	}
	return nil
}

// String returns a string representation of an ancestral record
func (record AncestralRecord) String() string {
	return fmt.Sprintf("AncestralRecord{op=%s, key=%x, valueLen=%d}",
		record.Operation, record.Key, len(record.PreviousValue))
}
