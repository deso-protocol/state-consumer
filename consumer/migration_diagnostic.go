package consumer

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/deso-protocol/core/lib"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

// attemptMigrationHeightDiagnostic tries decoding the entry with different migration heights
func (consumer *StateSyncerConsumer) attemptMigrationHeightDiagnostic(
	buffer []byte,
	decodeErr error,
	blockHeight uint64,
) {
	// Only proceed if:
	// 1. Diagnostic is enabled
	// 2. Error contains "encoder type" AND "doesn't match"
	if !consumer.EnableMigrationHeightDiagnostic {
		return
	}

	errStr := decodeErr.Error()
	if !strings.Contains(errStr, "encoder type") || !strings.Contains(errStr, "doesn't match") {
		return
	}

	glog.Infof("\n========================================")
	glog.Infof("MIGRATION HEIGHT DIAGNOSTIC MODE")
	glog.Infof("========================================")
	glog.Infof("Error: %v", decodeErr)
	glog.Infof("Current Block Height: %d", blockHeight)
	glog.Infof("Buffer Size: %d bytes", len(buffer))
	glog.Infof("Testing all migration heights...\n")

	// Get all migration heights for the network
	migrationHeights := GetMigrationHeights(consumer.Params)

	successfulHeights := []MigrationHeightInfo{}
	failedHeights := map[uint64]string{}

	// Test each migration height
	for _, migrationInfo := range migrationHeights {
		entry, success, err := tryDecodeWithHeight(buffer, migrationInfo.Height)

		if success {
			successfulHeights = append(successfulHeights, migrationInfo)
			glog.Infof("✓ SUCCESS - Height %d (%s, Version %d)",
				migrationInfo.Height, migrationInfo.Name, migrationInfo.Version)
			glog.Infof("  Entry Details:")
			glog.Infof("    - EncoderType: %d (%v)", entry.EncoderType, entry.EncoderType)
			glog.Infof("    - OperationType: %d", entry.OperationType)
			glog.Infof("    - BlockHeight: %d", entry.BlockHeight)
			glog.Infof("    - FlushID: %v", entry.FlushId)
			glog.Infof("    - IsReverted: %v", entry.IsReverted)
			if entry.Encoder != nil {
				glog.Infof("    - Encoder: %T", entry.Encoder)
			}
			glog.Infof("")
		} else {
			failedHeights[migrationInfo.Height] = err.Error()
		}
	}

	// Summary
	glog.Infof("========================================")
	glog.Infof("DIAGNOSTIC SUMMARY")
	glog.Infof("========================================")
	glog.Infof("Successful Heights: %d/%d", len(successfulHeights), len(migrationHeights))

	if len(successfulHeights) > 0 {
		glog.Infof("\n✓ Successful Migration Heights:")
		for _, mh := range successfulHeights {
			glog.Infof("  - %d (%s, Version %d)", mh.Height, mh.Name, mh.Version)
		}

		glog.Infof("\n📊 ANALYSIS:")
		if len(successfulHeights) == 1 {
			mh := successfulHeights[0]
			glog.Infof("  → Exactly ONE height works: %d (%s)", mh.Height, mh.Name)
			glog.Infof("  → This suggests the state-changes file was written with Height %d", mh.Height)
			glog.Infof("  → But your consumer is using Height %d for decoding", blockHeight)
			glog.Infof("  → SOLUTION: Ensure your core version matches the node that wrote the file")
		} else {
			glog.Infof("  → Multiple heights work. This could indicate:")
			glog.Infof("    - The entry doesn't have migration-dependent fields")
			glog.Infof("    - The differences between migrations don't affect this entry type")
		}
	} else {
		glog.Infof("\n✗ NO successful heights found")
		glog.Infof("\n📊 ANALYSIS:")
		glog.Infof("  → None of the known migration heights can decode this entry")
		glog.Infof("  → This suggests:")
		glog.Infof("    - File corruption at this position")
		glog.Infof("    - Unknown/future migration height")
		glog.Infof("    - Incompatible core version")
		glog.Infof("\n  → Failed Height Errors (first 3):")
		count := 0
		for height, errMsg := range failedHeights {
			if count >= 3 {
				break
			}
			// Truncate error message if too long
			if len(errMsg) > 150 {
				errMsg = errMsg[:150] + "..."
			}
			glog.Infof("    Height %d: %v", height, errMsg)
			count++
		}
	}

	glog.Infof("========================================\n")
}

// tryDecodeWithHeight attempts to decode a buffer using a specific block height
func tryDecodeWithHeight(buffer []byte, blockHeight uint64) (*lib.StateChangeEntry, bool, error) {
	// Create a bytes reader
	rr := bytes.NewReader(buffer)

	// Create a new state change entry
	entry := &lib.StateChangeEntry{}

	// Decode OperationType
	operationType, err := lib.ReadUvarint(rr)
	if err != nil {
		return nil, false, fmt.Errorf("error decoding operation type: %v", err)
	}
	entry.OperationType = lib.StateSyncerOperationType(operationType)

	// Decode IsReverted
	isReverted, err := lib.ReadBoolByte(rr)
	if err != nil {
		return nil, false, fmt.Errorf("error decoding is reverted: %v", err)
	}
	entry.IsReverted = isReverted

	// Decode EncoderType
	encoderType, err := lib.ReadUvarint(rr)
	if err != nil {
		return nil, false, fmt.Errorf("error decoding encoder type: %v", err)
	}
	entry.EncoderType = lib.EncoderType(encoderType)

	// Decode KeyBytes
	entry.KeyBytes, err = lib.DecodeByteArray(rr)
	if err != nil {
		return nil, false, fmt.Errorf("error decoding key bytes: %v", err)
	}

	// Decode the encoder with our custom blockHeight
	encoder := entry.EncoderType.New()
	if exist, err := decodeFromBytesWithCustomHeight(encoder, rr, blockHeight); exist && err == nil {
		entry.Encoder = encoder
		entry.EncoderBytes = lib.EncodeToBytes(blockHeight, encoder)
	} else if err != nil {
		return nil, false, fmt.Errorf("error decoding encoder: %v", err)
	}

	// Decode ancestral record
	ancestralRecord := entry.EncoderType.New()
	if exist, err := decodeFromBytesWithCustomHeight(ancestralRecord, rr, blockHeight); exist && err == nil {
		entry.AncestralRecord = ancestralRecord
		entry.AncestralRecordBytes = lib.EncodeToBytes(blockHeight, ancestralRecord)
	} else if err != nil {
		return nil, false, fmt.Errorf("error decoding ancestral record: %v", err)
	}

	// Decode FlushID
	flushIdBytes := make([]byte, 16)
	_, err = rr.Read(flushIdBytes)
	if err != nil {
		return nil, false, fmt.Errorf("error decoding flush UUID: %v", err)
	}
	entry.FlushId, err = uuid.FromBytes(flushIdBytes)
	if err != nil {
		return nil, false, fmt.Errorf("error parsing flush UUID: %v", err)
	}

	// Decode block height
	entryBlockHeight, err := lib.ReadUvarint(rr)
	if err != nil {
		return nil, false, fmt.Errorf("error decoding block height: %v", err)
	}
	entry.BlockHeight = entryBlockHeight

	// Decode the block if this is a utxo operation
	if entry.EncoderType == lib.EncoderTypeUtxoOperation || entry.EncoderType == lib.EncoderTypeUtxoOperationBundle {
		block := &lib.MsgDeSoBlock{}
		if exist, err := lib.DecodeFromBytes(block, rr); exist && err == nil {
			entry.Block = block
		} else if err != nil {
			return nil, false, fmt.Errorf("error decoding block: %v", err)
		}
	}

	// Success!
	return entry, true, nil
}

// decodeFromBytesWithCustomHeight is a modified version of lib.DecodeFromBytes that uses a custom block height
func decodeFromBytesWithCustomHeight(encoder lib.DeSoEncoder, rr io.Reader, blockHeight uint64) (bool, error) {
	bytesReader, ok := rr.(*bytes.Reader)
	if !ok {
		return false, fmt.Errorf("reader must be *bytes.Reader")
	}

	if existenceByte, err := lib.ReadBoolByte(bytesReader); existenceByte && err == nil {
		encoderType, err := lib.ReadUvarint(bytesReader)
		if err != nil {
			return false, fmt.Errorf("error decoding encoder type: %v", err)
		}

		// Verify encoder type matches
		if lib.EncoderType(encoderType) != encoder.GetEncoderType() {
			return false, fmt.Errorf("encoder type (%v) doesn't match entry type (%v)",
				encoderType, encoder.GetEncoderType())
		}

		// Read version byte (but don't use it - we're using our custom height)
		_, err = lib.ReadUvarint(bytesReader)
		if err != nil {
			return false, fmt.Errorf("error decoding version byte: %v", err)
		}

		// Decode with our custom blockHeight
		err = encoder.RawDecodeWithoutMetadata(blockHeight, bytesReader)
		if err != nil {
			return false, fmt.Errorf("error in RawDecodeWithoutMetadata: %v", err)
		}
		return true, nil
	} else if err != nil {
		return false, err
	}
	return false, nil
}

// tryDecodeAtPositionWithAllHeights attempts to decode at a position using all migration heights
// Returns the entry, success status, migration height that worked, bytes read, and any error
func tryDecodeAtPositionWithAllHeights(file *os.File, position int64, migrationHeights []MigrationHeightInfo) (*lib.StateChangeEntry, bool, uint64, uint64, error) {
	// Try each migration height
	for _, migrationInfo := range migrationHeights {
		entry, success, bytesRead, err := tryDecodeAtPositionWithHeight(file, position, migrationInfo.Height)
		if success {
			return entry, true, migrationInfo.Height, bytesRead, nil
		}
		// If we got an error but it's not about EOF/unexpected EOF, continue trying other heights
		if err != nil && !strings.Contains(err.Error(), "EOF") {
			continue
		}
	}
	return nil, false, 0, 0, fmt.Errorf("no migration height could decode at position %d", position)
}

// tryDecodeAtPositionWithHeight attempts to decode at a specific position with a specific migration height
func tryDecodeAtPositionWithHeight(file *os.File, position int64, blockHeight uint64) (*lib.StateChangeEntry, bool, uint64, error) {
	// Save original position
	originalPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, false, 0, err
	}
	defer file.Seek(originalPos, io.SeekStart)

	// Seek to the requested position
	if _, err := file.Seek(position, io.SeekStart); err != nil {
		return nil, false, 0, err
	}

	// Create a temporary reader
	tempReader := bufio.NewReader(file)

	// Try to read the entry size (varint)
	entryByteSize, err := lib.ReadUvarint(tempReader)
	if err != nil {
		return nil, false, 0, err
	}

	// Sanity check on entry size
	if entryByteSize == 0 || entryByteSize > 100*1024*1024 { // Max 100MB per entry
		return nil, false, 0, fmt.Errorf("invalid entry size: %d", entryByteSize)
	}

	// Check if slice size is safe
	if err := CheckSliceSize(int(entryByteSize)); err != nil {
		return nil, false, 0, err
	}

	// Create a buffer to hold the entry
	buffer := make([]byte, entryByteSize)
	bytesRead, err := io.ReadFull(tempReader, buffer)
	if err != nil {
		return nil, false, 0, err
	}
	if bytesRead < int(entryByteSize) {
		return nil, false, 0, fmt.Errorf("not enough bytes read: expected %d, got %d", entryByteSize, bytesRead)
	}

	// Try to decode with the specified block height
	entry, success, err := tryDecodeWithHeight(buffer, blockHeight)
	if !success || err != nil {
		return nil, false, entryByteSize, err
	}

	return entry, true, entryByteSize, nil
}
