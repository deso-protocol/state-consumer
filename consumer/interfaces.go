package consumer

import "github.com/deso-protocol/core/lib"

type SyncEvent uint8

const (
	// We intentionally skip zero as otherwise that would be the default value.
	SyncEventStart             SyncEvent = 0
	SyncEventHypersyncComplete SyncEvent = 1
	SyncEventComplete          SyncEvent = 2
)

// The StateSyncerDataHandler interface is implemented by the data handler implementation. It is used by the
// consumer to get the relevant encoder for a given prefix id.
type StateSyncerDataHandler interface {
	HandleEntry(key []byte, encoder lib.DeSoEncoder, encoderType lib.EncoderType, dbOprationType lib.StateSyncerOperationType) error
	HandleEntryBatch(batchedEntries *BatchedEntries) error
	HandleSyncEvent(syncEvent SyncEvent) error
}
