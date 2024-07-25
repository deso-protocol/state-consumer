package consumer

import "github.com/deso-protocol/core/lib"

type SyncEvent uint8

// SyncEvent is an enum that represents the different sync events that can occur while consuming DeSo state.
const (
	// We intentionally skip zero as otherwise that would be the default value.
	SyncEventStart             SyncEvent = 0
	SyncEventHypersyncStart    SyncEvent = 1
	SyncEventHypersyncComplete SyncEvent = 2
	// TODO: implement this. Should fire when the consumer has caught up to the tip.
	SyncEventComplete       SyncEvent = 3
	SyncEventBlocksyncStart SyncEvent = 4
)

// The StateSyncerDataHandler interface is implemented by the data handler implementation. It is used by the
// consumer to get the relevant encoder for a given prefix id.
type StateSyncerDataHandler interface {
	HandleEntryBatch(batchedEntries []*lib.StateChangeEntry) error
	HandleSyncEvent(syncEvent SyncEvent) error
	InitiateTransaction() error
	CommitTransaction() error
	RollbackTransaction() error
	GetParams() *lib.DeSoParams
}
