package consumer

import (
	"fmt"
	"github.com/deso-protocol/core/lib"
)

type BatchedEntry struct {
	Encoder *lib.DeSoEncoder
	Key     []byte
}

// BatchedEntries is a struct that stores a list of encoders that are of the same type. This is used to do bulk
// inserts into the database.
type BatchedEntries struct {
	EncoderType   lib.EncoderType
	OperationType lib.StateSyncerOperationType
	// The interface here will be one of our bun PG models.
	Entries []*BatchedEntry
}

// TODO: Make this function handle inserts AND deletes.
// HandleEntryOperationBatch handles the logic for batching entries to be inserted into the database.
func (consumer *StateSyncerConsumer) HandleEntryOperationBatch(key []byte, encoder *lib.DeSoEncoder, encoderType lib.EncoderType, dbOperationType lib.StateSyncerOperationType) error {
	// If the batched entries has been set, isn't empty, and matches the current encoder type and db operation,
	// and the entry batch isn't past the limit, add to the batch and return.
	if consumer.BatchedEntries != nil &&
		len(consumer.BatchedEntries.Entries) > 0 &&
		consumer.BatchedEntries.OperationType == dbOperationType &&
		encoderType == consumer.BatchedEntries.EncoderType &&
		len(consumer.BatchedEntries.Entries) < consumer.MaxBatchSize {
		consumer.BatchedEntries.Entries = append(consumer.BatchedEntries.Entries, &BatchedEntry{
			Encoder: encoder,
			Key:     key,
		})
		return nil
	} else if consumer.BatchedEntries != nil && len(consumer.BatchedEntries.Entries) > 0 {
		// If the batched entries do exist, but the batched encoder type and db operation don't match, or the max
		// batched size has been reached, then do the insert/upsert/delete.
		err := consumer.DataHandler.HandleEntryBatch(consumer.BatchedEntries)
		if err != nil {
			return err
		}
		fmt.Printf("Handled batch %d\n", consumer.BatchCount)
		handledEntries := len(UniqueEntries(consumer.BatchedEntries.Entries))
		err = consumer.saveConsumerProgressToFile(consumer.LastScannedIndex + uint32(handledEntries))
		if err != nil {
			return err
		}
		consumer.BatchCount = consumer.BatchCount + 1
	}

	// Since this is either a brand new batched encoder instance, or the batched entries were just inserted, replace
	// the batch with the current encoder.
	consumer.BatchedEntries = &BatchedEntries{
		EncoderType:   encoderType,
		OperationType: dbOperationType,
		Entries: []*BatchedEntry{
			&BatchedEntry{
				Encoder: encoder,
				Key:     key,
			},
		},
	}
	return nil
}

func UniqueEntries(entries []*BatchedEntry) []*BatchedEntry {
	uniqueEntryMap := make(map[string]bool)

	uniqueEntries := make([]*BatchedEntry, 0)

	// Loop through the encoders, and only add the unique ones to the return array.
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		keyString := string(entry.Key)
		if _, exists := uniqueEntryMap[keyString]; exists {
			continue
		} else {
			uniqueEntryMap[keyString] = true
			uniqueEntries = append(uniqueEntries, entry)
		}
	}
	return uniqueEntries
}

func KeysToDelete(entries []*BatchedEntry) [][]byte {
	keysToDelete := make([][]byte, len(entries))
	for i, entry := range entries {
		keysToDelete[i] = entry.Key
	}
	return keysToDelete
}
