package consumer

import (
	"fmt"
	"github.com/deso-protocol/core/lib"
	"github.com/pkg/errors"
)

// HandleEntryOperationBatch handles the logic for batching entries to be inserted into the database.
func (consumer *StateSyncerConsumer) HandleEntryOperationBatch(stateChangeEntry *lib.StateChangeEntry) error {
	// If the batched entries has been set, isn't empty, and matches the current encoder type and db operation,
	// and the entry batch isn't past the limit, add to the batch and return.
	if len(consumer.BatchedEntries) > 0 &&
		consumer.BatchedEntries[0].OperationType == stateChangeEntry.OperationType &&
		stateChangeEntry.EncoderType == consumer.BatchedEntries[0].EncoderType &&
		len(consumer.BatchedEntries) < consumer.MaxBatchSize {
		consumer.BatchedEntries = append(consumer.BatchedEntries, stateChangeEntry)
		return nil
	} else if len(consumer.BatchedEntries) > 0 {
		// If the batched entries do exist, but the batched encoder type and db operation don't match, or the max
		// batched size has been reached, then do the insert/upsert/delete.
		err := consumer.DataHandler.HandleEntryBatch(consumer.BatchedEntries)
		if err != nil {
			return errors.Wrapf(err, "consumer.HandleEntryOperationBatch: Problem handling entry batch")
		}
		fmt.Printf("Handled batch %d\n", consumer.BatchCount)
		handledEntries := len(UniqueEntries(consumer.BatchedEntries))
		err = consumer.saveConsumerProgressToFile(consumer.LastScannedIndex + uint32(handledEntries))
		if err != nil {
			return errors.Wrapf(err, "consumer.HandleEntryOperationBatch: Problem saving consumer progress to file")
		}
		consumer.BatchCount = consumer.BatchCount + 1
	}

	// Since this is either a brand new batched encoder instance, or the batched entries were just inserted, replace
	// the batch with the current encoder.
	consumer.BatchedEntries = []*lib.StateChangeEntry{
		stateChangeEntry,
	}
	return nil
}

func UniqueEntries(entries []*lib.StateChangeEntry) []*lib.StateChangeEntry {
	uniqueEntryMap := make(map[string]bool)

	uniqueEntries := make([]*lib.StateChangeEntry, 0)

	// Loop through the encoders, and only add the unique ones to the return array.
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		keyString := string(entry.KeyBytes)
		if _, exists := uniqueEntryMap[keyString]; exists {
			continue
		} else {
			uniqueEntryMap[keyString] = true
			uniqueEntries = append(uniqueEntries, entry)
		}
	}
	return uniqueEntries
}

func KeysToDelete(entries []*lib.StateChangeEntry) [][]byte {
	keysToDelete := make([][]byte, len(entries))
	for i, entry := range entries {
		keysToDelete[i] = entry.KeyBytes
	}
	return keysToDelete
}
