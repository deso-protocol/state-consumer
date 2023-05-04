package consumer

import (
	"fmt"
	"github.com/deso-protocol/core/lib"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"math"
	"time"
)

const (
	retryLimit = 10
)

// BatchIndex is a struct that contains the index of the first entry in a batch, and the batch's index relative to all other executed batches.
type BatchIndexInfo struct {
	MinEntryIndex uint64
	Index         uint64
}

// manageBatchedEntries calls the data handler to process a batch of entries, and calculates & logs the current batch progress.
func (consumer *StateSyncerConsumer) manageBatchedEntries(batchedEntries []*lib.StateChangeEntry, entryCount uint64, batchCount uint64) {
	// Call the data handler to process the batch. We do this with retries, in case the data handler fails.
	err := consumer.callHandlerWithRetries(batchedEntries, 0)
	if err != nil {
		glog.Fatalf("consumer.manageBatchedEntries: %v", err)
	}

	// Upon success, add the batch index info to the batch index slice.
	batchInfo := &BatchIndexInfo{
		MinEntryIndex: entryCount + uint64(len(batchedEntries)),
		Index:         batchCount,
	}
	consumer.BatchIndexes = insertBatchIndexInOrder(consumer.BatchIndexes, batchInfo)

	// Log progress.
	fmt.Printf("Handled batch %d\n", batchCount)

	// If the number of batches is greater than the thread limit, remove the first batch index from the slice.
	// We know that anything outside the bounds of the thread limit must have already been processed successfully.
	if len(consumer.BatchIndexes) > consumer.ThreadLimit {
		consumer.BatchIndexes = consumer.BatchIndexes[1:]
	}

	lastConsecutiveBatchEntryIndex := consumer.findLastConsecutiveBatchEntryIndex()

	if err = consumer.saveConsumerProgressToFile(lastConsecutiveBatchEntryIndex); err != nil {
		glog.Errorf("consumer.manageBatchedEntries: %v", err)
	}

	// Remove a value from the blocking channel to allow the next batch to be processed.
	<-consumer.DBBlockingChannel
	// Decrement the blocking wait group. This is used at the very end of hypersync to wait for all batches to be processed.
	consumer.DBBlockingWG.Done()
}

// findLastConsecutiveBatchEntryIndex finds the last consecutive batch entry index. Because batches are processed
// asynchronously, there may be gaps in batches that have been successfully processed (e.g. batches 1, 2, 4, and 7 may
// have been processed, but batches 3, 5, and 6 may not have been processed yet). When resuming from a failed state,
// rather than tracking every unprocessed batch, we just track the last consecutive batch entry index that was processed
// successfully (2 in the example above) and start processing from there.
func (consumer *StateSyncerConsumer) findLastConsecutiveBatchEntryIndex() uint64 {
	// Starting from batch 0, the last index that was processed successfully.
	var lastConsecutiveBatchEntryIndex uint64
	for ii, batchIndex := range consumer.BatchIndexes {
		if ii == 0 {
			// We know every batch prior to the first batch index in the slice has been processed successfully.
			lastConsecutiveBatchEntryIndex = batchIndex.MinEntryIndex
			// Continue to avoid index out of range error below.
			continue
		}

		// If the current batch index is not consecutive with the prior batch index, break.
		lastBatchIndex := consumer.BatchIndexes[ii-1].Index
		if batchIndex.Index != lastBatchIndex+1 {
			break
		}
		// Otherwise, set the last consecutive batch entry index to the current batch index's min entry index.
		lastConsecutiveBatchEntryIndex = batchIndex.MinEntryIndex
	}
	return lastConsecutiveBatchEntryIndex
}

// insertBatchIndexInOrder inserts a batch index info into a slice of batch index infos in ascending Index order.
func insertBatchIndexInOrder(batchIndexes []*BatchIndexInfo, newIndexInfo *BatchIndexInfo) []*BatchIndexInfo {
	// Find the position to insert the newIndexInfo
	position := -1
	for i := len(batchIndexes) - 1; i >= 0; i-- {
		if batchIndexes[i].Index < newIndexInfo.Index {
			position = i + 1
			break
		}
	}

	// Insert the newIndexInfo at the found position
	batchIndexes = append(batchIndexes, nil) // Extend the capacity of the slice
	if position == -1 {
		copy(batchIndexes[1:], batchIndexes)
		batchIndexes[0] = newIndexInfo
	} else {
		copy(batchIndexes[position+1:], batchIndexes[position:])
		batchIndexes[position] = newIndexInfo
	}

	return batchIndexes
}

// callHandlerWithRetries calls the data handler to process a batch of entries. If the call fails, it will retry
// with a smaller batch size until it succeeds or hits the max number of retries. These failures can happen due to
// an overloaded database or a duplicate key error.
func (consumer *StateSyncerConsumer) callHandlerWithRetries(batchedEntries []*lib.StateChangeEntry, retries int) error {
	// Attempt to process the batch.
	if err := consumer.DataHandler.HandleEntryBatch(batchedEntries); err != nil {
		batchSize := len(batchedEntries)

		// Make sure the batch isn't empty. This should never happen.
		if batchSize == 0 {
			return errors.New("consumer.callHandlerWithRetries: batch size is 0")
		}

		fmt.Printf("Received error with batch of size %d: %v\n", batchSize, err)

		// If an insert is being performed, try performing an upsert instead.
		// This is useful for when the database runs into duplicate key errors.
		operationType := batchedEntries[0].OperationType
		if operationType == lib.DbOperationTypeInsert {
			operationType = lib.DbOperationTypeUpsert
		}

		// Exponential backoff for retries
		waitTime := 5 * time.Duration(math.Pow(2, float64(retries))) * time.Second

		// If we've hit the max number of retries, return the error.
		if retries > retryLimit {
			return errors.Wrapf(err, "consumer.callHandlerWithRetries: tried %d times to process batch", retries)
		} else if batchSize == 1 {
			time.Sleep(waitTime)
			// Set the operation type.
			batchedEntries[0].OperationType = operationType
			err = consumer.callHandlerWithRetries(batchedEntries, retries)
			if err != nil {
				return errors.Wrapf(err, "consumer.callHandlerWithRetries: ")
			}
		} else {
			// If we failed to process a batch, try processing the batch in halves. This can be useful if the reason
			// for failure was a db timeout.
			batch1 := batchedEntries[:batchSize/2]
			batch2 := batchedEntries[batchSize/2:]
			// Set the operation type.
			batch1[0].OperationType = operationType
			batch2[0].OperationType = operationType
			time.Sleep(waitTime)
			err = consumer.callHandlerWithRetries(batch1, retries)
			if err != nil {
				return errors.Wrapf(err, "consumer.callHandlerWithRetries: ")
			}
			time.Sleep(waitTime)
			err = consumer.callHandlerWithRetries(batch2, retries)
			if err != nil {
				return errors.Wrapf(err, "consumer.callHandlerWithRetries: ")
			}

		}
	}
	return nil
}

// QueueBatch takes a slice of state change entries and add them to the appropriate channel if we are hypersyncing.
// If we are not hypersyncing, it calls the data handler directly.
func (consumer *StateSyncerConsumer) QueueBatch(batchedEntries []*lib.StateChangeEntry) error {
	if consumer.IsHypersyncing {
		// Add bool to blocking channel so that we can block the next batch from being processed if the channel is at capacity.
		consumer.DBBlockingChannel <- true
		consumer.DBBlockingWG.Add(1)
		// Handle the batched entries in a non-blocking way.
		go consumer.manageBatchedEntries(batchedEntries, consumer.EntryCount, consumer.BatchCount)
		consumer.BatchCount++
		consumer.EntryCount += uint64(len(batchedEntries))

		//// Add the state change entry batch to the channel so that it can be processed by the listener.
		//consumer.DBEntryChannel <- batchedEntries
	} else {
		// When not in hypersync, just call the data handler directly.
		// We don't processNewEntriesInFile transactions concurrently, as transactions may be dependent on each other.
		if err := consumer.callHandlerWithRetries(batchedEntries, 0); err != nil {
			return errors.Wrapf(err, "consumer.QueueBatch: Error calling batch with retries")
		}
		// If the batch was successfully processed, increment the entry count.
		consumer.EntryCount += uint64(len(batchedEntries))
		// Save the consumer progress to file.
		if err := consumer.saveConsumerProgressToFile(consumer.EntryCount); err != nil {
			return errors.Wrapf(err, "consumer.QueueBatch: Error saving consumer progress to file")
		}
	}
	return nil
}

// HandleEntryOperationBatch handles a batch of state change entries. It will batch entries of the same type and
// encoder type together, and will call the data handler when the batch is full or when the encoder type or db
// operation changes.
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

		if err := consumer.executeBatch(); err != nil {
			return errors.Wrapf(err, "consumer.HandleEntryOperationBatch: Problem executing batch")
		}
	}

	// This is either a brand new batched encoder instance, or the batched entries were just handled. Replace
	// the batch with an array containing the passed StateChangeEntry param.
	consumer.BatchedEntries = []*lib.StateChangeEntry{
		stateChangeEntry,
	}
	return nil
}

// executeBatch executes the batched entries and saves the consumer progress to file.
func (consumer *StateSyncerConsumer) executeBatch() error {
	if consumer.BatchedEntries == nil || len(consumer.BatchedEntries) == 0 {
		return nil
	}
	// This queues the batch to be handled asynchronously, so that multiple batches can be processed at once.
	if err := consumer.QueueBatch(consumer.BatchedEntries); err != nil {
		return errors.Wrapf(err, "consumer.HandleEntryOperationBatch: Problem queuing batch")
	}

	// Reset the batched entries to an empty array after executing them.
	consumer.BatchedEntries = []*lib.StateChangeEntry{}
	return nil
}

// UniqueEntries takes a slice of state change entries and returns a slice of unique entries.
// It de-duplicates based on the key bytes.
func UniqueEntries(entries []*lib.StateChangeEntry) []*lib.StateChangeEntry {
	uniqueEntryMap := make(map[string]bool)

	uniqueEntries := make([]*lib.StateChangeEntry, 0)

	// Loop through the encoders, and only add the unique ones to the return array.
	// Loop through them in reverse so that in the case of duplicates, the most recent entry is kept.
	for ii := len(entries) - 1; ii >= 0; ii-- {
		entry := entries[ii]
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

// KeysToDelete takes a slice of state change entries and returns a slice of key bytes. This helper can be used by
// the data handler to construct a slice of IDs to delete given a slice of StateChangeEntries.
func KeysToDelete(entries []*lib.StateChangeEntry) [][]byte {
	keysToDelete := make([][]byte, len(entries))
	for i, entry := range entries {
		keysToDelete[i] = entry.KeyBytes
	}
	return keysToDelete
}
