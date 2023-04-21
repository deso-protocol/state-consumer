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

// listenForBatchEvents consumes from the DBEntryChannel and calls the data handler to process the batch as it receives
// new entries.
func (consumer *StateSyncerConsumer) listenForBatchEvents() {
	for batchEvent := range consumer.DBEntryChannel {
		go consumer.callDataHandlerBatch(batchEvent)
	}
}

// callDataHandlerBatch calls the data handler to process a batch of entries.
func (consumer *StateSyncerConsumer) callDataHandlerBatch(batchedEntries []*lib.StateChangeEntry) {
	// Call the data handler to process the batch. We do this with retries, in case the data handler fails.
	err := consumer.callBatchWithRetries(batchedEntries, 0)
	if err != nil {
		glog.Fatalf("consumer.callDataHandlerBatch: %v", err)
	}
	// Remove a value from the blocking channel to allow the next batch to be processed.
	<-consumer.DBBlockingChannel
}

// callBatchWithRetries calls the data handler to process a batch of entries. If the call fails, it will retry
// with a smaller batch size until it succeeds or hits the max number of retries. These failures can happen due to
// an overloaded database or a duplicate key error.
func (consumer *StateSyncerConsumer) callBatchWithRetries(batchedEntries []*lib.StateChangeEntry, retries int) error {
	// Attempt to process the batch.
	if err := consumer.DataHandler.HandleEntryBatch(batchedEntries); err != nil {
		batchSize := len(batchedEntries)

		// Make sure the batch isn't empty. This should never happen.
		if batchSize == 0 {
			return errors.New("consumer.callBatchWithRetries: batch size is 0")
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
			return errors.Wrapf(err, "consumer.callBatchWithRetries: tried %d times to process batch", retries)
		} else if batchSize == 1 {
			time.Sleep(waitTime)
			// Set the operation type.
			batchedEntries[0].OperationType = operationType
			err = consumer.callBatchWithRetries(batchedEntries, retries)
			if err != nil {
				return errors.Wrapf(err, "consumer.callBatchWithRetries: ")
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
			err = consumer.callBatchWithRetries(batch1, retries)
			if err != nil {
				return errors.Wrapf(err, "consumer.callBatchWithRetries: ")
			}
			time.Sleep(waitTime)
			err = consumer.callBatchWithRetries(batch2, retries)
			if err != nil {
				return errors.Wrapf(err, "consumer.callBatchWithRetries: ")
			}

		}
	}
	fmt.Printf("Handled batch %d\n", consumer.BatchCount)
	consumer.BatchCount++
	return nil
}

// QueueBatch takes a slice of state change entries and add them to the appropriate channel if we are hypersyncing.
// If we are not hypersyncing, it calls the data handler directly.
func (consumer *StateSyncerConsumer) QueueBatch(batchedEntries []*lib.StateChangeEntry) error {
	if consumer.IsHypersyncing {
		// Add bool to blocking channel so that we can block the next batch from being processed if the channel is at capacity.
		consumer.DBBlockingChannel <- true
		// Add the state change entry batch to the channel so that it can be processed by the listener.
		consumer.DBEntryChannel <- batchedEntries
	} else {
		// When not in hypersync, just call the data handler directly.
		// We don't run transactions concurrently, as transactions may be dependent on each other.
		if err := consumer.callBatchWithRetries(batchedEntries, 0); err != nil {
			return errors.Wrapf(err, "consumer.QueueBatch: Error calling batch with retries")
		}
	}
	return nil
}

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

		// This queues the batch to be handled asynchronously, so that multiple batches can be processed at once.
		if err := consumer.QueueBatch(consumer.BatchedEntries); err != nil {
			return errors.Wrapf(err, "consumer.HandleEntryOperationBatch: Problem queuing batch")
		}

		// Save the consumer progress to file.
		handledEntries := len(UniqueEntries(consumer.BatchedEntries))
		err := consumer.saveConsumerProgressToFile(consumer.LastScannedIndex + uint32(handledEntries))
		if err != nil {
			return errors.Wrapf(err, "consumer.HandleEntryOperationBatch: Problem saving consumer progress to file")
		}
	}

	// This is either a brand new batched encoder instance, or the batched entries were just handled. Replace
	// the batch with an array containing the passed StateChangeEntry param.
	consumer.BatchedEntries = []*lib.StateChangeEntry{
		stateChangeEntry,
	}
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
