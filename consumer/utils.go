package consumer

import (
	"bytes"

	"github.com/deso-protocol/core/lib"
	lru "github.com/hashicorp/golang-lru/v2"
)

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

// FilterCachedEntries takes a slice of entries and a map of cached entries, and returns a slice of entries that are not
// in the cached entries map.
func FilterCachedEntries(entries []*lib.StateChangeEntry, cachedEntries *lru.Cache[string, []byte]) []*lib.StateChangeEntry {
	filteredEntries := make([]*lib.StateChangeEntry, 0)

	for _, entry := range entries {
		if cachedEntry, exists := cachedEntries.Get(string(entry.KeyBytes)); !exists || !bytes.Equal(cachedEntry, entry.EncoderBytes) {
			filteredEntries = append(filteredEntries, entry)
		}
	}
	return filteredEntries
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
