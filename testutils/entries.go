package testutils

import (
	"github.com/deso-protocol/core/lib"
)

// TestEntry represents a test database entry that can be written to state change files
type TestEntry struct {
	Key       []byte
	Value     []byte
	EntryType lib.EncoderType
}

// StateFileBuilder provides a fluent interface for building DeSo state change files
type StateFileBuilder struct {
	entries []TestEntry
}

// NewStateFileBuilder creates a new builder for constructing state change files
func NewStateFileBuilder() *StateFileBuilder {
	return &StateFileBuilder{
		entries: make([]TestEntry, 0),
	}
}

// WithPost adds a post entry to the file
func (b *StateFileBuilder) WithPost(postHash *lib.BlockHash, posterPubKey []byte, body string, timestampNanos uint64) *StateFileBuilder {
	postEntry := &lib.PostEntry{
		PostHash:        postHash,
		PosterPublicKey: posterPubKey,
		Body:            []byte(body),
		TimestampNanos:  timestampNanos,
	}

	key := append([]byte{}, lib.Prefixes.PrefixPostHashToPostEntry...)
	key = append(key, postHash[:]...)
	value := lib.EncodeToBytes(100, postEntry) // Default block height 100

	b.entries = append(b.entries, TestEntry{
		Key:       key,
		Value:     value,
		EntryType: lib.EncoderTypePostEntry,
	})

	return b
}

// WithProfile adds a profile entry to the file
func (b *StateFileBuilder) WithProfile(pkid *lib.PKID, pubKey []byte, username, description, profilePic string) *StateFileBuilder {
	profileEntry := &lib.ProfileEntry{
		PublicKey:   pubKey,
		Username:    []byte(username),
		Description: []byte(description),
		ProfilePic:  []byte(profilePic),
	}

	key := append([]byte{}, lib.Prefixes.PrefixPKIDToProfileEntry...)
	key = append(key, pkid.ToBytes()...)
	value := lib.EncodeToBytes(100, profileEntry)

	b.entries = append(b.entries, TestEntry{
		Key:       key,
		Value:     value,
		EntryType: lib.EncoderTypeProfileEntry,
	})

	return b
}

// WithFollow adds a follow entry to the file
func (b *StateFileBuilder) WithFollow(followerPKID, followedPKID *lib.PKID) *StateFileBuilder {
	key := append([]byte{}, lib.Prefixes.PrefixFollowerPKIDToFollowedPKID...)
	key = append(key, followerPKID.ToBytes()...)
	key = append(key, followedPKID.ToBytes()...)
	// Follow entries have empty values - the data is in the key

	b.entries = append(b.entries, TestEntry{
		Key:       key,
		Value:     []byte{},
		EntryType: lib.EncoderTypeFollowEntry,
	})

	return b
}

// WithLike adds a like entry to the file
func (b *StateFileBuilder) WithLike(likerPubKey []byte, likedPostHash *lib.BlockHash) *StateFileBuilder {
	key := append([]byte{}, lib.Prefixes.PrefixLikerPubKeyToLikedPostHash...)
	key = append(key, likerPubKey...)
	key = append(key, likedPostHash[:]...)
	// Like entries have empty values - the data is in the key

	b.entries = append(b.entries, TestEntry{
		Key:       key,
		Value:     []byte{},
		EntryType: lib.EncoderTypeLikeEntry,
	})

	return b
}

// WithDiamond adds a diamond entry to the file
func (b *StateFileBuilder) WithDiamond(senderPKID, receiverPKID *lib.PKID, postHash *lib.BlockHash, diamondLevel int64) *StateFileBuilder {
	diamondEntry := &lib.DiamondEntry{
		SenderPKID:      senderPKID,
		ReceiverPKID:    receiverPKID,
		DiamondPostHash: postHash,
		DiamondLevel:    diamondLevel,
	}

	prefixCopy := append([]byte{}, lib.Prefixes.PrefixDiamondSenderPKIDDiamondReceiverPKIDPostHash...)
	key := append(prefixCopy, diamondEntry.SenderPKID[:]...)
	key = append(key, diamondEntry.ReceiverPKID[:]...)
	key = append(key, diamondEntry.DiamondPostHash[:]...)

	value := lib.EncodeToBytes(100, diamondEntry)

	b.entries = append(b.entries, TestEntry{
		Key:       key,
		Value:     value,
		EntryType: lib.EncoderTypeDiamondEntry,
	})

	return b
}

// WithCustomEntry adds a custom entry with full control
func (b *StateFileBuilder) WithCustomEntry(key, value []byte, entryType lib.EncoderType) *StateFileBuilder {
	b.entries = append(b.entries, TestEntry{
		Key:       key,
		Value:     value,
		EntryType: entryType,
	})

	return b
}

// GetEntries returns the accumulated test entries
func (b *StateFileBuilder) GetEntries() []TestEntry {
	return b.entries
}

// Reset clears all entries from the builder
func (b *StateFileBuilder) Reset() *StateFileBuilder {
	b.entries = make([]TestEntry, 0)
	return b
}

// Helper functions for creating specific types of entries
func CreateTestPost(postHash *lib.BlockHash, posterPubKey []byte, body string, timestampNanos uint64) TestEntry {
	return NewStateFileBuilder().WithPost(postHash, posterPubKey, body, timestampNanos).GetEntries()[0]
}

func CreateTestProfile(pkid *lib.PKID, pubKey []byte, username, description, profilePic string) TestEntry {
	return NewStateFileBuilder().WithProfile(pkid, pubKey, username, description, profilePic).GetEntries()[0]
}

func CreateTestFollow(followerPKID, followedPKID *lib.PKID) TestEntry {
	return NewStateFileBuilder().WithFollow(followerPKID, followedPKID).GetEntries()[0]
}

func CreateTestLike(likerPubKey []byte, likedPostHash *lib.BlockHash) TestEntry {
	return NewStateFileBuilder().WithLike(likerPubKey, likedPostHash).GetEntries()[0]
}

func CreateTestDiamond(senderPKID, receiverPKID *lib.PKID, postHash *lib.BlockHash, diamondLevel int64) TestEntry {
	return NewStateFileBuilder().WithDiamond(senderPKID, receiverPKID, postHash, diamondLevel).GetEntries()[0]
}
