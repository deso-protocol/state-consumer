package testutils

import (
	"crypto/rand"
	"fmt"

	"github.com/deso-protocol/core/lib"
)

// Helper functions for creating test data

// NewTestPKID creates a new PKID with test data
func NewTestPKID(suffix string) *lib.PKID {
	bytes := []byte(fmt.Sprintf("test_pkid_%s_dummy_bytes_here", suffix))
	if len(bytes) < 33 {
		// Pad to ensure minimum length
		padding := make([]byte, 33-len(bytes))
		bytes = append(bytes, padding...)
	}
	return lib.NewPKID(bytes[:33])
}

// NewTestBlockHash creates a new block hash with test data
func NewTestBlockHash(suffix string) *lib.BlockHash {
	bytes := []byte(fmt.Sprintf("test_hash_%s_dummy_bytes", suffix))
	if len(bytes) < 32 {
		// Pad to ensure minimum length
		padding := make([]byte, 32-len(bytes))
		bytes = append(bytes, padding...)
	}
	hash := lib.NewBlockHash(bytes[:32])
	return hash
}

// NewTestPublicKey creates a test public key
func NewTestPublicKey(suffix string) []byte {
	return []byte(fmt.Sprintf("test_public_key_%s", suffix))
}

// NewRandomPKID creates a PKID with random bytes
func NewRandomPKID() *lib.PKID {
	bytes := make([]byte, 33)
	rand.Read(bytes)
	return lib.NewPKID(bytes)
}

// NewRandomBlockHash creates a block hash with random bytes
func NewRandomBlockHash() *lib.BlockHash {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return lib.NewBlockHash(bytes)
}

// NewRandomPublicKey creates a public key with random bytes
func NewRandomPublicKey() []byte {
	bytes := make([]byte, 33)
	rand.Read(bytes)
	return bytes
}

// TestDataSet provides a set of related test data for scenarios
type TestDataSet struct {
	Users []TestUser
	Posts []TestPost
}

// TestUser represents a test user with related data
type TestUser struct {
	PKID      *lib.PKID
	PublicKey []byte
	Username  string
	Profile   *lib.ProfileEntry
}

// TestPost represents a test post with related data
type TestPost struct {
	Hash           *lib.BlockHash
	PosterPKID     *lib.PKID
	Body           string
	TimestampNanos uint64
	Entry          *lib.PostEntry
}

// NewTestDataSet creates a test data set with the specified number of users and posts
func NewTestDataSet(numUsers, numPosts int) *TestDataSet {
	dataset := &TestDataSet{
		Users: make([]TestUser, numUsers),
		Posts: make([]TestPost, numPosts),
	}

	// Create test users
	for i := 0; i < numUsers; i++ {
		suffix := fmt.Sprintf("%d", i+1)
		pkid := NewTestPKID(suffix)
		pubKey := NewTestPublicKey(suffix)
		username := fmt.Sprintf("testuser%d", i+1)

		profile := &lib.ProfileEntry{
			PublicKey:   pubKey,
			Username:    []byte(username),
			Description: []byte(fmt.Sprintf("Test user %d description", i+1)),
			ProfilePic:  []byte(fmt.Sprintf("https://example.com/pic%d.jpg", i+1)),
		}

		dataset.Users[i] = TestUser{
			PKID:      pkid,
			PublicKey: pubKey,
			Username:  username,
			Profile:   profile,
		}
	}

	// Create test posts
	for i := 0; i < numPosts; i++ {
		suffix := fmt.Sprintf("%d", i+1)
		hash := NewTestBlockHash(suffix)

		// Assign posts to users in round-robin fashion
		userIndex := i % numUsers
		if numUsers == 0 {
			userIndex = 0
		}

		var posterPKID *lib.PKID
		var posterPubKey []byte
		if numUsers > 0 {
			posterPKID = dataset.Users[userIndex].PKID
			posterPubKey = dataset.Users[userIndex].PublicKey
		} else {
			posterPKID = NewTestPKID(suffix)
			posterPubKey = NewTestPublicKey(suffix)
		}

		body := fmt.Sprintf("This is test post %d content", i+1)
		timestampNanos := uint64(1640995200000000000 + int64(i*1000000000)) // Increment by 1 second each

		entry := &lib.PostEntry{
			PostHash:        hash,
			PosterPublicKey: posterPubKey,
			Body:            []byte(body),
			TimestampNanos:  timestampNanos,
		}

		dataset.Posts[i] = TestPost{
			Hash:           hash,
			PosterPKID:     posterPKID,
			Body:           body,
			TimestampNanos: timestampNanos,
			Entry:          entry,
		}
	}

	return dataset
}

// GetUser returns the user at the specified index
func (d *TestDataSet) GetUser(index int) TestUser {
	if index < 0 || index >= len(d.Users) {
		panic(fmt.Sprintf("User index %d out of range [0, %d)", index, len(d.Users)))
	}
	return d.Users[index]
}

// GetPost returns the post at the specified index
func (d *TestDataSet) GetPost(index int) TestPost {
	if index < 0 || index >= len(d.Posts) {
		panic(fmt.Sprintf("Post index %d out of range [0, %d)", index, len(d.Posts)))
	}
	return d.Posts[index]
}
