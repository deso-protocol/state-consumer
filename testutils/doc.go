// Package testutils provides utilities for creating DeSo state change files for testing.
//
// This package allows easy construction of state change files that can be consumed by
// DeSo state processors, enabling unit testing without requiring a running DeSo node.
//
// Key Components:
//
// - StateFileBuilder: Fluent interface for building state change entries
// - StateFileSet: Manages creation of multiple related state change files
// - TestScenario: Provides predefined test scenarios for common use cases
// - Helper functions: Utilities for creating test data (PKIDs, hashes, etc.)
//
// Basic Usage:
//
//	// Create a simple state change file
//	builder := testutils.NewStateFileBuilder()
//	builder.WithPost(postHash, posterKey, "Hello world", timestamp).
//		WithProfile(pkid, pubKey, "username", "description", "pic.jpg")
//
//	err := testutils.CreateBadgerBackupFile("state_changes_101.bin", builder.GetEntries())
//
// Advanced Usage with FileSet:
//
//	// Create a complete test scenario
//	fileSet := testutils.NewStateFileSet(tempDir)
//
//	// Create hypersync files
//	err := fileSet.CreateHypersyncFileWithBuilder(100, 1, timestamp, func(b *StateFileBuilder) *StateFileBuilder {
//		return b.WithProfile(pkid, pubKey, "user1", "desc", "pic.jpg").
//			WithPost(postHash, pubKey, "First post", timestamp)
//	})
//
//	// Create committed block files
//	err = fileSet.CreateCommittedBlockFileWithBuilder(101, func(b *StateFileBuilder) *StateFileBuilder {
//		return b.WithLike(userPubKey, postHash).
//			WithFollow(followerPKID, followedPKID)
//	})
//
// Predefined Scenarios:
//
//	// Use predefined scenarios for common test cases
//	scenario := testutils.NewTestScenario("basic", "Basic social activity", tempDir)
//	err := scenario.CreateBasicSocialActivityScenario()
//
//	// Diamond-focused testing
//	err = scenario.CreateDiamondFocusedScenario()
//
// The package handles all the low-level details of:
// - Proper DeSo entry encoding using core library functions
// - Badger backup file format
// - Correct DB key generation using core library prefixes
// - File naming conventions
//
// This allows tests to focus on the business logic rather than file format details.
package testutils
