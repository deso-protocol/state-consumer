# DeSo State Change Test Utilities

This package provides utilities for creating DeSo state change files for testing purposes. It allows you to create realistic test scenarios without requiring a running DeSo node.

## Overview

The testutils package enables you to:

- Create state change files in the correct Badger backup format
- Build DeSo entries (posts, profiles, follows, likes, diamonds) with proper encoding
- Generate complete test scenarios with multiple related files
- Test your state consumers and data handlers in isolation

## Quick Start

### Basic Usage

```go
import "github.com/deso-protocol/state-consumer/testutils"

// Create a simple state change file
builder := testutils.NewStateFileBuilder()
builder.WithPost(postHash, posterKey, "Hello world", timestamp).
    WithProfile(pkid, pubKey, "username", "description", "pic.jpg")

err := testutils.CreateBadgerBackupFile("state_changes_101.bin", builder.GetEntries())
```

### Using FileSet for Multiple Files

```go
fileSet := testutils.NewStateFileSet(tempDir)

// Create hypersync files  
err := fileSet.CreateHypersyncFileWithBuilder(100, 1, timestamp, 
    func(b *testutils.StateFileBuilder) *testutils.StateFileBuilder {
        return b.WithProfile(pkid, pubKey, "user1", "desc", "pic.jpg").
            WithPost(postHash, pubKey, "First post", timestamp)
    })

// Create committed block files
err = fileSet.CreateCommittedBlockFileWithBuilder(101, 
    func(b *testutils.StateFileBuilder) *testutils.StateFileBuilder {
        return b.WithLike(userPubKey, postHash).
            WithFollow(followerPKID, followedPKID)
    })
```

### Predefined Scenarios

```go
// Use predefined scenarios for common test cases
scenario := testutils.NewTestScenario("basic", "Basic social activity", tempDir)
err := scenario.CreateBasicSocialActivityScenario()

// Diamond-focused testing
err = scenario.CreateDiamondFocusedScenario()
```

## API Reference

### Core Types

#### `TestEntry`
Represents a single database entry that can be written to state change files.

```go
type TestEntry struct {
    Key       []byte
    Value     []byte
    EntryType lib.EncoderType
}
```

#### `StateFileBuilder` 
Fluent interface for building collections of state change entries.

**Methods:**
- `WithPost(postHash, posterPubKey, body, timestamp)` - Add a post entry
- `WithProfile(pkid, pubKey, username, description, profilePic)` - Add a profile entry  
- `WithFollow(followerPKID, followedPKID)` - Add a follow relationship
- `WithLike(likerPubKey, likedPostHash)` - Add a like entry
- `WithDiamond(senderPKID, receiverPKID, postHash, level)` - Add a diamond entry
- `WithCustomEntry(key, value, entryType)` - Add a custom entry
- `GetEntries()` - Get all accumulated entries
- `Reset()` - Clear all entries

#### `StateFileSet`
Manages creation of multiple related state change files with proper naming conventions.

**Methods:**
- `CreateHypersyncFile(blockHeight, chunkId, timestamp, entries)` - Create hypersync chunk file
- `CreateCommittedBlockFile(blockHeight, entries)` - Create committed block file
- `CreateMempoolFile(blockHeight, timestamp, entries)` - Create mempool file
- `CreateAncestralFile(blockHeight, timestamp, entries)` - Create ancestral record file
- `CreateEmptyAncestralFile(blockHeight, timestamp)` - Create empty ancestral file

#### `TestScenario`
Provides predefined test scenarios combining multiple files and realistic data.

**Methods:**
- `CreateBasicSocialActivityScenario()` - Create files simulating basic social activity
- `CreateDiamondFocusedScenario()` - Create scenario focused on diamond transactions

### Helper Functions

#### Test Data Creation
- `NewTestPKID(suffix)` - Create a test PKID
- `NewTestBlockHash(suffix)` - Create a test block hash  
- `NewTestPublicKey(suffix)` - Create a test public key
- `NewRandomPKID()` - Create a PKID with random bytes
- `NewRandomBlockHash()` - Create a block hash with random bytes

#### File Operations
- `CreateBadgerBackupFile(filePath, entries)` - Create a single Badger backup file

#### Database Keys
Database keys are generated automatically using the core library's prefix constants. No manual key generation is required - the builder methods handle this internally.

## File Format Details

The package creates files in the Badger backup format, which consists of:

1. **Length** (4 bytes) - Length of the protobuf data
2. **CRC** (4 bytes) - Checksum (currently placeholder)  
3. **Data** (variable) - Protobuf-serialized KVList

### File Naming Conventions

- **Hypersync**: `hypersync_chunk_{blockHeight}_{chunkId}_{timestamp}.bin`
- **Committed Block**: `state_changes_{blockHeight}.bin`
- **Mempool**: `mempool_{blockHeight}_{timestamp}.bin`
- **Ancestral**: `mempool_ancestral_{blockHeight}_{timestamp}.bin`

## Integration with State Consumer

The generated files can be consumed by any implementation of `consumer.StateSyncerDataHandler`:

```go
// Point your state consumer to the test directory
stateConsumer, err := consumer.NewStateSyncerConsumer(
    testDirectory,     // state change directory
    progressDirectory, // progress tracking directory  
    1,                // batch size
    1,                // thread limit
    false,            // sync mempool
    yourDataHandler,  // your handler implementation
)
```

## Examples

### Basic Social Activity Test

```go
func TestBasicSocialActivity(t *testing.T) {
    tempDir, _ := os.MkdirTemp("", "test_*")
    defer os.RemoveAll(tempDir)
    
    scenario := testutils.NewTestScenario("basic", "Basic test", tempDir)
    err := scenario.CreateBasicSocialActivityScenario()
    require.NoError(t, err)
    
    // Your test consumer reads from tempDir
    // ... test logic ...
}
```

### Custom Diamond Testing

```go
func TestDiamondUpgrades(t *testing.T) {
    tempDir, _ := os.MkdirTemp("", "diamonds_*")
    defer os.RemoveAll(tempDir)
    
    fileSet := testutils.NewStateFileSet(tempDir)
    
    // Block 101: Initial diamond
    err := fileSet.CreateCommittedBlockFileWithBuilder(101, 
        func(b *testutils.StateFileBuilder) *testutils.StateFileBuilder {
            return b.WithDiamond(senderPKID, receiverPKID, postHash, 1)
        })
    
    // Block 102: Upgrade diamond
    err = fileSet.CreateCommittedBlockFileWithBuilder(102,
        func(b *testutils.StateFileBuilder) *testutils.StateFileBuilder {
            return b.WithDiamond(senderPKID, receiverPKID, postHash, 3)
        })
    
    // Test diamond level progression
    // ... test logic ...
}
```

## Best Practices

1. **Use temporary directories**: Always create test files in temporary directories and clean them up
2. **Realistic data**: Use the helper functions to create realistic PKIDs and hashes
3. **Incremental timestamps**: Use increasing timestamps for proper ordering
4. **Predefined scenarios**: Start with predefined scenarios before creating custom ones
5. **Error handling**: Always check for errors when creating files

## Testing Without DeSo Node

This package is specifically designed to enable testing without requiring a running DeSo node. The generated files contain properly encoded DeSo entries that can be consumed by any state consumer implementation.

This is particularly useful for:
- Unit testing data handlers
- CI/CD pipelines where running a full DeSo node is impractical  
- Isolated testing of specific scenarios
- Performance testing with controlled data sets