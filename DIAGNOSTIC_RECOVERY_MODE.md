# Diagnostic Recovery Mode

## Overview

A new diagnostic mode has been implemented to help identify and diagnose file position misalignment issues in the state consumer when encoder type mismatch errors occur during initial sync of committed entries.

## How It Works

When the consumer encounters an "encoder type doesn't match" error during committed entry decoding, it can automatically:

1. **Search backward and/or forward byte-by-byte** from the error position to find valid StateChangeEntry candidates
2. **Validate each candidate** by attempting to read forward and requiring a minimum number of successful reads
3. **Continue searching if validation fails** - if a candidate doesn't pass forward reading validation, the search continues
4. **Report diagnostic information** about the found entry, validation results, and the offset from the error position
5. **Provide detailed logs** to help understand the extent of corruption

## Configuration

### Environment Variables

#### MAX_RECOVERY_LOOKBACK_BYTES (Required)
Set this environment variable to enable diagnostic mode:

```bash
export MAX_RECOVERY_LOOKBACK_BYTES=10000  # Search up to 10KB in each direction
```

- **Value**: Number of bytes to search (must be > 0 to enable)
- **Default**: 0 (disabled)
- **Recommended**: Start with 10000 (10KB) and increase if needed

#### RECOVERY_SEARCH_DIRECTION (Optional)
Controls which direction(s) to search:

```bash
export RECOVERY_SEARCH_DIRECTION=both  # Search backward, then forward if needed
```

- **Values**: `backward`, `forward`, or `both`
- **Default**: `backward`
- **Description**:
  - `backward`: Search backward from error position (classic mode)
  - `forward`: Search forward from error position
  - `both`: Try backward first, then forward if backward finds nothing

#### MIN_SUCCESSFUL_FORWARD_READS (Optional)
Sets the minimum number of entries that must successfully decode forward to validate a candidate:

```bash
export MIN_SUCCESSFUL_FORWARD_READS=5  # Require 5 successful forward reads
```

- **Value**: Number of successful forward reads required (integer)
- **Default**: 3
- **Description**: When a candidate entry is found, the diagnostic tool reads forward from that position. The candidate is only accepted if at least this many entries decode successfully. This prevents false positives where random bytes happen to decode as an entry but aren't actually valid.

### Activation Conditions

Diagnostic mode ONLY activates when:
- ✅ `MAX_RECOVERY_LOOKBACK_BYTES` > 0
- ✅ Error is for **committed entries** (NOT mempool entries)
- ✅ Error message contains "encoder type" AND "doesn't match"
- ✅ During the initial sync (optional check via `SyncingFromBeginning` flag)

## Example Usage

### Basic Usage (Backward Search Only)
```bash
export MAX_RECOVERY_LOOKBACK_BYTES=50000
./postgres-data-handler
```

### Search Both Directions
```bash
export MAX_RECOVERY_LOOKBACK_BYTES=100000
export RECOVERY_SEARCH_DIRECTION=both
export MIN_SUCCESSFUL_FORWARD_READS=5
./postgres-data-handler
```

### Forward Search Only
```bash
export MAX_RECOVERY_LOOKBACK_BYTES=50000
export RECOVERY_SEARCH_DIRECTION=forward
./postgres-data-handler
```

## Output Format

When diagnostic mode activates, you'll see output like:

```
=== DIAGNOSTIC RECOVERY MODE ACTIVATED ===
Error: DecodeFromBytes: encoder type (11918) doesn't match the entry type (43)
Current Position: 781366087101 bytes
Max Search Distance: 1000000000 bytes
Search Direction: both
Entry Type: Committed (NOT Mempool)
Initial Sync Mode: false
Minimum Successful Forward Reads: 3

Searching backward...
Searched back 1000 bytes... (checked 0 candidates)
Searched back 2000 bytes... (checked 1 candidates)

Candidate found at position 781366085000 (-2101 bytes), verifying by reading forward...
Entry #1 at position 781366085000: EncoderType=16, OpType=2, Height=12345, Size=2048 bytes
Entry #2 at position 781366087048: EncoderType=21, OpType=2, Height=12346, Size=512 bytes
✗ Candidate rejected: only 2 successful forward reads (need 3), continuing search...

Candidate found at position 781366082500 (-4601 bytes), verifying by reading forward...
Entry #1 at position 781366082500: EncoderType=43, OpType=2, Height=12340, Size=1024 bytes
Entry #2 at position 781366083524: EncoderType=16, OpType=2, Height=12341, Size=2048 bytes
Entry #3 at position 781366085572: EncoderType=21, OpType=2, Height=12342, Size=512 bytes
Entry #4 at position 781366086084: EncoderType=16, OpType=2, Height=12343, Size=1536 bytes
... (up to 50 entries shown)
✓ Candidate validated with 50 successful forward reads

✓ SUCCESSFUL DECODE at position: 781366082500
  Offset from error position: 4601 bytes backward
  Successful forward reads: 50 entries
  Entry Details:
    - Encoder Type: 43 (EncoderTypeBlock)
    - Operation Type: 2
    - Block Height: 12340
    - Flush ID: 550e8400-e29b-41d4-a716-446655440000
    - Entry Size: 1024 bytes

=== DIAGNOSTIC RECOVERY COMPLETE ===
```

## Implementation Details

### Modified Files

1. **consumer/consumer.go**
   - Added `MaxRecoveryLookbackBytes` field to `StateSyncerConsumer` struct
   - Modified `initialize()` to parse `MAX_RECOVERY_LOOKBACK_BYTES` environment variable
   - Modified `readAndDecodeNextEntry()` to accept `isMempool` parameter
   - Added `attemptDiagnosticRecovery()` function
   - Added `readForwardFromPosition()` helper function
   - Updated all calls to `readAndDecodeNextEntry()` to pass `isMempool` parameter

2. **consumer/helpers.go**
   - Added `tryDecodeAtPosition()` helper function
   - Added `tryDecodeAtPositionWithReader()` helper function

### Key Functions

#### `attemptDiagnosticRecovery(errorPos, file, reader, decodeErr)`
- Validates that the error is an encoder type mismatch
- Searches backward byte-by-byte up to `MaxRecoveryLookbackBytes`
- Logs detailed diagnostic information
- Calls `readForwardFromPosition()` if a valid entry is found

#### `tryDecodeAtPosition(file, position)`
- Attempts to decode a StateChangeEntry at a specific file position
- Returns: entry, success status, bytes read, error
- Does not affect the current file position

#### `tryDecodeAtPositionWithReader(file, reader, position)`
- Similar to above but uses an existing reader for sequential reading
- More efficient for forward reading

#### `readForwardFromPosition(file, startPos)`
- Reads forward from a given position
- Decodes and logs up to 50 successive entries
- Stops after 3 consecutive decode errors

## Important Notes

1. **Diagnostic Only**: This mode does NOT:
   - Modify consumer progress
   - Continue normal processing
   - Fix the corruption automatically
   - It only prints diagnostic information then returns the original error

2. **Performance**: 
   - Byte-by-byte searching can be slow for large lookback values
   - Progress indicators are printed every 1000 bytes

3. **Mempool Protection**:
   - Diagnostic mode never activates for mempool entries
   - Only committed entries trigger the diagnostic recovery

4. **Exit Behavior**:
   - After diagnostics complete, the consumer still exits with the original error
   - Manual intervention is required to fix the underlying issue

## Troubleshooting

If diagnostic mode doesn't activate:
- Verify `MAX_RECOVERY_LOOKBACK_BYTES` is set and > 0
- Check that the error is for committed entries (not mempool)
- Confirm the error message contains "encoder type" and "doesn't match"
- Review logs for the initialization message: "Diagnostic recovery mode enabled..."

## Next Steps

After running diagnostic mode and identifying the corruption:
1. Note the offset between error position and recovered position
2. Review the successfully decoded entries to understand the corruption pattern
3. Determine if the issue is:
   - Version mismatch between core and data handler
   - File truncation/corruption
   - Write interruption during node shutdown
4. Consider:
   - Updating to compatible versions
   - Resetting and resyncing from scratch
   - Using the diagnostic output to manually repair the file

