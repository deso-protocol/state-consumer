# Diagnostic Recovery Mode

## Overview

A new diagnostic mode has been implemented to help identify and diagnose file position misalignment issues in the state consumer when encoder type mismatch errors occur during initial sync of committed entries.

## How It Works

When the consumer encounters an "encoder type doesn't match" error during committed entry decoding, it can automatically:

1. **Search backward byte-by-byte** from the error position to find a valid StateChangeEntry
2. **Report diagnostic information** about the found entry and the offset from the error position
3. **Read forward** from the recovered position to show subsequent entries
4. **Provide detailed logs** to help understand the extent of corruption

## Configuration

### Environment Variable

Set the `MAX_RECOVERY_LOOKBACK_BYTES` environment variable to enable diagnostic mode:

```bash
export MAX_RECOVERY_LOOKBACK_BYTES=10000  # Search back up to 10KB
```

- **Value**: Number of bytes to search backward (must be > 0 to enable)
- **Default**: 0 (disabled)
- **Recommended**: Start with 10000 (10KB) and increase if needed

### Activation Conditions

Diagnostic mode ONLY activates when:
- ✅ `MAX_RECOVERY_LOOKBACK_BYTES` > 0
- ✅ Error is for **committed entries** (NOT mempool entries)
- ✅ Error message contains "encoder type" AND "doesn't match"
- ✅ During the initial sync (optional check via `SyncingFromBeginning` flag)

## Example Usage

```bash
# Enable diagnostic recovery mode with 50KB lookback
export MAX_RECOVERY_LOOKBACK_BYTES=50000

# Run your postgres-data-handler
./postgres-data-handler
```

## Output Format

When diagnostic mode activates, you'll see output like:

```
=== DIAGNOSTIC RECOVERY MODE ACTIVATED ===
Error: DecodeFromBytes: encoder type (11918) doesn't match the entry type (43)
Current Position: 1234567 bytes
Max Lookback: 10000 bytes
Entry Type: Committed (NOT Mempool)
Initial Sync Mode: true

Searching backward...
Searched back 1000 bytes...
Searched back 2000 bytes...
Searched back 3000 bytes...

✓ SUCCESSFUL DECODE at position: 1231245
  Offset from error position: -3322 bytes
  Entry Details:
    - Encoder Type: 43 (EncoderTypeBlock)
    - Operation Type: 2
    - Block Height: 12345
    - Flush ID: 550e8400-e29b-41d4-a716-446655440000
    - Entry Size: 1024 bytes

Reading forward from recovered position...
Entry #1 at position 1232269: EncoderType=43, OpType=2, Height=12345, Size=1024 bytes
Entry #2 at position 1233293: EncoderType=16, OpType=2, Height=12346, Size=2048 bytes
Entry #3 at position 1235341: EncoderType=21, OpType=2, Height=12347, Size=512 bytes
...
(stopping after 50 successful entries)

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

