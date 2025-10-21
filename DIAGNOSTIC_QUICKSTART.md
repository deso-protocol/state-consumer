# Quick Start: Diagnostic Recovery Mode

## Problem
Your postgres-data-handler is crash looping with:
```
encoder type (11918) doesn't match the entry type (43)
```

## Solution: Use Diagnostic Mode

### Step 1: Enable Diagnostic Mode
Add the environment variable to your docker-compose or kubernetes config:

```yaml
environment:
  - MAX_RECOVERY_LOOKBACK_BYTES=50000  # Search back 50KB
```

Or export it before running:
```bash
export MAX_RECOVERY_LOOKBACK_BYTES=50000
```

### Step 2: Run Your Handler
```bash
./postgres-data-handler
```

### Step 3: Review Output
Look for the diagnostic output in your logs:

```
=== DIAGNOSTIC RECOVERY MODE ACTIVATED ===
Error: DecodeFromBytes: encoder type (11918) doesn't match the entry type (43)
Current Position: 1234567 bytes
...
✓ SUCCESSFUL DECODE at position: 1231245
  Offset from error position: -3322 bytes
  Entry Details:
    - Encoder Type: 43 (EncoderTypeBlock)
    - Block Height: 12345
    ...
```

### Step 4: Analyze Results

#### If successful decode found:
- Note the **offset** (how many bytes backward)
- Review the **block height** of the recovered entry
- Check if subsequent entries decode successfully
- This indicates file position misalignment

#### If no valid entry found:
- Increase `MAX_RECOVERY_LOOKBACK_BYTES`
- Or the file may be more severely corrupted
- Consider full resync

## Common Values for MAX_RECOVERY_LOOKBACK_BYTES

- `10000` (10KB) - Quick check for minor misalignment
- `50000` (50KB) - Standard diagnostic check
- `100000` (100KB) - Deep search for corruption
- `1000000` (1MB) - Very thorough but slow

## What's Next?

After diagnostics, you'll need to:
1. **Version check**: Ensure core and data handler versions match
2. **Manual recovery**: Use the found position to update consumer progress
3. **Full resync**: Delete state files and start fresh (safest option)

See [DIAGNOSTIC_RECOVERY_MODE.md](./DIAGNOSTIC_RECOVERY_MODE.md) for complete documentation.

