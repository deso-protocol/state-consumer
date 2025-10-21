# Quick Start: Diagnostic Recovery Mode

## Problem
Your postgres-data-handler is crash looping with:
```
encoder type (11918) doesn't match the entry type (43)
```

## Solution: Use Diagnostic Mode

### Step 1: Enable Diagnostic Mode
Add environment variables to your docker-compose or kubernetes config:

**Basic (Backward Search):**
```yaml
environment:
  - MAX_RECOVERY_LOOKBACK_BYTES=50000  # Search 50KB
```

**Advanced (Both Directions):**
```yaml
environment:
  - MAX_RECOVERY_LOOKBACK_BYTES=100000
  - RECOVERY_SEARCH_DIRECTION=both  # Try backward, then forward
  - MIN_SUCCESSFUL_FORWARD_READS=5  # Require 5 valid forward reads
```

Or export them before running:
```bash
export MAX_RECOVERY_LOOKBACK_BYTES=50000
export RECOVERY_SEARCH_DIRECTION=both
export MIN_SUCCESSFUL_FORWARD_READS=5
```

### Step 2: Run Your Handler
```bash
./postgres-data-handler
```

### Step 3: Review Output
Look for the diagnostic output in your logs:

**Successful Recovery:**
```
=== DIAGNOSTIC RECOVERY MODE ACTIVATED ===
Search Direction: both
Minimum Successful Forward Reads: 5

Searching backward...
Candidate found at position 1231245 (-3322 bytes), verifying...
✓ Candidate validated with 10 successful forward reads

✓ SUCCESSFUL DECODE at position: 1231245
  Offset from error position: 3322 bytes backward
  Successful forward reads: 10 entries
  Entry Details:
    - Encoder Type: 43 (EncoderTypeBlock)
    - Block Height: 12345
```

**Failed Validation:**
```
Candidate found at position 1233000 (-1567 bytes), verifying...
Entry #1 at position 1233000: EncoderType=16, OpType=2, Height=12344
Entry #2 at position 1235048: EncoderType=21, OpType=2, Height=12345
✗ Candidate rejected: only 2 successful forward reads (need 5), continuing search...
```

### Step 4: Analyze Results

#### If successful decode found:
- Note the **offset** and **direction** (backward/forward)
- Review the **successful forward reads count** (higher is better)
- Check the **block height** of the recovered entry
- File position misalignment confirmed at that location

#### If candidates rejected:
- Lower `MIN_SUCCESSFUL_FORWARD_READS` (try 2 or 3)
- Increase `MAX_RECOVERY_LOOKBACK_BYTES`
- Try different `RECOVERY_SEARCH_DIRECTION`

#### If no valid entry found:
- Significantly increase `MAX_RECOVERY_LOOKBACK_BYTES`
- Or the file may be more severely corrupted
- Consider full resync

## Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_RECOVERY_LOOKBACK_BYTES` | 0 (off) | Distance to search. **Required** to enable. |
| `RECOVERY_SEARCH_DIRECTION` | `backward` | `backward`, `forward`, or `both` |
| `MIN_SUCCESSFUL_FORWARD_READS` | 3 | Validation threshold (lower = less strict) |

## Common Search Distances

- `10000` (10KB) - Quick check for minor misalignment
- `50000` (50KB) - Standard diagnostic check
- `100000` (100KB) - Deep search for corruption
- `1000000` (1MB) - Very thorough but slow

## Search Strategies

### Strategy 1: Quick Backward Check
```bash
MAX_RECOVERY_LOOKBACK_BYTES=50000
RECOVERY_SEARCH_DIRECTION=backward
```
Best for: Recent corruption near error position

### Strategy 2: Comprehensive Search
```bash
MAX_RECOVERY_LOOKBACK_BYTES=100000
RECOVERY_SEARCH_DIRECTION=both
MIN_SUCCESSFUL_FORWARD_READS=5
```
Best for: Unknown corruption location, need high confidence

### Strategy 3: Lenient Validation
```bash
MAX_RECOVERY_LOOKBACK_BYTES=100000
RECOVERY_SEARCH_DIRECTION=both
MIN_SUCCESSFUL_FORWARD_READS=2
```
Best for: Many candidates being rejected

### Strategy 4: Forward Only
```bash
MAX_RECOVERY_LOOKBACK_BYTES=50000
RECOVERY_SEARCH_DIRECTION=forward
```
Best for: Corruption before error position

## What's Next?

After diagnostics, you'll need to:
1. **Version check**: Ensure core and data handler versions match
2. **Root cause**: Check disk errors, node crashes, version mismatches
3. **Full resync**: Delete state files and start fresh (safest option)

## Important Notes

- ⚠️ Diagnostic mode does NOT fix corruption - it only finds it
- ⚠️ The handler will exit after diagnostics complete
- ⚠️ Only works for committed entry errors (not mempool)
- ⚠️ Byte-by-byte search is slow - use only when debugging

See [DIAGNOSTIC_RECOVERY_MODE.md](./DIAGNOSTIC_RECOVERY_MODE.md) for complete documentation.
