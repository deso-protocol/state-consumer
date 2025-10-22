# Quick Start: Diagnostic Modes

## Problem
Your postgres-data-handler is crash looping with:
```
encoder type (11918) doesn't match the entry type (43)
```

## Two Diagnostic Tools Available

### 🔬 Migration Height Diagnostic (Try This First!)
Tests if the issue is caused by migration/version mismatches. **Fast and non-intrusive.**

### 🔍 Byte-Position Diagnostic
Searches for file corruption by testing different byte positions. **Slower but thorough.**

---

## Option 1: Migration Height Diagnostic (RECOMMENDED FIRST)

### Step 1: Enable Migration Height Diagnostic
```yaml
environment:
  - ENABLE_MIGRATION_HEIGHT_DIAGNOSTIC=true
```

Or:
```bash
export ENABLE_MIGRATION_HEIGHT_DIAGNOSTIC=true
./postgres-data-handler
```

### Step 2: Review Output
```
========================================
MIGRATION HEIGHT DIAGNOSTIC MODE
========================================
Error: encoder type (121) doesn't match entry type (18)
Current Block Height: 24242496

✓ SUCCESS - Height 1113866 (ProofOfStake1StateSetupMigration, Version 4)
  Entry Details:
    - EncoderType: 21 (EncoderTypeProfileEntry)
    - BlockHeight: 1113900

📊 ANALYSIS:
  → Exactly ONE height works: 1113866
  → State-changes file was written with different migration height
  → SOLUTION: Ensure your core version matches the node
========================================
```

### Step 3: Interpret Results
- **One height works**: Version mismatch → Sync core versions or resync
- **Multiple heights work**: Not migration-related → Try byte-position diagnostic
- **No heights work**: Severe corruption → Full resync needed

---

## Option 2: Byte-Position Diagnostic

Use this if migration height diagnostic doesn't help.

### Step 1: Enable Byte-Position Diagnostic
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

## Configuration Summary

### Migration Height Diagnostic
| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_MIGRATION_HEIGHT_DIAGNOSTIC` | `false` | Enable migration testing (fast) |

### Byte-Position Diagnostic

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

### If Migration Height Diagnostic Found a Match:
1. **Verify Versions**: Ensure core library version matches between node and consumer
2. **Check Migration Timing**: Entry may be at migration boundary
3. **Consider Resync**: Safest if you're uncertain about versions

### If Byte-Position Diagnostic Found Corruption:
1. **Version Check**: Ensure core and data handler versions match
2. **Root Cause**: Check disk errors, node crashes, version mismatches  
3. **Full Resync**: Delete state files and start fresh (safest option)

### If Both Diagnostics Fail:
1. **Network Check**: Verify mainnet vs testnet configuration
2. **Core Version**: Check for incompatible core library version
3. **Full Resync Required**: Likely your only option

## Important Notes

- ⚠️ **Migration diagnostic is fast** - Always try this first!
- ⚠️ **Byte diagnostic is slow** - Use only when migration diagnostic doesn't help
- ⚠️ Diagnostic modes do NOT fix corruption - they only find it
- ⚠️ The handler will exit after diagnostics complete
- ⚠️ Only works for committed entry errors (not mempool)
- ⚠️ Both diagnostics can run simultaneously

## Documentation

- [MIGRATION_HEIGHT_DIAGNOSTIC.md](./MIGRATION_HEIGHT_DIAGNOSTIC.md) - Migration height diagnostic details
- [DIAGNOSTIC_RECOVERY_MODE.md](./DIAGNOSTIC_RECOVERY_MODE.md) - Byte-position diagnostic details
