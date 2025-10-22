# Combined Migration Height + Byte-Position Diagnostic

## 🎯 Major Enhancement: Comprehensive Diagnostic Approach

The diagnostic system now combines **migration height testing** with **byte-position searching** to provide the most thorough corruption analysis possible.

## How It Works

### Phase 1: Migration Height Test (at current position)
When an "encoder type doesn't match" error occurs, the system first tests if **different migration heights** can decode the entry at the current position.

### Phase 2: Combined Byte + Migration Search
If Phase 1 fails, the byte-position diagnostic searches byte-by-byte BUT **tests all migration heights at each position**.

## Key Innovation

**Old Approach:**
- Test each byte position with current migration height only
- Miss entries that need different migration heights

**New Approach:**
- Test each byte position with ALL 5 migration heights
- Find entries even if they were written with different core version
- Provides which migration height works for each candidate

## Configuration

```bash
# Enable both diagnostics
export ENABLE_MIGRATION_HEIGHT_DIAGNOSTIC=true
export MAX_RECOVERY_LOOKBACK_BYTES=100000
export RECOVERY_SEARCH_DIRECTION=both
export MIN_SUCCESSFUL_FORWARD_READS=3
```

## Example Output

```
========================================
MIGRATION HEIGHT DIAGNOSTIC MODE
========================================
Error: encoder type (11918) doesn't match entry type (43)
Testing all migration heights...
✗ NO successful heights found at current position

=== DIAGNOSTIC RECOVERY MODE ACTIVATED ===
Searching backward...
Testing 5 migration heights at each position

Searched back 1000 bytes... (checked 0 candidates)
Searched back 2000 bytes... (checked 0 candidates)
Searched back 3000 bytes... (checked 1 candidates)

Candidate found at position 781363084099 (-3002 bytes) using migration height 1113866 (ProofOfStake1StateSetupMigration)
Verifying by reading forward...
Will only count entries at positions >= 781363087101 (error position)
Entry #1 at position 781363084099: EncoderType=21, OpType=2, Height=1113900
Entry #5 at position 781363087212: EncoderType=18, OpType=2, Height=1113901 [PAST ERROR POSITION]
Entry #8 at position 781363089445: EncoderType=21, OpType=2, Height=1113902 [PAST ERROR POSITION]
...
✓ Candidate validated with 15 successful forward reads past error position
✓ Working migration height: 1113866 (ProofOfStake1StateSetupMigration)

✓ SUCCESSFUL DECODE at position: 781363084099
  Offset from error position: 3002 bytes backward
  Successful forward reads: 15 entries
  Working migration height: 1113866 (ProofOfStake1StateSetupMigration)
  Entry Details:
    - Encoder Type: 21 (EncoderTypeProfileEntry)
    - Block Height: 1113900
========================================
```

## What This Tells You

1. **Position**: File corruption/misalignment is 3002 bytes before error
2. **Migration Height**: Data was written at height 1113866 (ProofOfStake1StateSetupMigration boundary)
3. **Recovery**: Successfully read 15 entries forward, indicating data is recoverable
4. **Root Cause**: State-changes file was written during/after migration, but with different schema version

## Why This is Powerful

### Scenario: Migration Boundary Corruption

Your node crashed at block 1,113,866 (ProofOfStake1StateSetupMigration). When it restarted:
- File pointer got misaligned by a few thousand bytes
- Data is still there but at wrong offset
- Data uses migration height 1,113,866 schema
- Current reader uses height 24,242,486 schema

**Old diagnostic:** Would find byte position but fail to decode
**New diagnostic:** Finds byte position AND correct migration height!

## Technical Implementation

### tryDecodeAtPositionWithAllHeights

```go
func tryDecodeAtPositionWithAllHeights(file *os.File, position int64, migrationHeights []MigrationHeightInfo) 
    (*lib.StateChangeEntry, bool, uint64, uint64, error)
```

For each migration height:
1. Seek to position
2. Read varint size
3. Read entry bytes
4. Attempt decode with migration height
5. Return first successful decode with height info

### Enhanced Search Functions

Both `searchBackwardForValidEntry` and `searchForwardForValidEntry` now:
1. Get all migration heights for network
2. At each byte position, test ALL heights
3. Report which height worked
4. Validate with forward reads
5. Return entry + working migration height

## Migration Heights Tested

**Mainnet:**
- 0 (DefaultMigration)
- 467,217 (UnlimitedDerivedKeysMigration)
- 596,555 (AssociationsAndAccessGroupsMigration)
- 683,058 (BalanceModelMigration)
- 1,113,866 (ProofOfStake1StateSetupMigration)

**5 heights × thousands of positions = extremely thorough**

## Performance Considerations

**Overhead per position:** ~5x (testing 5 heights instead of 1)
**Still reasonable:** Modern systems test ~1000 positions/second
**Worth it:** Finds corruption that would otherwise require full resync

## When to Use

### Use combined diagnostic when:
- ✅ "encoder type doesn't match" errors
- ✅ Suspect migration boundary issues
- ✅ Node crashed during state-changes write
- ✅ Core version may have changed

### Results interpretation:
- **Found with height X**: File written at migration X, need matching core version
- **Found with current height**: Byte misalignment only, not migration issue
- **Not found**: Severe corruption, need full resync

## See Also

- [MIGRATION_HEIGHT_DIAGNOSTIC.md](./MIGRATION_HEIGHT_DIAGNOSTIC.md) - Phase 1 details
- [DIAGNOSTIC_RECOVERY_MODE.md](./DIAGNOSTIC_RECOVERY_MODE.md) - Phase 2 details
- [DIAGNOSTIC_QUICKSTART.md](./DIAGNOSTIC_QUICKSTART.md) - Quick setup guide

