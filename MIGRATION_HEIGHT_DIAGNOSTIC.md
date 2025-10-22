# Migration Height Diagnostic Mode

## Overview

The **Migration Height Diagnostic Mode** is a specialized debugging tool that helps diagnose "encoder type doesn't match" errors by testing different DeSo migration block heights during entry decoding. This tool operates independently from the byte-position diagnostic and provides insights into version/migration mismatches between the state-changes file and the consumer.

## When to Use

Use this diagnostic when you encounter errors like:
```
DecodeFromBytes: encoder type (121) doesn't match the entry type (18)
```

This typically indicates:
- State-changes file was written with a different core version
- Migration boundary corruption
- Encoder schema version mismatch

## How It Works

When an "encoder type doesn't match" error occurs during committed entry decoding, the diagnostic:

1. **Tests All Known Migrations**: Attempts to decode the entry using each known migration height:
   - DefaultMigration (Height 0)
   - UnlimitedDerivedKeysMigration (Height 467,217)
   - AssociationsAndAccessGroupsMigration (Height 596,555)
   - BalanceModelMigration (Height 683,058)
   - ProofOfStake1StateSetupMigration (Height 1,113,866)

2. **Reports Success/Failure**: For each migration height, shows whether decoding succeeded and provides entry details

3. **Provides Analysis**: Interprets the results to suggest likely root causes and solutions

## Configuration

### Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ENABLE_MIGRATION_HEIGHT_DIAGNOSTIC` | bool | `false` | Enable migration height diagnostic mode |

### Example Usage

```bash
# Enable migration height diagnostic
export ENABLE_MIGRATION_HEIGHT_DIAGNOSTIC=true

# Run your consumer
./postgres-data-handler
```

## Output Format

When activated, you'll see output like:

```
========================================
MIGRATION HEIGHT DIAGNOSTIC MODE
========================================
Error: DecodeFromBytes: encoder type (121) doesn't match the entry type (18)
Current Block Height: 24242496
Buffer Size: 1847 bytes
Testing all migration heights...

✗ Height 0 (DefaultMigration, Version 0) - FAILED
✗ Height 467217 (UnlimitedDerivedKeysMigration, Version 1) - FAILED  
✗ Height 596555 (AssociationsAndAccessGroupsMigration, Version 2) - FAILED
✗ Height 683058 (BalanceModelMigration, Version 3) - FAILED
✓ SUCCESS - Height 1113866 (ProofOfStake1StateSetupMigration, Version 4)
  Entry Details:
    - EncoderType: 21 (EncoderTypeProfileEntry)
    - OperationType: 2
    - BlockHeight: 1113900
    - FlushID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
    - IsReverted: false
    - Encoder: *lib.ProfileEntry

========================================
DIAGNOSTIC SUMMARY
========================================
Successful Heights: 1/5

✓ Successful Migration Heights:
  - 1113866 (ProofOfStake1StateSetupMigration, Version 4)

📊 ANALYSIS:
  → Exactly ONE height works: 1113866 (ProofOfStake1StateSetupMigration)
  → This suggests the state-changes file was written with Height 1113866
  → But your consumer is using Height 24242496 for decoding
  → SOLUTION: Ensure your core version matches the node that wrote the file

========================================
```

## Interpreting Results

### Scenario 1: Exactly ONE height works

**Meaning**: The entry was written at a specific migration boundary with a particular schema version.

**Likely Cause**: 
- State-changes file written at migration boundary
- Corruption occurred during migration transition
- Version mismatch between writer and reader

**Solution**:
- Verify core versions match between node and consumer
- Check if corruption occurred at migration block height
- Consider resyncing from a clean state

### Scenario 2: Multiple heights work

**Meaning**: The entry doesn't contain migration-dependent fields.

**Likely Cause**:
- Entry type not affected by any migrations
- All tested migrations produce same result for this encoder type

**Solution**:
- Issue is likely NOT migration-related
- Use byte-position diagnostic for corruption analysis
- Check for other types of file corruption

### Scenario 3: NO heights work

**Meaning**: Entry cannot be decoded with any known migration height.

**Likely Cause**:
- Severe file corruption at this position
- Unknown/future migration height
- Completely incompatible core version
- Wrong network (mainnet vs testnet)

**Solution**:
- Verify you're using correct network parameters
- Check core version compatibility
- Likely requires full resync
- Use byte-position diagnostic to analyze file structure

## Technical Details

### Migration Heights by Network

**Mainnet**:
- DefaultMigration: 0
- UnlimitedDerivedKeysMigration: 467,217
- AssociationsAndAccessGroupsMigration: 596,555
- BalanceModelMigration: 683,058
- ProofOfStake1StateSetupMigration: 1,113,866

**Testnet**:
- DefaultMigration: 0
- UnlimitedDerivedKeysMigration: [testnet height]
- AssociationsAndAccessGroupsMigration: [testnet height]
- BalanceModelMigration: [testnet height]
- ProofOfStake1StateSetupMigration: 349,167

### How Migration Heights Affect Decoding

DeSo encoders use conditional encoding based on block height:

```go
if MigrationTriggered(blockHeight, ProofOfStake1StateSetupMigration) {
    data = append(data, byte(ce.LockupTransferRestrictionStatus))
}
```

When the migration height changes:
- Additional fields may be encoded/decoded
- Struct layout changes
- Version bytes change
- Decoder behavior differs

### Integration with Byte-Position Diagnostic

The migration height diagnostic runs **BEFORE** the byte-position diagnostic:

1. **Migration Height Test**: Quick check if wrong migration height is the issue
2. **Byte-Position Search**: If migration heights don't help, search for file corruption

Both diagnostics are independent and provide different insights:
- **Migration**: Tests different schema versions
- **Byte-Position**: Tests different file positions

## Limitations

1. **Only Tests Known Migrations**: Cannot detect future or unknown migrations
2. **Network-Specific**: Results depend on correct network configuration
3. **Non-Intrusive**: Diagnostic mode doesn't fix issues, only reports them
4. **Committed Entries Only**: Does not run for mempool entries

## Best Practices

1. **Enable During Investigation**: Only enable when debugging specific errors
2. **Combine with Byte Diagnostic**: Use both diagnostics together for comprehensive analysis
3. **Check Core Versions**: Always verify core version compatibility first
4. **Document Results**: Save diagnostic output for troubleshooting
5. **Disable After Diagnosis**: Turn off once issue is identified

## Files Modified

- `consumer/consumer.go`: Added configuration field, env parsing, integration
- `consumer/migration_heights.go`: Migration height manager
- `consumer/migration_diagnostic.go`: Diagnostic implementation

## See Also

- [Diagnostic Recovery Mode](./DIAGNOSTIC_RECOVERY_MODE.md) - Byte-position diagnostic
- [Diagnostic Quick Start](./DIAGNOSTIC_QUICKSTART.md) - Quick setup guide
- [DeSo Core Migrations](https://github.com/deso-protocol/core/blob/main/lib/constants.go) - Migration definitions

