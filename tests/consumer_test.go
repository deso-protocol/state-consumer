package tests

import (
	"fmt"
	"github.com/deso-protocol/backend/routes"
	"github.com/deso-protocol/core/lib"
	pdh_tests "github.com/deso-protocol/postgres-data-handler/tests"
	"github.com/deso-protocol/state-consumer/consumer"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, _, _ := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt().SetUint64(123212312324),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	_, txnRes, err := nodeClient.DAOCoins(mintDaoCoinReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash := txnRes.TxnHashHex

	// Wait for the transaction to be consumed by the state consumer.
	mintCoinTxnRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)

	consumedEvents := mintCoinTxnRes.ConsumedEvents
	flushId := mintCoinTxnRes.FlushId
	transactionConfirmed := false
	flushesBeforeConfirmation := 0
	var balanceEntryRes *EntryScanResult

	// Keep waiting for balance entry operations until the transaction is confirmed.
	for !transactionConfirmed {
		// Wait for the next balance entry operation.
		balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
			precedingEvents:     consumedEvents,
			targetConsumerEvent: &consumerEventBatch,
			targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
			exitWhenEmpty:       false,
		})
		require.NoError(t, err)
		consumedEvents = nil

		// On the first flush, it should have the same flush ID as the transaction.
		// After that, it should have a different flush ID, until it's mined.

		if balanceEntryRes.IsMempool {
			require.Equal(t, balanceEntryRes.IsMempool, true)

			if flushesBeforeConfirmation == 0 {
				require.Equal(t, balanceEntryRes.FlushId, flushId)
			} else {
				require.NotEqual(t, balanceEntryRes.FlushId, flushId)
			}

			flushesBeforeConfirmation++
		} else {
			require.Equal(t, balanceEntryRes.FlushId, flushId)
			transactionConfirmed = true
			require.Equal(t, balanceEntryRes.IsMempool, false)
		}

		flushId = balanceEntryRes.FlushId

		// Clear the preceeding batch events to avoid matching the same batch again.
		require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
		require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
		require.Equal(t, balanceEntryRes.IsReverted, false)
		balanceEntries, balanceAncestralEntries, err := DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
		require.NoError(t, err)
		require.Len(t, balanceEntries, 1)
		require.Len(t, balanceAncestralEntries, 1)
		require.NotNil(t, balanceEntries[0])
		balanceEntry := *balanceEntries[0]
		require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
		require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
		require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
		ancestralBalanceEntry := balanceAncestralEntries[0]
		require.Nil(t, ancestralBalanceEntry)

		if !transactionConfirmed {
			// Wait for the next balance entry operation - it should be a delete operation.
			balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
				targetConsumerEvent: &consumerEventBatch,
				targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
				exitWhenEmpty:       false,
			})
			require.NoError(t, err)
			require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
			require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeDelete)
			require.Equal(t, balanceEntryRes.IsReverted, false)
			balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
			require.NoError(t, err)
			require.Len(t, balanceEntries, 1)
			require.Len(t, balanceAncestralEntries, 1)
			require.NotNil(t, balanceEntries[0])
			balanceEntry = *balanceEntries[0]
			require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
			require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
			require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
			ancestralBalanceEntry = balanceAncestralEntries[0]
			require.Nil(t, ancestralBalanceEntry)
			require.Equal(t, balanceEntryRes.IsMempool, !transactionConfirmed)
			require.Equal(t, balanceEntryRes.FlushId, flushId)
		}

	}

	require.Less(t, flushesBeforeConfirmation, 4, "Flushes before confirmation: %d", flushesBeforeConfirmation)
	require.Greater(t, flushesBeforeConfirmation, 0, "Flushes before confirmation: %d", flushesBeforeConfirmation)
	require.True(t, transactionConfirmed)

	confirmedMintTxnRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:       balanceEntryRes.ConsumedEvents,
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)
	// Make sure that the mint transaction is confirmed, and was confirmed during the same flush.
	require.Equal(t, flushId, confirmedMintTxnRes.FlushId)

	// That should be the last balance entry operation in the queue.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       true,
	})
	require.Error(t, err)

	// Lock coins for the coin user.
	lockCoinsReq := &routes.CoinLockupRequest{
		TransactorPublicKeyBase58Check: coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58Check:    coinUser.PublicKeyBase58,
		RecipientPublicKeyBase58Check:  coinUser.PublicKeyBase58,
		UnlockTimestampNanoSecs:        time.Now().UnixNano() + 1000000000000,
		VestingEndTimestampNanoSecs:    time.Now().UnixNano() + 1000000000000,
		LockupAmountBaseUnits:          uint256.NewInt().SetUint64(1e9),
		ExtraData:                      nil,
		MinFeeRateNanosPerKB:           pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                nil,
	}

	_, txnRes, err = nodeClient.LockCoins(lockCoinsReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash = txnRes.TxnHashHex

	lockCoinTxnRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		targetIsMempool:       &trueValue,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)

	// Wait for the next locked balance entry operation.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:     lockCoinTxnRes.ConsumedEvents,
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeLockedBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeLockedBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.IsReverted, false)
	require.Equal(t, lockCoinTxnRes.FlushId, balanceEntryRes.FlushId)
	lockedBalanceEntries, lockedBalanceAncestralEntries, err := DecodeStateChangeEntries[*lib.LockedBalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, lockedBalanceEntries, 1)
	require.Len(t, lockedBalanceAncestralEntries, 1)
	require.NotNil(t, lockedBalanceEntries[0])
	lockedBalanceEntry := *lockedBalanceEntries[0]
	require.True(t, lockedBalanceEntry.BalanceBaseUnits.Eq(lockCoinsReq.LockupAmountBaseUnits))
	require.Equal(t, lockCoinsReq.TransactorPublicKeyBase58Check, consumer.PublicKeyBytesToBase58Check(lockedBalanceEntry.ProfilePKID[:], desoParams))
	require.Equal(t, lockCoinsReq.ProfilePublicKeyBase58Check, consumer.PublicKeyBytesToBase58Check(lockedBalanceEntry.HODLerPKID[:], desoParams))
	require.Equal(t, lockCoinsReq.UnlockTimestampNanoSecs, lockedBalanceEntry.UnlockTimestampNanoSecs)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(lockedBalanceEntry.HODLerPKID[:], desoParams), lockCoinsReq.RecipientPublicKeyBase58Check)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(lockedBalanceEntry.ProfilePKID[:], desoParams), lockCoinsReq.ProfilePublicKeyBase58Check)
	ancestralLockedBalanceEntry := lockedBalanceAncestralEntries[0]
	require.Nil(t, ancestralLockedBalanceEntry)
}

func TestRemoveTransaction(t *testing.T) {

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, nodeServer, _ := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt().SetUint64(123212312324),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	_, txnRes, err := nodeClient.DAOCoins(mintDaoCoinReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash := txnRes.TxnHashHex

	// Wait for the transaction to be consumed by the state consumer.
	mintCoinTxnRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)

	preceedingBatchEvents := mintCoinTxnRes.ConsumedEvents
	var balanceEntryRes *EntryScanResult

	err = nodeServer.GetMempool().RemoveTransaction(txnRes.Transaction.Hash())
	require.NoError(t, err)
	flushId := mintCoinTxnRes.FlushId

	// Wait for the next balance entry operation.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:     preceedingBatchEvents,
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, flushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsReverted, false)
	balanceEntries, balanceAncestralEntries, err := DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry := *balanceEntries[0]
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
	ancestralBalanceEntry := balanceAncestralEntries[0]
	require.Nil(t, ancestralBalanceEntry)

	// This entry should be the revert of the previous entry.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, flushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsReverted, true)
	balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry = *balanceEntries[0]
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
	ancestralBalanceEntry = balanceAncestralEntries[0]
	require.Nil(t, ancestralBalanceEntry)

	// This entry should be the revert of the revert.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, flushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeDelete)
	require.Equal(t, balanceEntryRes.IsReverted, true)
	balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry = *balanceEntries[0]
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
	ancestralBalanceEntry = balanceAncestralEntries[0]
	require.Nil(t, ancestralBalanceEntry)

	// This entry should be the revert of the original.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, flushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeDelete)
	require.Equal(t, balanceEntryRes.IsReverted, false)
	balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry = *balanceEntries[0]
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
	ancestralBalanceEntry = balanceAncestralEntries[0]
	require.Nil(t, ancestralBalanceEntry)

	// The transaction shouldn't be confirmed.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:       balanceEntryRes.ConsumedEvents,
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		targetIsMempool:       &falseValue,
		targetEncoderTypes:    []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:         true,
	})
	require.Error(t, err)

	// That should be the last balance entry operation in the queue.
	_, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:     balanceEntryRes.ConsumedEvents,
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       true,
	})
	require.Error(t, err)
}

func TestRemoveTransactionWithAncRecord(t *testing.T) {

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, nodeServer, _ := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt().SetUint64(1e9 + 1),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	_, txnRes, err := nodeClient.DAOCoins(mintDaoCoinReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash := txnRes.TxnHashHex

	// Wait for the transaction to be consumed by the state consumer and mined.
	_, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		targetIsMempool:       &falseValue,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)

	_, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		targetIsMempool:     &falseValue,
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)

	_, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		targetIsMempool:     &falseValue,
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)

	// Clear out the rest of the event channel.
	testHandler.ConsumeAllEvents()

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq2 := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt().SetUint64(1234982123),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	//_, _, err = nodeClient.DAOCoins(mintDaoCoinReq2, coinUser.PrivateKey, false, true)
	_, txnRes2, err := nodeClient.DAOCoins(mintDaoCoinReq2, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash2 := txnRes2.TxnHashHex

	// Wait for the transaction to be consumed by the state consumer.
	searchRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash2,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)

	err = nodeServer.GetMempool().RemoveTransaction(txnRes2.Transaction.Hash())
	require.NoError(t, err)

	// We expect to see the balance entry operation coming from the mempool.
	balanceEntryRes, _ := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:     searchRes.ConsumedEvents,
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, searchRes.FlushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsReverted, false)
	balanceEntries, balanceAncestralEntries, err := DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry := *balanceEntries[0]
	totalBalance := uint256.NewInt().Add(&mintDaoCoinReq.CoinsToMintNanos, &mintDaoCoinReq2.CoinsToMintNanos)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq2.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq2.UpdaterPublicKeyBase58Check)
	require.True(t, balanceEntry.BalanceNanos.Eq(totalBalance), "Balance: %s, Total balance: %s", balanceEntry.BalanceNanos.String(), totalBalance.String())
	require.NotNil(t, balanceAncestralEntries[0])
	ancestralBalanceEntry := *balanceAncestralEntries[0]
	require.True(t, ancestralBalanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)

	// We expect the next entry to be a revert of the previous entry.
	balanceEntryRes, _ = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, searchRes.FlushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsReverted, true)
	balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry = *balanceEntries[0]
	totalBalance = uint256.NewInt().Add(&mintDaoCoinReq.CoinsToMintNanos, &mintDaoCoinReq2.CoinsToMintNanos)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq2.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq2.UpdaterPublicKeyBase58Check)
	require.True(t, balanceEntry.BalanceNanos.Eq(totalBalance), "Balance: %s, Total balance: %s", balanceEntry.BalanceNanos.String(), totalBalance.String())
	require.NotNil(t, balanceAncestralEntries[0])
	ancestralBalanceEntry = *balanceAncestralEntries[0]
	require.True(t, ancestralBalanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)

	// We expect the next entry to be a revert of the previous entry.
	balanceEntryRes, _ = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, searchRes.FlushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsReverted, true)
	balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry = *balanceEntries[0]
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq2.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq2.UpdaterPublicKeyBase58Check)
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos), "Balance: %s, Total balance: %s", balanceEntry.BalanceNanos.String(), mintDaoCoinReq.CoinsToMintNanos.String())
	require.NotNil(t, balanceAncestralEntries[0])
	ancestralBalanceEntry = *balanceAncestralEntries[0]
	require.True(t, ancestralBalanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)

	// This should be a revert of the revert.
	balanceEntryRes, _ = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent: &consumerEventBatch,
		targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBalanceEntry},
		exitWhenEmpty:       false,
	})
	require.NoError(t, err)
	require.Equal(t, balanceEntryRes.IsMempool, true)
	require.Equal(t, balanceEntryRes.FlushId, searchRes.FlushId)

	require.Equal(t, balanceEntryRes.EncoderType, lib.EncoderTypeBalanceEntry)
	require.Equal(t, balanceEntryRes.OperationType, lib.DbOperationTypeUpsert)
	require.Equal(t, balanceEntryRes.IsReverted, false)
	balanceEntries, balanceAncestralEntries, err = DecodeStateChangeEntries[*lib.BalanceEntry](balanceEntryRes.EntryBatch)
	require.NoError(t, err)
	require.Len(t, balanceEntries, 1)
	require.Len(t, balanceAncestralEntries, 1)
	require.NotNil(t, balanceEntries[0])
	balanceEntry = *balanceEntries[0]
	totalBalance = uint256.NewInt().Add(&mintDaoCoinReq.CoinsToMintNanos, &mintDaoCoinReq2.CoinsToMintNanos)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq2.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq2.UpdaterPublicKeyBase58Check)
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos), "Balance: %s, Total balance: %s", balanceEntry.BalanceNanos.String(), mintDaoCoinReq.CoinsToMintNanos.String())
	require.NotNil(t, balanceAncestralEntries[0])
	ancestralBalanceEntry = *balanceAncestralEntries[0]
	require.True(t, ancestralBalanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)

	// The transaction shouldn't be confirmed.
	balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:       balanceEntryRes.ConsumedEvents,
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash2,
		targetIsMempool:       &falseValue,
		exitWhenEmpty:         true,
	})
	require.Error(t, err)

	// Exit the test and clean up.
	//CleanupConsumerTestEnvironment(apiServer, nodeServer, cancel)

	// TODO: Move the full test environment into the consumer repo.
	// TODO: Make sure that state change dir and consumer progress dir gets cleaned up.
	// TODO: Test unconfirmed block, transaction, and block node logic.
	// TODO: Figure out how to get this working in CI/CD.
}

func TestBlockEvents(t *testing.T) {
	// Skip this until we figure out how to fix uncommitted txns.
	t.Skip()

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, _, _ := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt().SetUint64(123212312324),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	_, _, err := nodeClient.DAOCoins(mintDaoCoinReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	var balanceEntryRes *EntryScanResult

	for ii := 0; ii < 200; ii++ {
		balanceEntryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
			targetConsumerEvent: &consumerEventBatch,
			targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBlock, lib.EncoderTypeBlockNode, lib.EncoderTypeTxn},
			exitWhenEmpty:       false,
		})

		if balanceEntryRes.EncoderType == lib.EncoderTypeBlock {
			block, _, err := DecodeStateChangeEntries[*lib.MsgDeSoBlock](balanceEntryRes.EntryBatch)
			require.NoError(t, err)
			fmt.Printf("Block event: |%2d|%t|%t|%s\n", balanceEntryRes.OperationType, balanceEntryRes.IsMempool, balanceEntryRes.IsReverted, balanceEntryRes.FlushId.String())
			fmt.Printf("Block entry: %+v\n", *block[0])
			//fmt.Printf("Block entry key bytes: %+v\n", balanceEntryRes.EntryBatch[0].KeyBytes)
			//fmt.Printf("Block entry encoder bytes: %+v\n", balanceEntryRes.EntryBatch[0].EncoderBytes)
		}
		if balanceEntryRes.EncoderType == lib.EncoderTypeBlockNode {
			blockNode, _, err := DecodeStateChangeEntries[*lib.BlockNode](balanceEntryRes.EntryBatch)
			require.NoError(t, err)
			fmt.Printf("Node event: |%2d|%t|%t|%s\n", balanceEntryRes.OperationType, balanceEntryRes.IsMempool, balanceEntryRes.IsReverted, balanceEntryRes.FlushId.String())
			fmt.Printf("Node entry: %+v\n", *blockNode[0])
			//fmt.Printf("Node entry key bytes: %+v\n", balanceEntryRes.EntryBatch[0].KeyBytes)
			//fmt.Printf("Node entry encoder bytes: %+v\n", balanceEntryRes.EntryBatch[0].EncoderBytes)
		}
		if balanceEntryRes.EncoderType == lib.EncoderTypeTxn {
			blockNode, _, err := DecodeStateChangeEntries[*lib.MsgDeSoTxn](balanceEntryRes.EntryBatch)
			require.NoError(t, err)
			fmt.Printf("Txn event: |%2d|%t|%t|%s\n", balanceEntryRes.OperationType, balanceEntryRes.IsMempool, balanceEntryRes.IsReverted, balanceEntryRes.FlushId.String())
			fmt.Printf("Txn entry: %+v\n", *blockNode[0])
		}
	}
}
