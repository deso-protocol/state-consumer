package tests

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/deso-protocol/backend/routes"
	"github.com/deso-protocol/core/lib"
	pdh_tests "github.com/deso-protocol/postgres-data-handler/tests"
	"github.com/deso-protocol/state-consumer/consumer"
	"github.com/deso-protocol/uint256"
	"github.com/stretchr/testify/require"
)

// VerifyTransactionStateChanges is a helper function that verifies the state changes for a transaction.
// It waits for the transaction to be consumed by the state consumer, and then verifies that the
// state changes for the entries related to the transaction are applied and reverted correctly.
// TODO: When throwing out consumed events, add them to a slice. This should be returned from the function.
func VerifyTransactionStateChanges(
	t *testing.T,
	testHandler *TestHandler,
	txnHash string,
	encoderType lib.EncoderType,
	targetBadgerKeyBytes *[]byte,
	previousConsumedEvents []*StateConsumerEvent,
	validateAppliedEntry func(*EntryScanResult, *[]byte) error,
	validateRevertedEntry func(*EntryScanResult, *[]byte) error,
) {

	// We should first see a transaction event for the transaction being initiated.
	// Wait for the transaction to be consumed by the state consumer.
	txnRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:       previousConsumedEvents,
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)
	require.Equal(t, true, txnRes.IsMempool)
	// require.Equal(t, TransactionEventInitiate, txnRes.LastTransactionEvent)
	// TODO: Somehow pass the previous transaction state to the testHandler, so that if no transaction events occur, we stil know what the active transaction state is.
	// if txnRes.TransactionInitiates > 0 {
	// 	require.Equal(t, TransactionEventInitiate, txnRes.LastTransactionEvent)
	// } else {
	// 	require.Equal(t, TransactionEventUndefined, txnRes.LastTransactionEvent)
	// }
	requiredTxnInitiateCount := txnRes.TransactionInitiates
	requiredTxnCommitCount := txnRes.TransactionCommits
	// Add the events consumed from the transaction search to the previous consumed events.
	consumedEvents := append(txnRes.ConsumedEvents, txnRes.RemainingConsumedEvents...)
	fmt.Printf("Consumed event length: %d\n", len(consumedEvents))
	fmt.Printf("All preceding events length: %d\n", len(previousConsumedEvents))
	flushId := txnRes.FlushId
	transactionConfirmed := false
	flushesBeforeConfirmation := 0
	var entryRes *EntryScanResult

	// Keep waiting for balance entry operations until the transaction is confirmed.
	for !transactionConfirmed {
		fmt.Printf("Transaction not confirmed yet.\n")
		// Should somehow confirm that the transaction and the balance entry operation occur in the same initiate/commit event.
		// Wait for the next balance entry operation.
		entryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
			precedingEvents:      consumedEvents,
			targetConsumerEvent:  &consumerEventBatch,
			targetEncoderTypes:   []lib.EncoderType{encoderType},
			targetBadgerKeyBytes: targetBadgerKeyBytes,
			exitWhenEmpty:        false,
		})

		// fmt.Printf("Entry result: %+v\n", entryRes)

		// fmt.Printf("Target badger key bytes: %s\n", hex.EncodeToString(*targetBadgerKeyBytes))

		// for _, event := range entryRes.EntryBatch {
		// 	fmt.Printf("Event key: %s\n", hex.EncodeToString(event.KeyBytes))
		// }

		require.NoError(t, err)

		for _, entry := range entryRes.EntryBatch {
			if bytes.Equal(entry.KeyBytes, *targetBadgerKeyBytes) {
				// Decode the entry.
				postAssocEntries, _, err := DecodeStateChangeEntries[*lib.PostAssociationEntry]([]*lib.StateChangeEntry{entry})
				require.NoError(t, err)
				require.Greater(t, len(postAssocEntries), 0)
				fmt.Printf("Entry: %+v\n", &postAssocEntries[0])
			}
		}

		// TODO: Delete this.
		err = validateAppliedEntry(entryRes, targetBadgerKeyBytes)
		require.NoError(t, err)

		require.Equal(t, requiredTxnInitiateCount, entryRes.TransactionInitiates)
		require.Equal(t, requiredTxnCommitCount, entryRes.TransactionCommits)
		require.Equal(t, TransactionEventInitiate, txnRes.LastTransactionEvent)
		// Clear the batch events that were consumed to avoid matching the same batch again.
		// Only include the remaining events that weren't consumed.
		consumedEvents = entryRes.RemainingConsumedEvents

		// TODO: It seems like the mempool entries disappearing is being caused by an entry being removed from the mempool, but not being included in committed state for another two blocks.
		// We need to find a way to either ensure that mempool state includes these entries, or not revert them for another two blocks.

		if entryRes.IsMempool {
			require.Equal(t, entryRes.IsMempool, true)

			// On the first flush, it should have the same flush ID as the transaction.
			// After that, it should have a different flush ID, until it's mined.
			if flushesBeforeConfirmation == 0 {
				require.Equal(t, entryRes.FlushId, flushId)
			} else {
				require.NotEqual(t, entryRes.FlushId, flushId)
			}

			flushesBeforeConfirmation++
		} else {
			// Once the transaction is no longer in the mempool, it should be confirmed.
			require.Equal(t, entryRes.FlushId, flushId)
			transactionConfirmed = true
			require.Equal(t, entryRes.IsMempool, false)
		}

		flushId = entryRes.FlushId

		require.Equal(t, entryRes.EncoderType, encoderType)
		require.Equal(t, entryRes.OperationType, lib.DbOperationTypeUpsert)
		require.Equal(t, entryRes.IsReverted, false)

		err = validateAppliedEntry(entryRes, targetBadgerKeyBytes)
		require.NoError(t, err)

		if !transactionConfirmed {
			// Wait for the next balance entry operation - it should be a delete operation.
			entryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
				precedingEvents:      consumedEvents,
				targetConsumerEvent:  &consumerEventBatch,
				targetEncoderTypes:   []lib.EncoderType{encoderType},
				targetBadgerKeyBytes: targetBadgerKeyBytes,
				exitWhenEmpty:        false,
			})
			require.NoError(t, err)
			require.Equal(t, TransactionEventInitiate, entryRes.LastTransactionEvent)
			// After a revert, the next mempool entry re-apply should occur within the same transaction.
			requiredTxnInitiateCount = 0
			requiredTxnCommitCount = 0
			require.Equal(t, encoderType, entryRes.EncoderType)
			// TODO: Handle the revert case.
			require.Equal(t, false, entryRes.IsReverted)
			require.Equal(t, !transactionConfirmed, entryRes.IsMempool)

			fmt.Printf("Target badger key bytes: %s\n", hex.EncodeToString(*targetBadgerKeyBytes))

			for _, event := range entryRes.EntryBatch {
				fmt.Printf("Event key: %s\n", hex.EncodeToString(event.KeyBytes))
			}

			// Validate the reverted entry.
			err = validateRevertedEntry(entryRes, targetBadgerKeyBytes)
			require.NoError(t, err)
			consumedEvents = entryRes.RemainingConsumedEvents
		}

	}

	require.Less(t, flushesBeforeConfirmation, 4, "Flushes before confirmation should be less than 4, was %d", flushesBeforeConfirmation)
	require.Greater(t, flushesBeforeConfirmation, 0, "Flushes before confirmation should be greater than 0, was %d", flushesBeforeConfirmation)
	require.True(t, transactionConfirmed)

	// Search for the associated transaction associated with the committed entry.
	confirmedTxnRes, err := testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:       append(entryRes.ConsumedEvents, entryRes.RemainingConsumedEvents...),
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash,
		exitWhenEmpty:         false,
	})
	require.NoError(t, err)
	// Make sure that the mint transaction is confirmed, and was confirmed during the same flush as the balance entry.
	require.Equal(t, flushId, confirmedTxnRes.FlushId)
	require.Equal(t, confirmedTxnRes.IsMempool, false)
	require.Equal(t, confirmedTxnRes.TransactionInitiates, requiredTxnInitiateCount)
	require.Equal(t, confirmedTxnRes.TransactionCommits, requiredTxnCommitCount)
	require.Equal(t, TransactionEventInitiate, confirmedTxnRes.LastTransactionEvent)

	// Feed the fnal check the the remaining events after the previous entry or previous confirmed txn, whichever is smaller.
	finalEntryPrecedingEvents := entryRes.RemainingConsumedEvents
	if len(confirmedTxnRes.RemainingConsumedEvents) < len(entryRes.RemainingConsumedEvents) {
		finalEntryPrecedingEvents = confirmedTxnRes.RemainingConsumedEvents
	}
	// That should be the last balance entry operation in the queue.
	_, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		targetConsumerEvent:  &consumerEventBatch,
		precedingEvents:      finalEntryPrecedingEvents,
		targetEncoderTypes:   []lib.EncoderType{encoderType},
		targetBadgerKeyBytes: targetBadgerKeyBytes,
		exitWhenEmpty:        true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Entry not found in entry batch")
}

func TestConsumer(t *testing.T) {

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, _, _, cleanupFunc := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)
	defer cleanupFunc()

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt(0).SetUint64(123212312324),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	_, txnRes, err := nodeClient.DAOCoins(mintDaoCoinReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash := txnRes.TxnHashHex

	// Verify the state changes for the mint transaction apply as expected.
	VerifyTransactionStateChanges(t, testHandler, txnHash, lib.EncoderTypeBalanceEntry, nil, nil,
		func(entryRes *EntryScanResult, targetBadgerKeyBytes *[]byte) error {
			// TODO: Move these to a generic function param.
			balanceEntries, balanceAncestralEntries, err := DecodeStateChangeEntries[*lib.BalanceEntry](entryRes.EntryBatch)
			if err != nil {
				return err
			}
			require.Len(t, balanceEntries, 1)
			require.Len(t, balanceAncestralEntries, 1)
			require.NotNil(t, balanceEntries[0])
			balanceEntry := *balanceEntries[0]
			require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
			require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], testHandler.Params), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
			require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], testHandler.Params), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
			ancestralBalanceEntry := balanceAncestralEntries[0]
			require.Nil(t, ancestralBalanceEntry)
			return nil
		},
		func(entryRes *EntryScanResult, targetBadgerKeyBytes *[]byte) error {
			require.Equal(t, entryRes.OperationType, lib.DbOperationTypeDelete)
			balanceEntries, balanceAncestralEntries, err := DecodeStateChangeEntries[*lib.BalanceEntry](entryRes.EntryBatch)
			if err != nil {
				return err
			}
			require.Len(t, balanceEntries, 1)
			require.Len(t, balanceAncestralEntries, 1)
			require.NotNil(t, balanceEntries[0])
			balanceEntry := *balanceEntries[0]
			require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
			require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], testHandler.Params), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
			require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], testHandler.Params), mintDaoCoinReq.UpdaterPublicKeyBase58Check)
			ancestralBalanceEntry := balanceAncestralEntries[0]
			require.Nil(t, ancestralBalanceEntry)
			return nil
		},
	)

	// Lock coins for the coin user.
	lockCoinsReq := &routes.CoinLockupRequest{
		TransactorPublicKeyBase58Check: coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58Check:    coinUser.PublicKeyBase58,
		RecipientPublicKeyBase58Check:  coinUser.PublicKeyBase58,
		UnlockTimestampNanoSecs:        time.Now().UnixNano() + 1000000000000,
		VestingEndTimestampNanoSecs:    time.Now().UnixNano() + 1000000000000,
		LockupAmountBaseUnits:          uint256.NewInt(0).SetUint64(1e9),
		ExtraData:                      nil,
		MinFeeRateNanosPerKB:           pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                nil,
	}

	_, txnRes, err = nodeClient.LockCoins(lockCoinsReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	txnHash = txnRes.TxnHashHex

	// Verify the state changes for the lock transaction apply as expected.
	VerifyTransactionStateChanges(t, testHandler, txnHash, lib.EncoderTypeLockedBalanceEntry, nil, nil,
		func(entryRes *EntryScanResult, targetBadgerKeyBytes *[]byte) error {
			lockedBalanceEntries, lockedBalanceAncestralEntries, err := DecodeStateChangeEntries[*lib.LockedBalanceEntry](entryRes.EntryBatch)
			if err != nil {
				return err
			}
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
			return nil

		},
		func(entryRes *EntryScanResult, targetBadgerKeyBytes *[]byte) error {
			require.Equal(t, entryRes.OperationType, lib.DbOperationTypeDelete)
			lockedBalanceEntries, lockedBalanceAncestralEntries, err := DecodeStateChangeEntries[*lib.LockedBalanceEntry](entryRes.EntryBatch)
			if err != nil {
				return err
			}
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
			return nil
		},
	)
}

func TestConsumerBulk(t *testing.T) {

	// TODO: Figure out how to parallelize this test.
	// TODO: Figure out how to update our test handler to search for a specific entry. Maybe we search for a specific badger key? Can I even retrieve that from the transaction?
	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, _, _, cleanupFunc := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)
	defer cleanupFunc()

	parallelism := 40

	fmt.Printf("3\n")

	nodeClient := testConfig.NodeClient
	postUser := testConfig.TestUsers[0]

	createPostReq := &routes.SubmitPostRequest{
		UpdaterPublicKeyBase58Check: postUser.PublicKeyBase58,
		BodyObj: &lib.DeSoBodySchema{
			Body: "Test Post",
		},
		MinFeeRateNanosPerKB: pdh_tests.FeeRateNanosPerKB,
	}

	_, txnRes, err := nodeClient.SubmitPost(createPostReq, postUser.PrivateKey, false, true)
	require.NoError(t, err)
	postHash := txnRes.TxnHashHex

	// Create a slice to store the random post associations.
	randomPostAssocs := make([]string, parallelism)

	txnHashes := make([]string, parallelism)

	// wg := sync.WaitGroup{}

	// wg.Add(parallelism)

	// Generate the post associations.
	for ii := 0; ii < parallelism; ii++ {
		// go func(goroutineId int) {
		// 	defer wg.Done()
		randomPostAssoc := pdh_tests.RandString(10)

		createPostAssocReq := &routes.CreatePostAssociationRequest{
			TransactorPublicKeyBase58Check: postUser.PublicKeyBase58,
			AppPublicKeyBase58Check:        postUser.PublicKeyBase58,
			PostHashHex:                    postHash,
			AssociationType:                randomPostAssoc,
			AssociationValue:               randomPostAssoc,
			MinFeeRateNanosPerKB:           pdh_tests.FeeRateNanosPerKB,
		}

		_, txnRes, err = nodeClient.CreatePostAssociation(createPostAssocReq, postUser.PrivateKey, false, true)
		require.NoError(t, err)

		txnHash := txnRes.TxnHashHex

		txnHashBytes, err := hex.DecodeString(txnHash)
		require.NoError(t, err)

		fmt.Printf("txnHash: %s - target key bytes: %s - createPostAssocReq: %+v\n", txnHash, hex.EncodeToString(lib.DBKeyForPostAssociationByID(&lib.PostAssociationEntry{
			AssociationID: lib.NewBlockHash(txnHashBytes),
		})), createPostAssocReq)
		txnHashes[ii] = txnHash
		randomPostAssocs[ii] = randomPostAssoc
		// }(ii)
	}
	// Wait for all the transactions to be submitted.
	// wg.Wait()

	// consumedEvents := make([]*StateConsumerEvent, 0)

	// Verify each transaction in sequence.
	for ii := 0; ii < parallelism; ii++ {
		txnHash := txnHashes[ii]
		randomPostAssoc := randomPostAssocs[ii]
		txnHashBytes, err := hex.DecodeString(txnHash)
		require.NoError(t, err)

		dbKeyBytes := lib.DBKeyForPostAssociationByID(&lib.PostAssociationEntry{
			AssociationID: lib.NewBlockHash(txnHashBytes),
		})

		// Verify the state changes for the mint transaction apply as expected.
		VerifyTransactionStateChanges(t, testHandler, txnHash, lib.EncoderTypePostAssociationEntry, &dbKeyBytes, testHandler.ConsumedEvents,
			func(entryRes *EntryScanResult, targetBadgerKeyBytes *[]byte) error {

				require.Greater(t, len(entryRes.EntryBatch), 0)

				stateChangeEntry := entryRes.EntryBatch[0]
				found := false
				// If we have a target badger key bytes, we need to find the corresponding state change entry in the batch.
				if targetBadgerKeyBytes != nil {
					for _, entry := range entryRes.EntryBatch {
						if bytes.Equal(entry.KeyBytes, *targetBadgerKeyBytes) {
							stateChangeEntry = entry
							found = true
							break
						}
					}
				}
				require.True(t, found)

				// TODO: Move these to a generic function param.
				postAssocEntries, postAssocAncestralEntries, err := DecodeStateChangeEntries[*lib.PostAssociationEntry]([]*lib.StateChangeEntry{stateChangeEntry})
				if err != nil {
					return err
				}

				require.Len(t, postAssocEntries, 1)
				require.Len(t, postAssocAncestralEntries, 1)
				require.NotNil(t, postAssocEntries[0])
				postAssocEntry := *postAssocEntries[0]

				fmt.Printf("StateChangeEntry: %+v\n", *stateChangeEntry)
				fmt.Printf("PostAssocEntry: %+v\n", *postAssocEntry)

				require.Equal(t, postUser.PublicKeyBase58, consumer.PublicKeyBytesToBase58Check(postAssocEntry.AppPKID[:], desoParams))
				require.Equal(t, postUser.PublicKeyBase58, consumer.PublicKeyBytesToBase58Check(postAssocEntry.TransactorPKID[:], desoParams))
				require.Equal(t, postHash, hex.EncodeToString(postAssocEntry.PostHash[:]))
				require.Equal(t, randomPostAssoc, string(postAssocEntry.AssociationType[:]))
				require.Equal(t, randomPostAssoc, string(postAssocEntry.AssociationValue[:]))
				ancestralEntry := postAssocAncestralEntries[0]
				require.Nil(t, ancestralEntry)
				return nil
			},
			func(entryRes *EntryScanResult, targetBadgerKeyBytes *[]byte) error {
				// require.Equal(t, entryRes.OperationType, lib.DbOperationTypeDelete)

				require.Greater(t, len(entryRes.EntryBatch), 0)

				stateChangeEntry := entryRes.EntryBatch[0]
				found := false
				// If we have a target badger key bytes, we need to find the corresponding state change entry in the batch.
				if targetBadgerKeyBytes != nil {
					for _, entry := range entryRes.EntryBatch {
						if bytes.Equal(entry.KeyBytes, *targetBadgerKeyBytes) {
							stateChangeEntry = entry
							found = true
							break
						}
					}
					require.Truef(t, found, "Failed to find state change entry in batch for target key bytes %s", hex.EncodeToString(*targetBadgerKeyBytes))
				}

				// TODO: Move these to a generic function param.
				postAssocEntries, postAssocAncestralEntries, err := DecodeStateChangeEntries[*lib.PostAssociationEntry]([]*lib.StateChangeEntry{stateChangeEntry})
				if err != nil {
					return err
				}
				require.Len(t, postAssocEntries, 1)
				require.Len(t, postAssocAncestralEntries, 1)
				require.NotNil(t, postAssocEntries[0])
				postAssocEntry := *postAssocEntries[0]
				require.Equal(t, postUser.PublicKeyBase58, consumer.PublicKeyBytesToBase58Check(postAssocEntry.AppPKID[:], desoParams))
				require.Equal(t, postUser.PublicKeyBase58, consumer.PublicKeyBytesToBase58Check(postAssocEntry.TransactorPKID[:], desoParams))
				require.Equal(t, postHash, hex.EncodeToString(postAssocEntry.PostHash[:]))
				require.Equal(t, randomPostAssoc, string(postAssocEntry.AssociationType[:]))
				require.Equal(t, randomPostAssoc, string(postAssocEntry.AssociationValue[:]))
				ancestralEntry := postAssocAncestralEntries[0]
				require.Nil(t, ancestralEntry)
				return nil
			},
		)
		fmt.Printf("Validated txn %d\n", ii)
	}

}

func TestRemoveTransaction(t *testing.T) {

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, nodeServer, _, cleanupFunc := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)
	defer cleanupFunc()

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt(0).SetUint64(123212312324),
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
	testConfig, testHandler, _, nodeServer, _, cleanupFunc := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)
	defer cleanupFunc()

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt(0).SetUint64(1e9 + 1),
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

	// Mint some DAO coins for the coin user again. This will create a new balance entry, with an ancestral record being the old balance entry.
	mintDaoCoinReq2 := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt(0).SetUint64(1234982123),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

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
	totalBalance := uint256.NewInt(0).Add(&mintDaoCoinReq.CoinsToMintNanos, &mintDaoCoinReq2.CoinsToMintNanos)
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
	totalBalance = uint256.NewInt(0).Add(&mintDaoCoinReq.CoinsToMintNanos, &mintDaoCoinReq2.CoinsToMintNanos)
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
	totalBalance = uint256.NewInt(0).Add(&mintDaoCoinReq.CoinsToMintNanos, &mintDaoCoinReq2.CoinsToMintNanos)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq2.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(balanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq2.UpdaterPublicKeyBase58Check)
	require.True(t, balanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos), "Balance: %s, Total balance: %s", balanceEntry.BalanceNanos.String(), mintDaoCoinReq.CoinsToMintNanos.String())
	require.NotNil(t, balanceAncestralEntries[0])
	ancestralBalanceEntry = *balanceAncestralEntries[0]
	require.True(t, ancestralBalanceEntry.BalanceNanos.Eq(&mintDaoCoinReq.CoinsToMintNanos))
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.HODLerPKID[:], desoParams), mintDaoCoinReq.ProfilePublicKeyBase58CheckOrUsername)
	require.Equal(t, consumer.PublicKeyBytesToBase58Check(ancestralBalanceEntry.CreatorPKID[:], desoParams), mintDaoCoinReq.UpdaterPublicKeyBase58Check)

	// The transaction shouldn't be confirmed.
	_, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
		precedingEvents:       balanceEntryRes.ConsumedEvents,
		targetConsumerEvent:   &consumerEventBatch,
		targetTransactionHash: &txnHash2,
		targetIsMempool:       &falseValue,
		exitWhenEmpty:         true,
	})
	require.Error(t, err)

	// TODO: Move the full test environment into the consumer repo.
	// TODO: Test unconfirmed block, transaction, and block node logic.
	// TODO: Figure out how to get this working in CI/CD.
}

func TestBlockEvents(t *testing.T) {
	// Skip this until we figure out how to fix uncommitted txns.
	// t.Skip()

	desoParams := &lib.DeSoTestnetParams
	testConfig, testHandler, _, _, _, cleanupFunc := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)
	defer cleanupFunc()

	nodeClient := testConfig.NodeClient
	coinUser := testConfig.TestUsers[0]

	// Mint some DAO coins for the coin user.
	mintDaoCoinReq := &routes.DAOCoinRequest{
		UpdaterPublicKeyBase58Check:           coinUser.PublicKeyBase58,
		ProfilePublicKeyBase58CheckOrUsername: coinUser.PublicKeyBase58,
		OperationType:                         routes.DAOCoinOperationStringMint,
		CoinsToMintNanos:                      *uint256.NewInt(0).SetUint64(123212312324),
		TransferRestrictionStatus:             routes.TransferRestrictionStatusStringUnrestricted,
		MinFeeRateNanosPerKB:                  pdh_tests.FeeRateNanosPerKB,
		TransactionFees:                       nil,
	}

	_, _, err := nodeClient.DAOCoins(mintDaoCoinReq, coinUser.PrivateKey, false, true)
	require.NoError(t, err)

	var entryRes *EntryScanResult

	for ii := 0; ii < 250; ii++ {
		entryRes, err = testHandler.WaitForMatchingEntryBatch(&ConsumerEventSearch{
			targetConsumerEvent: &consumerEventBatch,
			targetEncoderTypes:  []lib.EncoderType{lib.EncoderTypeBlock, lib.EncoderTypeBlockNode, lib.EncoderTypeTxn},
			exitWhenEmpty:       false,
		})
		require.NoError(t, err)

		if entryRes.EncoderType == lib.EncoderTypeBlock {
			blocks, _, err := DecodeStateChangeEntries[*lib.MsgDeSoBlock](entryRes.EntryBatch)
			require.NoError(t, err)
			fmt.Printf("Block event: |%2d|%t|%t|%s\n", entryRes.OperationType, entryRes.IsMempool, entryRes.IsReverted, entryRes.FlushId.String())
			fmt.Printf("Block entry: %+v\n", *blocks[0])
			blockEntry := *blocks[0]
			blockTxns, err := BlockToTransactionEntries(blockEntry, entryRes.EntryBatch[0].KeyBytes, desoParams)
			require.NoError(t, err)
			for _, txn := range blockTxns {
				if txn.TxnType == uint16(lib.TxnTypeDAOCoin) {
					fmt.Printf("Block entry txn: %+v\n", *txn)
				}
			}
			//fmt.Printf("Block entry key bytes: %+v\n", balanceEntryRes.EntryBatch[0].KeyBytes)
			//fmt.Printf("Block entry encoder bytes: %+v\n", balanceEntryRes.EntryBatch[0].EncoderBytes)
		}
		if entryRes.EncoderType == lib.EncoderTypeBlockNode {
			blockNode, _, err := DecodeStateChangeEntries[*lib.BlockNode](entryRes.EntryBatch)
			require.NoError(t, err)
			fmt.Printf("Node event: |%2d|%t|%t|%s\n", entryRes.OperationType, entryRes.IsMempool, entryRes.IsReverted, entryRes.FlushId.String())
			fmt.Printf("Node entry: %+v\n", *blockNode[0])
			//fmt.Printf("Node entry key bytes: %+v\n", balanceEntryRes.EntryBatch[0].KeyBytes)
			//fmt.Printf("Node entry encoder bytes: %+v\n", balanceEntryRes.EntryBatch[0].EncoderBytes)
		}
		if entryRes.EncoderType == lib.EncoderTypeTxn {
			txn, _, err := DecodeStateChangeEntries[*lib.MsgDeSoTxn](entryRes.EntryBatch)
			require.NoError(t, err)
			fmt.Printf("Txn event: |%2d|%t|%t|%s\n", entryRes.OperationType, entryRes.IsMempool, entryRes.IsReverted, entryRes.FlushId.String())
			fmt.Printf("Txn entry: %+v\n", *txn[0])
		}
	}
}
