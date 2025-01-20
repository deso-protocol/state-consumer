package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/deso-protocol/backend/config"
	"github.com/deso-protocol/backend/routes"
	coreCmd "github.com/deso-protocol/core/cmd"
	"github.com/deso-protocol/core/lib"
	"github.com/deso-protocol/postgres-data-handler/entries"
	pdh_tests "github.com/deso-protocol/postgres-data-handler/tests"
	"github.com/deso-protocol/state-consumer/consumer"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const (
	globalStateSharedSecret = "abcdef"
)

// Variables to be used in the tests. These are defined here so we can de-reference them in the tests.
var (
	trueValue                = true
	falseValue               = false
	consumerEventBatch       = ConsumerEventBatch
	consumerEventTransaction = ConsumerEventTransaction
	consumerEventSyncEvent   = ConsumerEventSyncEvent
)

type TestHandler struct {
	// Params is a struct containing the current blockchain parameters.
	// It is used to determine which prefix to use for public keys.
	Params *lib.DeSoParams

	// ConsumerEventChan is a channel that receives StateConsumerEvents.
	ConsumerEventChan chan *StateConsumerEvent

	// ConsumedEvents is a list of all the events that have been consumed.
	ConsumedEvents []*StateConsumerEvent

	LastTransactionEvent TransactionEvent
}

func NewTestHandler(params *lib.DeSoParams) *TestHandler {
	th := &TestHandler{}
	th.Params = params

	th.ConsumerEventChan = make(chan *StateConsumerEvent)
	th.ConsumedEvents = []*StateConsumerEvent{}
	th.LastTransactionEvent = TransactionEventUndefined

	return th
}

type ConsumerEvent uint16

// Block view encoder types. These types to different structs implementing the DeSoEncoder interface.
const (
	ConsumerEventBatch       ConsumerEvent = 0
	ConsumerEventTransaction ConsumerEvent = 1
	ConsumerEventSyncEvent   ConsumerEvent = 2
	ConsumerEventUndefined   ConsumerEvent = 3
)

type TransactionEvent uint16

const (
	TransactionEventInitiate  TransactionEvent = 0
	TransactionEventCommit    TransactionEvent = 1
	TransactionEventRollback  TransactionEvent = 2
	TransactionEventUndefined TransactionEvent = 3
)

type StateConsumerEvent struct {
	BatchedEntries   []*lib.StateChangeEntry
	IsMempool        bool
	EventType        ConsumerEvent
	TransactionEvent TransactionEvent
}

func (th *TestHandler) HandleEntryBatch(batchedEntries []*lib.StateChangeEntry, isMempool bool) error {
	// Add the batched entries to the channel.
	th.ConsumerEventChan <- &StateConsumerEvent{
		BatchedEntries: batchedEntries,
		IsMempool:      isMempool,
		EventType:      ConsumerEventBatch,
	}

	return nil
}

func (th *TestHandler) HandleSyncEvent(syncEvent consumer.SyncEvent) error {
	//th.SyncEventChan <- syncEvent
	return nil
}

func (th *TestHandler) InitiateTransaction() error {
	th.ConsumerEventChan <- &StateConsumerEvent{
		TransactionEvent: TransactionEventInitiate,
		EventType:        ConsumerEventTransaction,
	}
	return nil
}

func (th *TestHandler) CommitTransaction() error {
	th.ConsumerEventChan <- &StateConsumerEvent{
		TransactionEvent: TransactionEventCommit,
		EventType:        ConsumerEventTransaction,
	}
	return nil
}

func (th *TestHandler) RollbackTransaction() error {
	th.ConsumerEventChan <- &StateConsumerEvent{
		TransactionEvent: TransactionEventRollback,
		EventType:        ConsumerEventTransaction,
	}
	return nil
}

func (th *TestHandler) GetParams() *lib.DeSoParams {
	return th.Params
}

func CleanupConsumerTestEnvironment(apiServer *routes.APIServer, nodeServer *lib.Server, cancelFunc context.CancelFunc) {
	cancelFunc()
	nodeServer.Stop()
}

func SetupConsumerTestEnvironment(t *testing.T, testUserCount int, entropyStr string, params *lib.DeSoParams) (*pdh_tests.TestConfig, *TestHandler, *routes.APIServer, *lib.Server, *consumer.StateSyncerConsumer, func()) {
	pdh_tests.SetupFlags("../.env")
	starterAccountSeed := viper.GetString("TEST_STARTER_DESO_SEED")
	starterUser, _, err := pdh_tests.CreateTestUser(starterAccountSeed, "", 0, params, nil)

	stateDirPostFix := pdh_tests.RandString(10)

	stateChangeDir := fmt.Sprintf("./ss/state-changes-%s-%s", t.Name(), stateDirPostFix)
	consumerProgressDir := fmt.Sprintf("./ss/consumer-progress-%s-%s", t.Name(), stateDirPostFix)

	apiServer, nodeServer := newTestApiServer(t, starterUser, 17001, stateChangeDir)

	require.NoError(t, err)

	// Start the api server in a non-blocking way.
	go func() {
		apiServer.Start()
	}()

	testConfig, err := pdh_tests.SetupTestEnvironment(testUserCount, entropyStr, false)
	require.NoError(t, err)

	testHandler := NewTestHandler(params)

	stateSyncerConsumer := &consumer.StateSyncerConsumer{}

	// Initialize and run the state syncer consumer in a non-blocking thread.
	// Create a context with cancel to control the goroutine
	_, cancel := context.WithCancel(context.Background())

	// Start consumer in goroutine
	go func() {
		err := stateSyncerConsumer.InitializeAndRun(
			stateChangeDir,
			consumerProgressDir,
			500000,
			1,
			true,
			testHandler,
		)
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	// Create cleanup function to return
	cleanupFunc := func() {
		// Cancel context to stop goroutine
		cancel()

		// Delete state change directory
		if err := os.RemoveAll(stateChangeDir); err != nil {
			fmt.Printf("Error removing state change dir: %v\n", err)
		}

		// Delete consumer progress directory
		if err := os.RemoveAll(consumerProgressDir); err != nil {
			fmt.Printf("Error removing consumer progress dir: %v\n", err)
		}
	}

	return testConfig, testHandler, apiServer, nodeServer, stateSyncerConsumer, cleanupFunc
}

// TODO: Make sure that state change dir gets cleaned up.
func newTestApiServer(t *testing.T, starterUser *pdh_tests.TestUser, apiPort uint16, stateChangeDir string) (*routes.APIServer, *lib.Server) {
	// Create a badger db instance.
	badgerDB, badgerDir := routes.GetTestBadgerDb(t)

	// Set core node's config.
	coreConfig := coreCmd.LoadConfig()
	coreConfig.Params = &lib.DeSoTestnetParams
	coreConfig.DataDirectory = badgerDir
	coreConfig.Regtest = true
	coreConfig.RegtestAccelerated = true
	coreConfig.TXIndex = false
	coreConfig.DisableNetworking = true
	coreConfig.MinerPublicKeys = []string{starterUser.PublicKeyBase58}
	coreConfig.BlockProducerSeed = starterUser.SeedPhrase
	coreConfig.PosValidatorSeed = starterUser.SeedPhrase
	coreConfig.NumMiningThreads = 1
	coreConfig.HyperSync = false
	coreConfig.MinFeerate = 2000
	coreConfig.LogDirectory = ""
	coreConfig.StateChangeDir = stateChangeDir
	coreConfig.GlogV = 0
	coreConfig.NoLogToStdErr = true

	// Create a core node.
	shutdownListener := make(chan struct{})
	node := coreCmd.NewNode(coreConfig)
	node.Start(&shutdownListener)

	// Set api server's config.
	apiConfig := config.LoadConfig(coreConfig)
	//      - STARTER_DESO_SEED=road congress client market couple bid risk escape artwork rookie artwork food
	//apiConfig.StarterDESOSeed = starterUser.SeedPhrase
	apiConfig.APIPort = apiPort
	apiConfig.GlobalStateRemoteNode = ""
	apiConfig.GlobalStateRemoteSecret = globalStateSharedSecret
	apiConfig.RunHotFeedRoutine = false
	apiConfig.RunSupplyMonitoringRoutine = false
	apiConfig.AdminPublicKeys = []string{starterUser.PublicKeyBase58}
	apiConfig.SuperAdminPublicKeys = []string{starterUser.PublicKeyBase58}

	// Create an api server.
	apiServer, err := routes.NewAPIServer(
		node.Server,
		node.Server.GetMempool(),
		node.Server.GetBlockchain(),
		node.Server.GetBlockProducer(),
		node.TXIndex,
		node.Params,
		apiConfig,
		node.Config.MinFeerate,
		badgerDB,
		nil,
		node.Config.BlockCypherAPIKey,
	)
	require.NoError(t, err)

	// Initialize api server.
	apiServer.MinFeeRateNanosPerKB = node.Config.MinFeerate

	return apiServer, node.Server
}

// Function to decode state change entries with a generic type EncoderType.
func DecodeStateChangeEntries[EncoderType lib.DeSoEncoder](entryBatch []*lib.StateChangeEntry) ([]*EncoderType, []*EncoderType, error) {
	var decodedEntries []*EncoderType
	var decodedAncestralRecords []*EncoderType

	for _, entry := range entryBatch {
		decodedEntry, decodedAncestralRecord, err := DecodeStateChangeEntryEncoders[EncoderType](entry)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "DecodeStateChangeEntries: Problem decoding entry")
		}
		decodedEntries = append(decodedEntries, decodedEntry)
		decodedAncestralRecords = append(decodedAncestralRecords, decodedAncestralRecord)
	}

	return decodedEntries, decodedAncestralRecords, nil
}

// Function to decode a single state change entry with a generic type EncoderType.
func DecodeStateChangeEntryEncoders[EncoderType lib.DeSoEncoder](entry *lib.StateChangeEntry) (*EncoderType, *EncoderType, error) {
	decodedEntry := entry.EncoderType.New()
	var decodedAncestralRecord lib.DeSoEncoder

	// You need to pass a pointer to the value of decodedEntry.
	err := consumer.DecodeEntry(decodedEntry, entry.EncoderBytes)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "DecodeStateChangeEntry: Problem decoding entry")
	}

	// Cast the decoded entry to the EncoderType.
	typedDecodedEntry := decodedEntry.(EncoderType)

	if entry.AncestralRecordBytes != nil && (len(entry.AncestralRecordBytes) > 0 && entry.AncestralRecordBytes[0] != 0) {
		decodedAncestralRecord = entry.EncoderType.New()
		// You need to pass a pointer to the value of decodedAncestralRecord.
		err = consumer.DecodeEntry(decodedAncestralRecord, entry.AncestralRecordBytes)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "DecodeStateChangeEntry: Problem decoding ancestral record")
		}
		typedAncestralRecord := decodedAncestralRecord.(EncoderType)
		return &typedDecodedEntry, &typedAncestralRecord, nil
	} else {
		return &typedDecodedEntry, nil, nil
	}
}

// EntryScanResult is a struct that contains the results of a search for a transaction or entry in the ConsumerEventChan.
type EntryScanResult struct {
	EventsScanned           int
	EntryBatch              []*lib.StateChangeEntry
	ConsumedEvents          []*StateConsumerEvent
	RemainingConsumedEvents []*StateConsumerEvent
	IsMempool               bool
	IsReverted              bool
	EncoderType             lib.EncoderType
	OperationType           lib.StateSyncerOperationType
	Txn                     *entries.PGTransactionEntry
	RemainingTxns           []*entries.PGTransactionEntry
	FlushId                 uuid.UUID
	TransactionCommits      int
	TransactionInitiates    int
	LastTransactionEvent    TransactionEvent
}

// GetNextBatch returns the next batch of entries from the ConsumerEventChan.
func (th *TestHandler) GetNextBatch() *EntryScanResult {
	batchEvent := <-th.ConsumerEventChan
	batchedEntries := batchEvent.BatchedEntries
	return &EntryScanResult{
		EntryBatch:    batchedEntries,
		IsMempool:     batchEvent.IsMempool,
		IsReverted:    batchedEntries[0].IsReverted,
		EventsScanned: 1,
		FlushId:       batchedEntries[0].FlushId,
		EncoderType:   batchedEntries[0].EncoderType,
		OperationType: batchedEntries[0].OperationType,
	}
}

func (th *TestHandler) ConsumeAllEvents() {
	for range th.ConsumerEventChan {
		if len(th.ConsumerEventChan) == 0 {
			return
		}
	}
}

// Struct to use as input for WaitForMatchingEntryBatch.
type ConsumerEventSearch struct {
	precedingEvents       []*StateConsumerEvent
	targetConsumerEvent   *ConsumerEvent
	targetEncoderTypes    []lib.EncoderType
	targetOpType          *lib.StateSyncerOperationType
	targetTransactionHash *string
	currentFlushId        *uuid.UUID
	targetIsMempool       *bool
	targetIsReverted      *bool
	targetTxnEvent        *TransactionEvent
	exitWhenEmpty         bool
	targetBadgerKeyBytes  *[]byte
}

// WaitForMatchingEntryBatch waits for an entry batch with the given encoder type, operation type, or flush id to appear in the ConsumerEventChan.
func (th *TestHandler) WaitForMatchingEntryBatch(searchCriteria *ConsumerEventSearch) (*EntryScanResult, error) {
	// Track the number of batches we've scanned.
	eventsScanned := 0

	// Track the # of transaction initiate and commit events that have occurred.
	transactionCommits := 0
	transactionInitiates := 0
	lastTransactionEvent := th.LastTransactionEvent

	// First check the preceding flush events to see if the event is there.
	for ii, precedingEvent := range searchCriteria.precedingEvents {
		if precedingEvent.EventType == ConsumerEventTransaction {
			if precedingEvent.TransactionEvent == TransactionEventCommit {
				transactionCommits += 1
			} else if precedingEvent.TransactionEvent == TransactionEventInitiate {
				transactionInitiates += 1
			}
			lastTransactionEvent = precedingEvent.TransactionEvent
			th.LastTransactionEvent = lastTransactionEvent
		}
		eventsScanned += 1

		if res, err := th.BatchMatchesSearch(precedingEvent, searchCriteria); err != nil {
			return nil, errors.Wrapf(err, "WaitForMatchingEntryBatch: Problem checking for matching entry batch")
		} else if res != nil {
			res.EventsScanned = eventsScanned
			res.TransactionCommits = transactionCommits
			res.TransactionInitiates = transactionInitiates
			res.LastTransactionEvent = lastTransactionEvent
			// Return the events that were parsed.
			res.ConsumedEvents = searchCriteria.precedingEvents[:ii+1]
			// Return only the preceding events that weren't parsed.
			res.RemainingConsumedEvents = searchCriteria.precedingEvents[ii+1:]
			// Return the result and the remaining preceding flush events.
			return res, nil
		}
	}

	consumedFlushEvents := []*StateConsumerEvent{}

	// Continue retrieving entries from the ConsumerEventChan until we find the transaction hash.
	for consumerEvent := range th.ConsumerEventChan {
		consumedFlushEvents = append(consumedFlushEvents, consumerEvent)
		th.ConsumedEvents = append(th.ConsumedEvents, consumerEvent)
		eventsScanned += 1

		if consumerEvent.EventType == ConsumerEventTransaction {
			if consumerEvent.TransactionEvent == TransactionEventCommit {
				transactionCommits += 1
			} else if consumerEvent.TransactionEvent == TransactionEventInitiate {
				transactionInitiates += 1
			}
			lastTransactionEvent = consumerEvent.TransactionEvent
			th.LastTransactionEvent = lastTransactionEvent
		}

		if res, err := th.BatchMatchesSearch(consumerEvent, searchCriteria); err != nil {
			return nil, errors.Wrapf(err, "WaitForMatchingEntryBatch: Problem checking for matching entry batch")
		} else if res != nil {
			res.EventsScanned = eventsScanned
			res.ConsumedEvents = consumedFlushEvents
			res.TransactionCommits = transactionCommits
			res.TransactionInitiates = transactionInitiates
			res.LastTransactionEvent = lastTransactionEvent
			return res, nil
		}
		if searchCriteria.exitWhenEmpty && len(th.ConsumerEventChan) == 0 {
			return &EntryScanResult{
				EventsScanned:        eventsScanned,
				ConsumedEvents:       consumedFlushEvents,
				TransactionCommits:   transactionCommits,
				TransactionInitiates: transactionInitiates,
				LastTransactionEvent: lastTransactionEvent,
			}, fmt.Errorf("WaitForMatchingEntryBatch: Entry not found in entry batch")
		}
	}
	return nil, fmt.Errorf("WaitForMatchingEntryBatch: Entry not found in entry batch")
}

// Check to see if the batch event matches the search criteria.
func (th *TestHandler) BatchMatchesSearch(
	consumerEvent *StateConsumerEvent,
	searchCriteria *ConsumerEventSearch,
) (*EntryScanResult, error) {

	if consumerEvent.EventType == ConsumerEventTransaction {
		if searchCriteria.targetTxnEvent != nil && consumerEvent.TransactionEvent == *searchCriteria.targetTxnEvent {
			return &EntryScanResult{
				EntryBatch: consumerEvent.BatchedEntries,
				IsMempool:  consumerEvent.IsMempool,
			}, nil
		}
		return nil, nil
	}

	nextEntryBatch := consumerEvent.BatchedEntries

	encoderType := nextEntryBatch[0].EncoderType
	operationType := nextEntryBatch[0].OperationType
	flushId := nextEntryBatch[0].FlushId
	isMempool := consumerEvent.IsMempool
	isReverted := nextEntryBatch[0].IsReverted

	transactionMatch := true
	var err error

	if searchCriteria.targetTransactionHash != nil {
		transactionMatch, err = th.TransactionInEntryBatch(*searchCriteria.targetTransactionHash, nextEntryBatch)
		if err != nil {
			return nil, errors.Wrapf(err, "BatchMatchesSearch: Problem checking for transaction in entry batch")
		}
	}

	badgerKeyMatch := true
	if searchCriteria.targetBadgerKeyBytes != nil {
		badgerKeyMatch, err = th.BadgerKeyInEntryBatch(*searchCriteria.targetBadgerKeyBytes, nextEntryBatch)
		if err != nil {
			return nil, errors.Wrapf(err, "BatchMatchesSearch: Problem checking for badger key in entry batch")
		}
	}

	if EncoderTypeMatchesTargets(encoderType, searchCriteria.targetEncoderTypes) &&
		(searchCriteria.targetConsumerEvent == nil || consumerEvent.EventType == *searchCriteria.targetConsumerEvent) &&
		(searchCriteria.targetOpType == nil || operationType == *searchCriteria.targetOpType) &&
		(searchCriteria.currentFlushId == nil || *searchCriteria.currentFlushId != flushId) &&
		(searchCriteria.targetIsMempool == nil || *searchCriteria.targetIsMempool == isMempool) &&
		(searchCriteria.targetIsReverted == nil || *searchCriteria.targetIsReverted == isReverted) &&
		(badgerKeyMatch) &&
		(transactionMatch) {

		return &EntryScanResult{
			EntryBatch:    nextEntryBatch,
			IsMempool:     consumerEvent.IsMempool,
			IsReverted:    nextEntryBatch[0].IsReverted,
			FlushId:       nextEntryBatch[0].FlushId,
			EncoderType:   encoderType,
			OperationType: operationType,
		}, nil
	}
	return nil, nil
}

func EncoderTypeMatchesTargets(encoderType lib.EncoderType, targetEncoderTypes []lib.EncoderType) bool {
	if targetEncoderTypes == nil {
		return true
	}
	for _, targetEncoderType := range targetEncoderTypes {
		if encoderType == targetEncoderType {
			return true
		}
	}
	return false
}

func (th *TestHandler) TransactionInEntryBatch(txnHash string, entryBatch []*lib.StateChangeEntry) (bool, error) {
	txns, err := ParseTransactionsFromEntryBatch(entryBatch, th.Params)
	if err != nil {
		return false, errors.Wrapf(err, "WaitForTxnHash: Problem parsing transactions from entry batch")
	}

	for _, txn := range txns {
		if txn.TransactionHash == txnHash {
			return true, nil
		}
	}
	return false, nil
}

func (th *TestHandler) BadgerKeyInEntryBatch(badgerKey []byte, entryBatch []*lib.StateChangeEntry) (bool, error) {
	for _, entry := range entryBatch {
		if bytes.Equal(entry.KeyBytes, badgerKey) {
			return true, nil
		}
	}
	return false, nil
}

// TODO: Delete this function.
// WaitForTxnHash waits for a transaction with the given hash to appear in the ConsumerEventChan.
func (th *TestHandler) WaitForTxnHash(txnHash string, isMempool *bool) (*EntryScanResult, error) {
	// Track the number of batches we've scanned.
	batchesScanned := 0

	precedingBatchesInFlush := []*StateConsumerEvent{}
	currentFlushId := uuid.Nil

	// Continue retrieving entries from the ConsumerEventChan until we find the transaction hash.
	for batchEvent := range th.ConsumerEventChan {
		batchesScanned += 1

		if currentFlushId == batchEvent.BatchedEntries[0].FlushId {
			precedingBatchesInFlush = append(precedingBatchesInFlush, batchEvent)
		} else {
			precedingBatchesInFlush = []*StateConsumerEvent{batchEvent}
			currentFlushId = batchEvent.BatchedEntries[0].FlushId
		}

		nextEntryBatch := batchEvent.BatchedEntries

		if isMempool != nil && *isMempool != batchEvent.IsMempool {
			continue
		}

		txns, err := ParseTransactionsFromEntryBatch(nextEntryBatch, th.Params)
		if err != nil {
			return nil, errors.Wrapf(err, "WaitForTxnHash: Problem parsing transactions from entry batch")
		}

		for ii, txn := range txns {
			if txn.TransactionHash == txnHash {
				// Return the transaction, and all the following transactions.
				return &EntryScanResult{
					EntryBatch:     nextEntryBatch,
					EventsScanned:  batchesScanned,
					ConsumedEvents: precedingBatchesInFlush[:len(precedingBatchesInFlush)-1],
					IsMempool:      batchEvent.IsMempool,
					IsReverted:     nextEntryBatch[0].IsReverted,
					FlushId:        nextEntryBatch[0].FlushId,
					EncoderType:    nextEntryBatch[0].EncoderType,
					OperationType:  nextEntryBatch[0].OperationType,
					Txn:            txn,
					RemainingTxns:  txns[ii+1:],
				}, nil
			}
		}
	}
	return nil, fmt.Errorf("WaitForTxnHash: Transaction not found in entry batch")
}

func ParseTransactionsFromEntryBatch(entryBatch []*lib.StateChangeEntry, params *lib.DeSoParams) ([]*entries.PGTransactionEntry, error) {
	encoderType := entryBatch[0].EncoderType
	operationType := entryBatch[0].OperationType

	txns := []*entries.PGTransactionEntry{}

	if operationType == lib.DbOperationTypeDelete {
		return txns, nil
	}

	if encoderType == lib.EncoderTypeTxn {
		transformedTxns, err := entries.TransformTransactionEntry(entryBatch, params)
		if err != nil {
			return nil, errors.Wrapf(err, "ParseTransactionsFromEntryBatch: Problem converting transaction entries")
		}
		return transformedTxns, nil
	}

	for _, entry := range entryBatch {
		if encoderType == lib.EncoderTypeBlock {
			blockTxns, err := BlockToTransactionEntries(entry.Encoder.(*lib.MsgDeSoBlock), entry.KeyBytes, params)
			if err != nil {
				return nil, errors.Wrapf(err, "ParseTransactionsFromEntryBatch: Problem converting block entry to transaction entries")
			}
			txns = append(txns, blockTxns...)
		} else if entry.Block != nil {
			blockTxns, err := BlockToTransactionEntries(entry.Block, entry.KeyBytes, params)
			if err != nil {
				return nil, errors.Wrapf(err, "ParseTransactionsFromEntryBatch: Problem converting block property to transaction entries")
			}
			txns = append(txns, blockTxns...)
		}
	}

	return txns, nil
}

func BlockToTransactionEntries(block *lib.MsgDeSoBlock, keyBytes []byte, params *lib.DeSoParams) ([]*entries.PGTransactionEntry, error) {
	blockEntry, _ := entries.BlockEncoderToPGStruct(block, keyBytes, params)
	txns := []*entries.PGTransactionEntry{}
	for ii, txn := range block.Txns {
		indexInBlock := uint64(ii)
		pgTxn, err := entries.TransactionEncoderToPGStruct(
			txn,
			&indexInBlock,
			blockEntry.BlockHash,
			blockEntry.Height,
			blockEntry.Timestamp,
			nil,
			nil,
			params,
		)
		if err != nil {
			return txns, errors.Wrapf(
				err,
				"entries.transformAndBulkInsertTransactionEntry: Problem converting transaction to PG struct",
			)
		}
		txns = append(txns, pgTxn)
	}
	return txns, nil
}
