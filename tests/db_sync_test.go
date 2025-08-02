package tests

import (
	"fmt"
	"log"
	"testing"

	routes "github.com/deso-protocol/backend/routes"
	"github.com/deso-protocol/core/lib"
	pdh_tests "github.com/deso-protocol/postgres-data-handler/tests"
	"github.com/dgraph-io/badger/v3"
)

func TestDBSync(t *testing.T) {
	desoParams := &lib.DeSoTestnetParams
	// TODO: Cleanup the consumer test environemnt fn to remove consumer specific logic.
	testConfig, _, _, _, _, cleanupFunc := SetupConsumerTestEnvironment(t, 3, pdh_tests.RandString(10), desoParams)
	defer cleanupFunc()

	// Get the badger directory.
	dir := testConfig.NodeClient.BadgerDir

	// Open a badgerdb in a temporary directory.
	opts := badger.DefaultOptions(dir)
	opts.Dir = dir
	opts.ValueDir = dir
	// No logger when running tests
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		routes.CleanUpBadger(db)
	}()
	fmt.Printf("\n\n***DB: %+v\n\n", db)
}
