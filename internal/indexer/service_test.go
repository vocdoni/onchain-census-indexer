package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

type compactingDB struct {
	db.Database
	compactCalls int
}

func (d *compactingDB) Compact() error {
	d.compactCalls++
	return d.Database.Compact()
}

func TestSyncContractsPurgesExpiredContractsAndCompactsStore(t *testing.T) {
	ctx := context.Background()
	baseDB, err := metadb.New(db.TypeInMem, "")
	if err != nil {
		t.Fatalf("create in-memory db: %v", err)
	}
	defer func() {
		if cerr := baseDB.Close(); cerr != nil {
			t.Fatalf("close db: %v", cerr)
		}
	}()

	database := &compactingDB{Database: baseDB}
	eventStore := store.New(database)

	contract := common.HexToAddress("0x9999999999999999999999999999999999999999")
	expiresAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	if err := eventStore.SaveContract(ctx, 1, contract, 100, expiresAt); err != nil {
		t.Fatalf("save expired contract: %v", err)
	}
	if err := eventStore.SaveEvents(ctx, 1, contract, []store.Event{
		{ChainID: 1, Contract: contract.Hex(), Account: "0xabc", PreviousWeight: "1", NewWeight: "2", BlockNumber: 101, LogIndex: 0},
	}, 101); err != nil {
		t.Fatalf("save contract events: %v", err)
	}

	svc, err := NewService(ServiceConfig{
		Pool:  rpc.NewWeb3Pool(),
		Store: eventStore,
	})
	if err != nil {
		t.Fatalf("create indexer service: %v", err)
	}

	errCh := make(chan error, 1)
	svc.syncContracts(ctx, errCh)

	select {
	case err := <-errCh:
		t.Fatalf("unexpected sync error: %v", err)
	default:
	}

	records, err := eventStore.ListContracts(ctx)
	if err != nil {
		t.Fatalf("list contracts: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected expired contract metadata to be purged, got %+v", records)
	}

	events, err := eventStore.ListEvents(ctx, store.ListOptions{
		ChainID:  1,
		Contract: contract,
	})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected expired contract events to be purged, got %d", len(events))
	}

	if _, ok, err := eventStore.LastIndexedBlock(ctx, 1, contract); err != nil {
		t.Fatalf("last indexed block: %v", err)
	} else if ok {
		t.Fatalf("expected last indexed block to be purged")
	}

	if database.compactCalls != 1 {
		t.Fatalf("expected exactly one compaction after purge, got %d", database.compactCalls)
	}
}
