package indexer

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

func TestSyncOnceVerificationRecoversMissingEvent(t *testing.T) {
	ctx := context.Background()
	database, err := metadb.New(db.TypeInMem, "")
	if err != nil {
		t.Fatalf("create in-memory db: %v", err)
	}
	defer func() {
		if cerr := database.Close(); cerr != nil {
			t.Fatalf("close db: %v", cerr)
		}
	}()
	eventStore := store.New(database)

	contract := common.HexToAddress("0x1212121212121212121212121212121212121212")
	idx := &Indexer{
		store:           eventStore,
		chainID:         1,
		contract:        contract,
		startBlock:      1,
		batchSize:       3,
		verifyBatchSize: 3,
	}

	calls := 0
	idx.headFunc = func(context.Context) (uint64, error) {
		return 3, nil
	}
	idx.eventsFunc = func(context.Context, uint64, uint64) ([]store.Event, error) {
		calls++
		if calls == 1 {
			return []store.Event{
				{ChainID: 1, Contract: contract.Hex(), Account: "0xaaa", PreviousWeight: "1", NewWeight: "2", BlockNumber: 1, LogIndex: 0},
				{ChainID: 1, Contract: contract.Hex(), Account: "0xccc", PreviousWeight: "3", NewWeight: "4", BlockNumber: 3, LogIndex: 0},
			}, nil
		}
		return []store.Event{
			{ChainID: 1, Contract: contract.Hex(), Account: "0xaaa", PreviousWeight: "1", NewWeight: "2", BlockNumber: 1, LogIndex: 0},
			{ChainID: 1, Contract: contract.Hex(), Account: "0xbbb", PreviousWeight: "2", NewWeight: "3", BlockNumber: 2, LogIndex: 0},
			{ChainID: 1, Contract: contract.Hex(), Account: "0xccc", PreviousWeight: "3", NewWeight: "4", BlockNumber: 3, LogIndex: 0},
		}, nil
	}

	state, err := idx.loadProgress(ctx)
	if err != nil {
		t.Fatalf("load progress: %v", err)
	}
	if err := idx.syncOnce(ctx, &state); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	events, err := eventStore.ListEvents(ctx, store.ListOptions{
		ChainID:  1,
		Contract: contract,
	})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events after verification, got %d", len(events))
	}
	if events[1].BlockNumber != 2 || events[1].Account != "0xbbb" {
		t.Fatalf("expected recovered middle event at block 2, got %+v", events[1])
	}

	verifiedUntil, ok, err := eventStore.LastVerifiedBlock(ctx, 1, contract)
	if err != nil {
		t.Fatalf("last verified block: %v", err)
	}
	if !ok || verifiedUntil != 3 {
		t.Fatalf("expected verified block 3, got %d (ok=%t)", verifiedUntil, ok)
	}
}

func TestSyncOnceTailRescanRecoversLateEvent(t *testing.T) {
	ctx := context.Background()
	database, err := metadb.New(db.TypeInMem, "")
	if err != nil {
		t.Fatalf("create in-memory db: %v", err)
	}
	defer func() {
		if cerr := database.Close(); cerr != nil {
			t.Fatalf("close db: %v", cerr)
		}
	}()
	eventStore := store.New(database)

	contract := common.HexToAddress("0x3434343434343434343434343434343434343434")
	if err := eventStore.SaveEvents(ctx, 1, contract, []store.Event{
		{ChainID: 1, Contract: contract.Hex(), Account: "0xaaa", PreviousWeight: "1", NewWeight: "2", BlockNumber: 8, LogIndex: 0},
		{ChainID: 1, Contract: contract.Hex(), Account: "0xccc", PreviousWeight: "3", NewWeight: "4", BlockNumber: 10, LogIndex: 0},
	}, 10); err != nil {
		t.Fatalf("save initial events: %v", err)
	}

	idx := &Indexer{
		store:           eventStore,
		chainID:         1,
		contract:        contract,
		startBlock:      8,
		batchSize:       2,
		verifyBatchSize: 3,
		tailRescanDepth: 3,
	}
	idx.headFunc = func(context.Context) (uint64, error) {
		return 10, nil
	}
	idx.eventsFunc = func(context.Context, uint64, uint64) ([]store.Event, error) {
		return []store.Event{
			{ChainID: 1, Contract: contract.Hex(), Account: "0xaaa", PreviousWeight: "1", NewWeight: "2", BlockNumber: 8, LogIndex: 0},
			{ChainID: 1, Contract: contract.Hex(), Account: "0xbbb", PreviousWeight: "2", NewWeight: "3", BlockNumber: 9, LogIndex: 0},
			{ChainID: 1, Contract: contract.Hex(), Account: "0xccc", PreviousWeight: "3", NewWeight: "4", BlockNumber: 10, LogIndex: 0},
		}, nil
	}

	state, err := idx.loadProgress(ctx)
	if err != nil {
		t.Fatalf("load progress: %v", err)
	}
	if err := idx.syncOnce(ctx, &state); err != nil {
		t.Fatalf("sync once: %v", err)
	}

	events, err := eventStore.ListEvents(ctx, store.ListOptions{
		ChainID:  1,
		Contract: contract,
	})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events after tail rescan, got %d", len(events))
	}
	if events[1].BlockNumber != 9 || events[1].Account != "0xbbb" {
		t.Fatalf("expected recovered tail event at block 9, got %+v", events[1])
	}
}
