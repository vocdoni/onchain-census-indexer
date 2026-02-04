package store

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
)

func TestStoreListEvents(t *testing.T) {
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
	store := New(database)

	primaryContract := common.HexToAddress("0x1111111111111111111111111111111111111111")
	events := []Event{
		{ChainID: 1, Contract: primaryContract.Hex(), Account: "0xabc", PreviousWeight: "1", NewWeight: "2", BlockNumber: 3, LogIndex: 0},
		{ChainID: 1, Contract: primaryContract.Hex(), Account: "0xdef", PreviousWeight: "2", NewWeight: "3", BlockNumber: 1, LogIndex: 0},
		{ChainID: 1, Contract: primaryContract.Hex(), Account: "0x123", PreviousWeight: "3", NewWeight: "4", BlockNumber: 2, LogIndex: 1},
		{ChainID: 2, Contract: "0x2222222222222222222222222222222222222222", Account: "0x999", PreviousWeight: "4", NewWeight: "5", BlockNumber: 1, LogIndex: 0},
	}
	if err := store.SaveEvents(ctx, 1, primaryContract, events[:3], 3); err != nil {
		t.Fatalf("save events: %v", err)
	}
	if err := store.SaveEvents(ctx, 2, common.HexToAddress("0x2222222222222222222222222222222222222222"), events[3:], 1); err != nil {
		t.Fatalf("save events: %v", err)
	}

	lastBlock, ok, err := store.LastIndexedBlock(ctx, 1, primaryContract)
	if err != nil {
		t.Fatalf("last indexed block: %v", err)
	}
	if !ok {
		t.Fatalf("expected last indexed block")
	}
	if lastBlock != 3 {
		t.Fatalf("expected last block 3, got %d", lastBlock)
	}

	tests := []struct {
		name           string
		opts           ListOptions
		wantBlockOrder []uint64
	}{
		{
			name:           "asc_first_two",
			opts:           ListOptions{First: 2, Skip: 0, OrderBy: "blockNumber", OrderDirection: "asc", ChainID: 1, Contract: primaryContract},
			wantBlockOrder: []uint64{1, 2},
		},
		{
			name:           "asc_skip_one",
			opts:           ListOptions{First: 1, Skip: 1, OrderBy: "blockNumber", OrderDirection: "asc", ChainID: 1, Contract: primaryContract},
			wantBlockOrder: []uint64{2},
		},
		{
			name:           "desc_first_one",
			opts:           ListOptions{First: 1, Skip: 0, OrderBy: "blockNumber", OrderDirection: "desc", ChainID: 1, Contract: primaryContract},
			wantBlockOrder: []uint64{3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.ListEvents(ctx, tt.opts)
			if err != nil {
				t.Fatalf("list events: %v", err)
			}
			if len(got) != len(tt.wantBlockOrder) {
				t.Fatalf("expected %d events, got %d", len(tt.wantBlockOrder), len(got))
			}
			for i, wantBlock := range tt.wantBlockOrder {
				if got[i].BlockNumber != wantBlock {
					t.Fatalf("expected block %d at index %d, got %d", wantBlock, i, got[i].BlockNumber)
				}
			}
		})
	}
}

func TestSetContractStartBlock(t *testing.T) {
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
	eventStore := New(database)

	contractZero := common.HexToAddress("0x3333333333333333333333333333333333333333")
	contractFixed := common.HexToAddress("0x4444444444444444444444444444444444444444")
	if err := eventStore.SaveContract(ctx, 10, contractZero, 0); err != nil {
		t.Fatalf("save contract with zero start block: %v", err)
	}
	if err := eventStore.SaveContract(ctx, 11, contractFixed, 42); err != nil {
		t.Fatalf("save contract with fixed start block: %v", err)
	}

	if err := eventStore.SetContractStartBlock(ctx, 10, contractZero, 12345); err != nil {
		t.Fatalf("set start block: %v", err)
	}
	if err := eventStore.SetContractStartBlock(ctx, 11, contractFixed, 99999); err != nil {
		t.Fatalf("set start block for fixed contract: %v", err)
	}

	records, err := eventStore.ListContracts(ctx)
	if err != nil {
		t.Fatalf("list contracts: %v", err)
	}

	byChain := make(map[uint64]ContractRecord, len(records))
	for _, record := range records {
		byChain[record.ChainID] = record
	}
	if got := byChain[10].StartBlock; got != 12345 {
		t.Fatalf("expected updated start block 12345 for chain 10, got %d", got)
	}
	if got := byChain[11].StartBlock; got != 42 {
		t.Fatalf("expected unchanged start block 42 for chain 11, got %d", got)
	}
}
