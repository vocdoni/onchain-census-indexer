package store

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
)

func futureTime(offset time.Duration) time.Time {
	return time.Now().UTC().Add(offset).Truncate(time.Second)
}

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
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 10, contractZero, 0, expiresAt); err != nil {
		t.Fatalf("save contract with zero start block: %v", err)
	}
	if err := eventStore.SaveContract(ctx, 11, contractFixed, 42, expiresAt); err != nil {
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

func TestSaveContractUpdatesExpiresAt(t *testing.T) {
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

	contract := common.HexToAddress("0x5555555555555555555555555555555555555555")
	base := time.Now().UTC().Truncate(time.Second)
	initialExpiresAt := base.Add(24 * time.Hour).In(time.FixedZone("UTC+2", 2*60*60))
	if err := eventStore.SaveContract(ctx, 99, contract, 12, initialExpiresAt); err != nil {
		t.Fatalf("save contract with expiresAt: %v", err)
	}

	updatedExpiresAt := base.Add(48 * time.Hour)
	if err := eventStore.SaveContract(ctx, 99, contract, 999, updatedExpiresAt); err != nil {
		t.Fatalf("update contract expiresAt: %v", err)
	}

	records, err := eventStore.ListContracts(ctx)
	if err != nil {
		t.Fatalf("list contracts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(records))
	}

	record := records[0]
	if record.StartBlock != 12 {
		t.Fatalf("expected original start block 12 to be preserved, got %d", record.StartBlock)
	}
	if !record.ExpiresAt.Equal(updatedExpiresAt.UTC()) {
		t.Fatalf("expected expiresAt %s, got %s", updatedExpiresAt.UTC().Format(time.RFC3339), record.ExpiresAt.Format(time.RFC3339))
	}
}

func TestDeleteContractData(t *testing.T) {
	tests := []struct {
		name   string
		dbType string
		path   func(t *testing.T) string
	}{
		{
			name:   "inmemory",
			dbType: db.TypeInMem,
			path: func(*testing.T) string {
				return ""
			},
		},
		{
			name:   "pebble",
			dbType: db.TypePebble,
			path: func(t *testing.T) string {
				return t.TempDir()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := metadb.New(tt.dbType, tt.path(t))
			if err != nil {
				t.Fatalf("create %s db: %v", tt.dbType, err)
			}
			defer func() {
				if cerr := database.Close(); cerr != nil {
					t.Fatalf("close db: %v", cerr)
				}
			}()
			eventStore := New(database)

			primary := common.HexToAddress("0x6666666666666666666666666666666666666666")
			secondary := common.HexToAddress("0x7777777777777777777777777777777777777777")
			expiresAt := futureTime(24 * time.Hour)
			if err := eventStore.SaveContract(ctx, 1, primary, 100, expiresAt); err != nil {
				t.Fatalf("save primary contract: %v", err)
			}
			if err := eventStore.SaveContract(ctx, 1, secondary, 200, expiresAt); err != nil {
				t.Fatalf("save secondary contract: %v", err)
			}
			if err := eventStore.SaveEvents(ctx, 1, primary, []Event{
				{ChainID: 1, Contract: primary.Hex(), Account: "0xabc", PreviousWeight: "1", NewWeight: "2", BlockNumber: 101, LogIndex: 0},
			}, 101); err != nil {
				t.Fatalf("save primary events: %v", err)
			}
			if err := eventStore.SaveEvents(ctx, 1, secondary, []Event{
				{ChainID: 1, Contract: secondary.Hex(), Account: "0xdef", PreviousWeight: "2", NewWeight: "3", BlockNumber: 201, LogIndex: 0},
			}, 201); err != nil {
				t.Fatalf("save secondary events: %v", err)
			}

			if err := eventStore.DeleteContractData(ctx, 1, primary); err != nil {
				t.Fatalf("delete primary contract data: %v", err)
			}

			records, err := eventStore.ListContracts(ctx)
			if err != nil {
				t.Fatalf("list contracts: %v", err)
			}
			if len(records) != 1 || records[0].Contract != secondary.Hex() {
				t.Fatalf("expected only secondary contract to remain, got %+v", records)
			}

			if _, ok, err := eventStore.LastIndexedBlock(ctx, 1, primary); err != nil {
				t.Fatalf("last indexed block for primary: %v", err)
			} else if ok {
				t.Fatalf("expected no last indexed block for deleted contract")
			}
			if lastSecondary, ok, err := eventStore.LastIndexedBlock(ctx, 1, secondary); err != nil {
				t.Fatalf("last indexed block for secondary: %v", err)
			} else if !ok || lastSecondary != 201 {
				t.Fatalf("expected secondary last indexed block 201, got %d (ok=%t)", lastSecondary, ok)
			}

			primaryEvents, err := eventStore.ListEvents(ctx, ListOptions{
				ChainID:  1,
				Contract: primary,
			})
			if err != nil {
				t.Fatalf("list primary events: %v", err)
			}
			if len(primaryEvents) != 0 {
				t.Fatalf("expected no events for deleted contract, got %d", len(primaryEvents))
			}

			secondaryEvents, err := eventStore.ListEvents(ctx, ListOptions{
				ChainID:  1,
				Contract: secondary,
			})
			if err != nil {
				t.Fatalf("list secondary events: %v", err)
			}
			if len(secondaryEvents) != 1 {
				t.Fatalf("expected 1 event for secondary contract, got %d", len(secondaryEvents))
			}
		})
	}
}

func TestSaveContractRequiresExpiresAt(t *testing.T) {
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

	contract := common.HexToAddress("0x8888888888888888888888888888888888888888")
	if err := eventStore.SaveContract(ctx, 1, contract, 10, time.Time{}); err == nil {
		t.Fatalf("expected error when expiresAt is missing")
	}
}
