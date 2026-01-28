package store

import (
	"context"
	"testing"

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

	events := []Event{
		{Account: "0xabc", PreviousWeight: "1", NewWeight: "2", BlockNumber: 3, LogIndex: 0},
		{Account: "0xdef", PreviousWeight: "2", NewWeight: "3", BlockNumber: 1, LogIndex: 0},
		{Account: "0x123", PreviousWeight: "3", NewWeight: "4", BlockNumber: 2, LogIndex: 1},
	}
	if err := store.SaveEvents(ctx, events, 3); err != nil {
		t.Fatalf("save events: %v", err)
	}

	lastBlock, ok, err := store.LastIndexedBlock(ctx)
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
			opts:           ListOptions{First: 2, Skip: 0, OrderBy: "blockNumber", OrderDirection: "asc"},
			wantBlockOrder: []uint64{1, 2},
		},
		{
			name:           "asc_skip_one",
			opts:           ListOptions{First: 1, Skip: 1, OrderBy: "blockNumber", OrderDirection: "asc"},
			wantBlockOrder: []uint64{2},
		},
		{
			name:           "desc_first_one",
			opts:           ListOptions{First: 1, Skip: 0, OrderBy: "blockNumber", OrderDirection: "desc"},
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
