package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/graphql-go/handler"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"

	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

func TestWithCORSPreflightAllowedOrigin(t *testing.T) {
	handler := withCORS(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatalf("preflight request should not reach wrapped handler")
	}), []string{"https://app.example.com"})

	req := httptest.NewRequest(http.MethodOptions, "/contracts", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, X-Client-Version")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d, got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("expected allowed origin header to echo request origin, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type, X-Client-Version" {
		t.Fatalf("expected allow headers to mirror preflight request, got %q", got)
	}
}

func TestWithCORSPreflightDisallowedOrigin(t *testing.T) {
	handler := withCORS(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatalf("disallowed preflight should not reach wrapped handler")
	}), []string{"https://allowed.example.com"})

	req := httptest.NewRequest(http.MethodOptions, "/contracts", nil)
	req.Header.Set("Origin", "https://blocked.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d", http.StatusForbidden, rec.Code)
	}
}

func TestWithCORSWildcardAllowsAnyOrigin(t *testing.T) {
	called := false
	handler := withCORS(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://any-origin.example.com")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("expected wrapped handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected wildcard allow origin, got %q", got)
	}
}

type stubHeadResolver struct {
	heads map[uint64]uint64
}

func (s stubHeadResolver) HeadBlock(_ context.Context, chainID uint64) (uint64, error) {
	return s.heads[chainID], nil
}

func TestHandleRootIncludesSyncedStatus(t *testing.T) {
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

	contractSynced := common.HexToAddress("0x1111111111111111111111111111111111111111")
	contractUnsynced := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if err := eventStore.SaveEvents(ctx, 1, contractSynced, nil, 100); err != nil {
		t.Fatalf("save synced contract block: %v", err)
	}
	if err := eventStore.SaveEvents(ctx, 2, contractUnsynced, nil, 40); err != nil {
		t.Fatalf("save unsynced contract block: %v", err)
	}

	svc := &Service{
		store:             eventStore,
		chainHeadResolver: stubHeadResolver{heads: map[uint64]uint64{1: 100, 2: 41}},
		handlers:          make(map[string]*handler.Handler),
		contracts: []indexer.ContractInfo{
			{ChainID: 1, Address: contractSynced, StartBlock: 1},
			{ChainID: 2, Address: contractUnsynced, StartBlock: 1},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	svc.handleRoot(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}

	type apiInfo struct {
		Info struct {
			ChainID uint64 `json:"chainId"`
			Synced  bool   `json:"synced"`
		} `json:"info"`
	}
	var body []apiInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(body) != 2 {
		t.Fatalf("expected 2 contracts, got %d", len(body))
	}

	got := map[uint64]bool{}
	for _, item := range body {
		got[item.Info.ChainID] = item.Info.Synced
	}
	if !got[1] {
		t.Fatalf("expected chain 1 to be synced")
	}
	if got[2] {
		t.Fatalf("expected chain 2 to be unsynced")
	}
}

func TestContractsWithSyncStatusRefreshesStartBlockFromStore(t *testing.T) {
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

	contract := common.HexToAddress("0x5555555555555555555555555555555555555555")
	if err := eventStore.SaveContract(ctx, 1, contract, 0); err != nil {
		t.Fatalf("save contract with zero start block: %v", err)
	}
	if err := eventStore.SetContractStartBlock(ctx, 1, contract, 12345); err != nil {
		t.Fatalf("set contract start block: %v", err)
	}

	svc := &Service{
		store:    eventStore,
		handlers: make(map[string]*handler.Handler),
		contracts: []indexer.ContractInfo{
			{ChainID: 1, Address: contract, StartBlock: 0},
		},
	}

	contracts := svc.contractsWithSyncStatus(ctx)
	if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contracts))
	}
	if contracts[0].StartBlock != 12345 {
		t.Fatalf("expected refreshed start block 12345, got %d", contracts[0].StartBlock)
	}
}
