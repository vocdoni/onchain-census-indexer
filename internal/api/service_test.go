package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/graphql-go/handler"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"

	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

func futureTime(offset time.Duration) time.Time {
	return time.Now().UTC().Add(offset).Truncate(time.Second)
}

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
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 1, contractSynced, 1, expiresAt); err != nil {
		t.Fatalf("save synced contract: %v", err)
	}
	if err := eventStore.SaveContract(ctx, 2, contractUnsynced, 1, expiresAt); err != nil {
		t.Fatalf("save unsynced contract: %v", err)
	}
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
		JSONEndpoint string `json:"jsonEndpoint"`
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
		if item.JSONEndpoint == "" {
			t.Fatalf("expected jsonEndpoint to be populated for chain %d", item.Info.ChainID)
		}
		got[item.Info.ChainID] = item.Info.Synced
	}
	if !got[1] {
		t.Fatalf("expected chain 1 to be synced")
	}
	if got[2] {
		t.Fatalf("expected chain 2 to be unsynced")
	}
}

func TestContractsWithSyncStatusUsesVerifiedBlockAndConfirmations(t *testing.T) {
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

	contract := common.HexToAddress("0x9898989898989898989898989898989898989898")
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 1, contract, 1, expiresAt); err != nil {
		t.Fatalf("save contract: %v", err)
	}
	if err := eventStore.SetIndexedBlock(ctx, 1, contract, 100); err != nil {
		t.Fatalf("set indexed block: %v", err)
	}
	if err := eventStore.SetVerifiedBlock(ctx, 1, contract, 90); err != nil {
		t.Fatalf("set verified block: %v", err)
	}

	svc := &Service{
		store:             eventStore,
		chainHeadResolver: stubHeadResolver{heads: map[uint64]uint64{1: 100}},
		syncConfirmations: 10,
		handlers:          make(map[string]*handler.Handler),
		contracts: []indexer.ContractInfo{
			{ChainID: 1, Address: contract, StartBlock: 1},
		},
	}

	contracts := svc.contractsWithSyncStatus(ctx)
	if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(contracts))
	}
	if !contracts[0].Synced {
		t.Fatalf("expected contract to be synced when verified block satisfies safe head")
	}

	if err := eventStore.SetVerifiedBlock(ctx, 1, contract, 89); err != nil {
		t.Fatalf("set verified block below safe head: %v", err)
	}
	contracts = svc.contractsWithSyncStatus(ctx)
	if len(contracts) != 1 {
		t.Fatalf("expected 1 contract after lowering verified block, got %d", len(contracts))
	}
	if contracts[0].Synced {
		t.Fatalf("expected contract to be unsynced when verified block is below safe head")
	}
}

func TestHandleRootServesContractJSON(t *testing.T) {
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

	contract := common.HexToAddress("0x3333333333333333333333333333333333333333")
	accountA := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	accountB := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 1, contract, 1, expiresAt); err != nil {
		t.Fatalf("save contract: %v", err)
	}
	if err := eventStore.SaveEvents(ctx, 1, contract, []store.Event{
		{
			ChainID:        1,
			Contract:       contract.Hex(),
			Account:        accountA.Hex(),
			PreviousWeight: "1",
			NewWeight:      "2",
			BlockNumber:    10,
			LogIndex:       0,
		},
		{
			ChainID:        1,
			Contract:       contract.Hex(),
			Account:        accountB.Hex(),
			PreviousWeight: "2",
			NewWeight:      "5",
			BlockNumber:    11,
			LogIndex:       0,
		},
	}, 11); err != nil {
		t.Fatalf("save events: %v", err)
	}

	svc, err := New(eventStore, nil, 0)
	if err != nil {
		t.Fatalf("create api service: %v", err)
	}
	if err := svc.SyncFromStore(ctx); err != nil {
		t.Fatalf("sync from store: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/1/%s?first=1&skip=1&orderDirection=desc", contract.Hex()),
		nil,
	)
	rec := httptest.NewRecorder()
	svc.handleRoot(rec, req.WithContext(ctx))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (body=%s)", http.StatusOK, rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected application/json content type, got %q", got)
	}

	var body weightChangeEventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(body.WeightChangeEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(body.WeightChangeEvents))
	}
	if got := body.WeightChangeEvents[0]; got.Account.ID != accountA.Hex() || got.PreviousWeight != "1" || got.NewWeight != "2" || got.BlockNumber != "10" {
		t.Fatalf("unexpected event payload: %+v", got)
	}
}

func TestHandleRootContractJSONRejectsInvalidQuery(t *testing.T) {
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

	contract := common.HexToAddress("0x4444444444444444444444444444444444444444")
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 1, contract, 1, expiresAt); err != nil {
		t.Fatalf("save contract: %v", err)
	}

	svc, err := New(eventStore, nil, 0)
	if err != nil {
		t.Fatalf("create api service: %v", err)
	}
	if err := svc.SyncFromStore(ctx); err != nil {
		t.Fatalf("sync from store: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/1/%s?first=-1", contract.Hex()), nil)
	rec := httptest.NewRecorder()
	svc.handleRoot(rec, req.WithContext(ctx))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d (body=%s)", http.StatusBadRequest, rec.Code, rec.Body.String())
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
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 1, contract, 0, expiresAt); err != nil {
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

func TestSyncFromStorePrunesRemovedContracts(t *testing.T) {
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

	contractA := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	contractB := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	expiresAt := futureTime(24 * time.Hour)
	if err := eventStore.SaveContract(ctx, 1, contractA, 10, expiresAt); err != nil {
		t.Fatalf("save contract A: %v", err)
	}
	if err := eventStore.SaveContract(ctx, 1, contractB, 20, expiresAt); err != nil {
		t.Fatalf("save contract B: %v", err)
	}

	svc, err := New(eventStore, nil, 0)
	if err != nil {
		t.Fatalf("create api service: %v", err)
	}
	if err := svc.SyncFromStore(ctx); err != nil {
		t.Fatalf("sync from store: %v", err)
	}
	if len(svc.contracts) != 2 {
		t.Fatalf("expected 2 contracts after initial sync, got %d", len(svc.contracts))
	}

	if err := eventStore.DeleteContractData(ctx, 1, contractA); err != nil {
		t.Fatalf("delete contract A data: %v", err)
	}
	if err := svc.SyncFromStore(ctx); err != nil {
		t.Fatalf("sync from store after delete: %v", err)
	}

	if len(svc.contracts) != 1 {
		t.Fatalf("expected 1 contract after pruning, got %d", len(svc.contracts))
	}
	if got := strings.ToLower(svc.contracts[0].Address.Hex()); got != strings.ToLower(contractB.Hex()) {
		t.Fatalf("expected remaining contract %s, got %s", contractB.Hex(), got)
	}
	keyA := indexer.ContractInfo{ChainID: 1, Address: contractA}.Key()
	if _, ok := svc.handlers[keyA]; ok {
		t.Fatalf("expected handler for contract A to be removed")
	}
}

func TestHandleContractsRejectsExpiredContract(t *testing.T) {
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
	svc, err := New(eventStore, nil, 0)
	if err != nil {
		t.Fatalf("create api service: %v", err)
	}

	expiresAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	reqBody := `{"chainId":1,"address":"0x1111111111111111111111111111111111111111","startBlock":0,"expiresAt":"` + expiresAt + `"}`
	req := httptest.NewRequest(http.MethodPost, "/contracts", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	svc.handleContracts(rec, req.WithContext(ctx))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d (body=%s)", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
}

func TestHandleContractsRejectsMissingExpiresAt(t *testing.T) {
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
	svc, err := New(eventStore, nil, 0)
	if err != nil {
		t.Fatalf("create api service: %v", err)
	}

	reqBody := `{"chainId":1,"address":"0x1111111111111111111111111111111111111111","startBlock":0}`
	req := httptest.NewRequest(http.MethodPost, "/contracts", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	svc.handleContracts(rec, req.WithContext(ctx))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d (body=%s)", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
}
