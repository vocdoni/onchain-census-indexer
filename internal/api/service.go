package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/graphql-go/handler"

	"github.com/vocdoni/onchain-census-indexer/internal/graphqlapi"
	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

// Service exposes the GraphQL API for indexed contracts.
type Service struct {
	store     *store.Store
	mu        sync.RWMutex
	handlers  map[string]*handler.Handler
	contracts []indexer.ContractInfo
}

// New creates a new API service.
func New(eventStore *store.Store) (*Service, error) {
	if eventStore == nil {
		return nil, fmt.Errorf("store is required")
	}
	return &Service{
		store:    eventStore,
		handlers: make(map[string]*handler.Handler),
	}, nil
}

// RegisterContract registers a contract endpoint.
func (s *Service) RegisterContract(chainID uint64, contract common.Address) error {
	if chainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if contract == (common.Address{}) {
		return fmt.Errorf("contract is required")
	}
	schema, err := graphqlapi.NewSchema(s.store, chainID, contract)
	if err != nil {
		return fmt.Errorf("create graphql schema: %w", err)
	}
	contractConf := indexer.ContractInfo{ChainID: chainID, Address: contract}
	key := contractConf.Key()

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.handlers[key]; exists {
		return nil
	}
	s.handlers[key] = handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: true,
	})
	s.contracts = append(s.contracts, contractConf)
	return nil
}

// SyncFromStore registers GraphQL handlers for all contracts in the store.
func (s *Service) SyncFromStore(ctx context.Context) error {
	records, err := s.store.ListContracts(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if !common.IsHexAddress(record.Contract) {
			continue
		}
		if err := s.RegisterContract(record.ChainID, common.HexToAddress(record.Contract)); err != nil {
			return err
		}
	}
	return nil
}

// Start runs the HTTP server until the context is canceled.
func (s *Service) Start(ctx context.Context, addr string) error {
	if err := s.SyncFromStore(ctx); err != nil {
		return err
	}
	server := &http.Server{
		Addr:    addr,
		Handler: s.routes(),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	default:
	}
	return nil
}

func (s *Service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/contracts", s.handleContracts)
	mux.HandleFunc("/", s.handleRoot)
	return mux
}

type registerRequest = indexer.ContractInfo

type registerResponse struct {
	ChainID  uint64 `json:"chainId"`
	Contract string `json:"contract"`
	Endpoint string `json:"endpoint"`
}

func (s *Service) handleContracts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	contractAddr := req.Address
	if err := s.store.SaveContract(r.Context(), req.ChainID, req.Address, req.StartBlock); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.RegisterContract(req.ChainID, contractAddr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := registerResponse{
		ChainID:  req.ChainID,
		Contract: contractAddr.Hex(),
		Endpoint: fmt.Sprintf("/%d/%s/graphql", req.ChainID, contractAddr.Hex()),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Service) handleRoot(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "Available GraphQL endpoints:")
		for _, spec := range s.sortedContracts() {
			fmt.Fprintf(w, "- /%d/%s/graphql\n", spec.ChainID, spec.Address.Hex())
		}
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[2] != "graphql" {
		http.NotFound(w, r)
		return
	}
	chainID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || chainID == 0 {
		http.NotFound(w, r)
		return
	}
	contract := strings.ToLower(parts[1])
	if !common.IsHexAddress(contract) {
		http.NotFound(w, r)
		return
	}
	key := fmt.Sprintf("%d:%s", chainID, strings.ToLower(common.HexToAddress(contract).Hex()))

	s.mu.RLock()
	graphqlHandler, ok := s.handlers[key]
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	graphqlHandler.ServeHTTP(w, r)
}

func (s *Service) sortedContracts() []indexer.ContractInfo {
	s.mu.RLock()
	contracts := make([]indexer.ContractInfo, len(s.contracts))
	copy(contracts, s.contracts)
	s.mu.RUnlock()

	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].ChainID == contracts[j].ChainID {
			return strings.ToLower(contracts[i].Address.Hex()) < strings.ToLower(contracts[j].Address.Hex())
		}
		return contracts[i].ChainID < contracts[j].ChainID
	})
	return contracts
}
