package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/graphql-go/handler"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/graphqlapi"
	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

// Service exposes the GraphQL API for indexed contracts.
type Service struct {
	store             *store.Store
	chainHeadResolver chainHeadResolver
	mu                sync.RWMutex
	handlers          map[string]*handler.Handler
	contracts         []indexer.ContractInfo
}

type chainHeadResolver interface {
	HeadBlock(ctx context.Context, chainID uint64) (uint64, error)
}

type rpcChainHeadResolver struct {
	pool *rpc.Web3Pool
}

func (r *rpcChainHeadResolver) HeadBlock(ctx context.Context, chainID uint64) (uint64, error) {
	if r.pool == nil {
		return 0, fmt.Errorf("rpc pool is required")
	}
	client, err := r.pool.Client(chainID)
	if err != nil {
		return 0, err
	}
	return client.BlockNumber(ctx)
}

// New creates a new API service.
func New(eventStore *store.Store, pool *rpc.Web3Pool) (*Service, error) {
	if eventStore == nil {
		return nil, fmt.Errorf("store is required")
	}
	var resolver chainHeadResolver
	if pool != nil {
		resolver = &rpcChainHeadResolver{pool: pool}
	}
	return &Service{
		store:             eventStore,
		chainHeadResolver: resolver,
		handlers:          make(map[string]*handler.Handler),
	}, nil
}

// RegisterContract registers a contract endpoint.
func (s *Service) RegisterContract(info indexer.ContractInfo) error {
	if info.ChainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if info.Address == (common.Address{}) {
		return fmt.Errorf("contract is required")
	}
	schema, err := graphqlapi.NewSchema(s.store, info.ChainID, info.Address)
	if err != nil {
		return fmt.Errorf("create graphql schema: %w", err)
	}
	key := info.Key()

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
	s.contracts = append(s.contracts, info)
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
		if err := s.RegisterContract(indexer.ContractInfo{
			ChainID:    record.ChainID,
			Address:    common.HexToAddress(record.Contract),
			StartBlock: record.StartBlock,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Start runs the HTTP server until the context is canceled.
func (s *Service) Start(ctx context.Context, addr string, port int, allowedOrigins []string) error {
	if err := s.SyncFromStore(ctx); err != nil {
		return err
	}
	server := &http.Server{
		Addr:    net.JoinHostPort(addr, fmt.Sprint(port)),
		Handler: withCORS(s.routes(), allowedOrigins),
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

func withCORS(next http.Handler, allowedOrigins []string) http.Handler {
	origins := normalizeAllowedOrigins(allowedOrigins)
	allowAll := len(origins) == 1 && origins[0] == "*"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		isPreflight := r.Method == http.MethodOptions && strings.TrimSpace(r.Header.Get("Access-Control-Request-Method")) != ""
		allowedOrigin := ""
		if allowAll {
			allowedOrigin = "*"
		} else {
			for _, allowed := range origins {
				if strings.EqualFold(allowed, origin) {
					allowedOrigin = origin
					break
				}
			}
		}

		if allowedOrigin == "" {
			if isPreflight {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		if !allowAll {
			w.Header().Add("Vary", "Origin")
		}

		if isPreflight {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			requestHeaders := strings.TrimSpace(r.Header.Get("Access-Control-Request-Headers"))
			if requestHeaders == "" {
				requestHeaders = "Content-Type, Authorization"
			}
			w.Header().Set("Access-Control-Allow-Headers", requestHeaders)
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func normalizeAllowedOrigins(values []string) []string {
	if len(values) == 0 {
		return []string{"*"}
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		for _, entry := range splitList(value) {
			if entry == "*" {
				return []string{"*"}
			}
			key := strings.ToLower(entry)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, entry)
		}
	}
	if len(out) == 0 {
		return []string{"*"}
	}
	return out
}

func splitList(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
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
	if err := s.RegisterContract(indexer.ContractInfo{
		ChainID:    req.ChainID,
		Address:    contractAddr,
		StartBlock: req.StartBlock,
	}); err != nil {
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
		w.Header().Set("Content-Type", "application/json")
		type APIInfo struct {
			indexer.ContractInfo `json:"info"`
			Endpoint             string `json:"endpoint"`
		}
		var apiInfo []APIInfo
		for _, spec := range s.contractsWithSyncStatus(r.Context()) {
			apiInfo = append(apiInfo, APIInfo{
				ContractInfo: spec,
				Endpoint:     fmt.Sprintf("/%d/%s/graphql", spec.ChainID, spec.Address.Hex()),
			})
		}
		if err := json.NewEncoder(w).Encode(apiInfo); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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

func (s *Service) contractsWithSyncStatus(ctx context.Context) []indexer.ContractInfo {
	contracts := s.sortedContracts()
	if len(contracts) == 0 {
		return contracts
	}
	records, err := s.store.ListContracts(ctx)
	if err == nil {
		startBlocks := make(map[string]uint64, len(records))
		for _, record := range records {
			if !common.IsHexAddress(record.Contract) {
				continue
			}
			info := indexer.ContractInfo{
				ChainID: record.ChainID,
				Address: common.HexToAddress(record.Contract),
			}
			startBlocks[info.Key()] = record.StartBlock
		}
		for i := range contracts {
			if startBlock, ok := startBlocks[contracts[i].Key()]; ok {
				contracts[i].StartBlock = startBlock
			}
		}
	}
	type chainHead struct {
		head   uint64
		err    error
		loaded bool
	}
	heads := make(map[uint64]chainHead, len(contracts))
	for i := range contracts {
		lastBlock, ok, err := s.store.LastIndexedBlock(ctx, contracts[i].ChainID, contracts[i].Address)
		if err != nil || !ok {
			contracts[i].Synced = false
			continue
		}

		head := heads[contracts[i].ChainID]
		if !head.loaded {
			head.loaded = true
			if s.chainHeadResolver == nil {
				head.err = fmt.Errorf("chain head resolver unavailable")
			} else {
				queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				head.head, head.err = s.chainHeadResolver.HeadBlock(queryCtx, contracts[i].ChainID)
				cancel()
			}
			heads[contracts[i].ChainID] = head
		}

		contracts[i].Synced = head.err == nil && lastBlock >= head.head
	}
	return contracts
}
