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
	if info.IsExpiredAt(time.Now().UTC()) {
		return fmt.Errorf("contract has expired")
	}
	if err := s.registerHandler(info); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.contracts {
		if s.contracts[i].Key() == info.Key() {
			s.contracts[i] = info
			return nil
		}
	}
	s.contracts = append(s.contracts, info)
	return nil
}

func (s *Service) registerHandler(info indexer.ContractInfo) error {
	key := info.Key()
	s.mu.RLock()
	_, exists := s.handlers[key]
	s.mu.RUnlock()
	if exists {
		return nil
	}

	schema, err := graphqlapi.NewSchema(s.store, info.ChainID, info.Address)
	if err != nil {
		return fmt.Errorf("create graphql schema: %w", err)
	}
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
	return nil
}

// SyncFromStore reconciles GraphQL handlers with current non-expired contracts in the store.
func (s *Service) SyncFromStore(ctx context.Context) error {
	records, err := s.store.ListContracts(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	desired := make(map[string]indexer.ContractInfo, len(records))
	for _, record := range records {
		if !common.IsHexAddress(record.Contract) {
			continue
		}
		info := indexer.ContractInfo{
			ChainID:    record.ChainID,
			Address:    common.HexToAddress(record.Contract),
			StartBlock: record.StartBlock,
			ExpiresAt:  record.ExpiresAt,
		}
		if info.IsExpiredAt(now) {
			continue
		}
		desired[info.Key()] = info
	}

	for _, info := range desired {
		if err := s.registerHandler(info); err != nil {
			return err
		}
	}

	s.mu.Lock()
	for key := range s.handlers {
		if _, ok := desired[key]; ok {
			continue
		}
		delete(s.handlers, key)
	}
	contracts := make([]indexer.ContractInfo, 0, len(desired))
	for _, info := range desired {
		contracts = append(contracts, info)
	}
	s.contracts = contracts
	s.mu.Unlock()

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
	ChainID      uint64    `json:"chainId"`
	Contract     string    `json:"contract"`
	Endpoint     string    `json:"endpoint"`
	JSONEndpoint string    `json:"jsonEndpoint,omitempty"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

type weightChangeAccountResponse struct {
	ID string `json:"id"`
}

type weightChangeEventResponse struct {
	Account        weightChangeAccountResponse `json:"account"`
	PreviousWeight string                      `json:"previousWeight"`
	NewWeight      string                      `json:"newWeight"`
	BlockNumber    string                      `json:"blockNumber"`
}

type weightChangeEventsResponse struct {
	WeightChangeEvents []weightChangeEventResponse `json:"weightChangeEvents"`
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
	if req.IsExpiredAt(time.Now().UTC()) {
		http.Error(w, "expiresAt must be in the future", http.StatusBadRequest)
		return
	}
	contractAddr := req.Address
	if err := s.store.SaveContract(r.Context(), req.ChainID, req.Address, req.StartBlock, req.ExpiresAt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.SyncFromStore(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := registerResponse{
		ChainID:      req.ChainID,
		Contract:     contractAddr.Hex(),
		Endpoint:     fmt.Sprintf("/%d/%s/graphql", req.ChainID, contractAddr.Hex()),
		JSONEndpoint: fmt.Sprintf("/%d/%s", req.ChainID, contractAddr.Hex()),
		ExpiresAt:    req.ExpiresAt,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Service) handleRoot(w http.ResponseWriter, r *http.Request) {
	if err := s.SyncFromStore(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		w.Header().Set("Content-Type", "application/json")
		type APIInfo struct {
			indexer.ContractInfo `json:"info"`
			Endpoint             string `json:"endpoint"`
			JSONEndpoint         string `json:"jsonEndpoint"`
		}
		var apiInfo []APIInfo
		for _, spec := range s.contractsWithSyncStatus(r.Context()) {
			apiInfo = append(apiInfo, APIInfo{
				ContractInfo: spec,
				Endpoint:     fmt.Sprintf("/%d/%s/graphql", spec.ChainID, spec.Address.Hex()),
				JSONEndpoint: fmt.Sprintf("/%d/%s", spec.ChainID, spec.Address.Hex()),
			})
		}
		if err := json.NewEncoder(w).Encode(apiInfo); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) != 2 && (len(parts) != 3 || parts[2] != "graphql") {
		http.NotFound(w, r)
		return
	}
	chainID, contractAddr, key, ok := parseContractRoute(parts)
	if !ok {
		http.NotFound(w, r)
		return
	}

	s.mu.RLock()
	graphqlHandler, ok := s.handlers[key]
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 {
		s.handleContractJSON(w, r, chainID, contractAddr)
		return
	}
	graphqlHandler.ServeHTTP(w, r)
}

func parseContractRoute(parts []string) (uint64, common.Address, string, bool) {
	if len(parts) < 2 {
		return 0, common.Address{}, "", false
	}
	chainID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || chainID == 0 {
		return 0, common.Address{}, "", false
	}
	contract := strings.TrimSpace(parts[1])
	if !common.IsHexAddress(contract) {
		return 0, common.Address{}, "", false
	}
	contractAddr := common.HexToAddress(contract)
	return chainID, contractAddr, fmt.Sprintf("%d:%s", chainID, strings.ToLower(contractAddr.Hex())), true
}

func (s *Service) handleContractJSON(w http.ResponseWriter, r *http.Request, chainID uint64, contract common.Address) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	opts, err := listOptionsFromRequest(r, chainID, contract)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	events, err := s.store.ListEvents(r.Context(), opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := weightChangeEventsResponse{
		WeightChangeEvents: make([]weightChangeEventResponse, 0, len(events)),
	}
	for _, event := range events {
		resp.WeightChangeEvents = append(resp.WeightChangeEvents, weightChangeEventResponse{
			Account: weightChangeAccountResponse{
				ID: event.Account,
			},
			PreviousWeight: event.PreviousWeight,
			NewWeight:      event.NewWeight,
			BlockNumber:    strconv.FormatUint(event.BlockNumber, 10),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func listOptionsFromRequest(r *http.Request, chainID uint64, contract common.Address) (store.ListOptions, error) {
	first, err := parseOptionalNonNegativeInt(r.URL.Query().Get("first"), "first")
	if err != nil {
		return store.ListOptions{}, err
	}
	skip, err := parseOptionalNonNegativeInt(r.URL.Query().Get("skip"), "skip")
	if err != nil {
		return store.ListOptions{}, err
	}
	orderBy := strings.TrimSpace(r.URL.Query().Get("orderBy"))
	if orderBy != "" && orderBy != "blockNumber" {
		return store.ListOptions{}, fmt.Errorf("unsupported orderBy: %s", orderBy)
	}
	orderDirection := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("orderDirection")))
	if orderDirection != "" && orderDirection != "asc" && orderDirection != "desc" {
		return store.ListOptions{}, fmt.Errorf("unsupported orderDirection: %s", orderDirection)
	}
	return store.ListOptions{
		First:          first,
		Skip:           skip,
		OrderBy:        orderBy,
		OrderDirection: orderDirection,
		ChainID:        chainID,
		Contract:       contract,
	}, nil
}

func parseOptionalNonNegativeInt(raw, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
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
		now := time.Now().UTC()
		metadata := make(map[string]indexer.ContractInfo, len(records))
		for _, record := range records {
			if !common.IsHexAddress(record.Contract) {
				continue
			}
			info := indexer.ContractInfo{
				ChainID:    record.ChainID,
				Address:    common.HexToAddress(record.Contract),
				StartBlock: record.StartBlock,
				ExpiresAt:  record.ExpiresAt,
			}
			if info.IsExpiredAt(now) {
				continue
			}
			metadata[info.Key()] = info
		}
		filtered := contracts[:0]
		for i := range contracts {
			if info, ok := metadata[contracts[i].Key()]; ok {
				contracts[i].StartBlock = info.StartBlock
				contracts[i].ExpiresAt = info.ExpiresAt
				filtered = append(filtered, contracts[i])
			}
		}
		contracts = filtered
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
