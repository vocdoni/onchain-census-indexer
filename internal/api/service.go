package api

import (
	"context"
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
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

type contractSpec struct {
	chainID uint64
	address common.Address
}

func (c contractSpec) key() string {
	return fmt.Sprintf("%d:%s", c.chainID, strings.ToLower(c.address.Hex()))
}

// Service exposes the GraphQL API for indexed contracts.
type Service struct {
	store     *store.Store
	mu        sync.RWMutex
	handlers  map[string]*handler.Handler
	contracts []contractSpec
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
	spec := contractSpec{chainID: chainID, address: contract}
	key := spec.key()

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
	s.contracts = append(s.contracts, spec)
	return nil
}

// Start runs the HTTP server until the context is canceled.
func (s *Service) Start(ctx context.Context, addr string) error {
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
	mux.HandleFunc("/", s.handleRoot)
	return mux
}

func (s *Service) handleRoot(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "Available GraphQL endpoints:")
		for _, spec := range s.sortedContracts() {
			fmt.Fprintf(w, "- /%d/%s/graphql\n", spec.chainID, spec.address.Hex())
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

func (s *Service) sortedContracts() []contractSpec {
	s.mu.RLock()
	contracts := make([]contractSpec, len(s.contracts))
	copy(contracts, s.contracts)
	s.mu.RUnlock()

	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].chainID == contracts[j].chainID {
			return strings.ToLower(contracts[i].address.Hex()) < strings.ToLower(contracts[j].address.Hex())
		}
		return contracts[i].chainID < contracts[j].chainID
	})
	return contracts
}
