package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/web3/rpc"
	"github.com/vocdoni/davinci-node/web3/rpc/chainlist"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

// ServiceConfig configures the indexer service.
type ServiceConfig struct {
	Pool                 *rpc.Web3Pool
	Store                *store.Store
	PollInterval         time.Duration
	BatchSize            uint64
	ContractSyncInterval time.Duration
	AutoRPC              bool
	AutoRPCMaxEndpoints  int
}

// ContractInfo defines a contract indexing target.
type ContractInfo struct {
	ChainID    uint64         `json:"chainId"`
	Address    common.Address `json:"address"`
	StartBlock uint64         `json:"startBlock"`
	ExpiresAt  time.Time      `json:"expiresAt"`
	Synced     bool           `json:"synced"`
}

// Key returns a unique key for the contract config.
func (c ContractInfo) Key() string {
	return fmt.Sprintf("%d:%s", c.ChainID, strings.ToLower(c.Address.Hex()))
}

type contractInfoJSON struct {
	ChainID    uint64     `json:"chainId"`
	Address    string     `json:"address"`
	StartBlock uint64     `json:"startBlock"`
	ExpiresAt  *time.Time `json:"expiresAt"`
}

// UnmarshalJSON parses contract config from JSON with hex address string.
func (c *ContractInfo) UnmarshalJSON(data []byte) error {
	var tmp contractInfoJSON
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if tmp.ChainID == 0 {
		return fmt.Errorf("chainId is required")
	}
	if !common.IsHexAddress(tmp.Address) {
		return fmt.Errorf("invalid contract address")
	}
	if tmp.ExpiresAt == nil {
		return fmt.Errorf("expiresAt is required")
	}
	c.ChainID = tmp.ChainID
	c.Address = common.HexToAddress(tmp.Address)
	c.StartBlock = tmp.StartBlock
	c.ExpiresAt = tmp.ExpiresAt.UTC()
	return nil
}

// IsExpiredAt reports whether the contract should be purged at the provided time.
func (c ContractInfo) IsExpiredAt(now time.Time) bool {
	return !c.ExpiresAt.After(now)
}

type managedIndexer struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Service manages multiple indexers.
type Service struct {
	pool                 *rpc.Web3Pool
	store                *store.Store
	pollInterval         time.Duration
	batchSize            uint64
	contractSyncInterval time.Duration
	autoRPC              bool
	autoRPCMaxEndpoints  int
	mu                   sync.Mutex
	indexers             map[string]*managedIndexer
}

// NewService creates a new indexer service.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("pool is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 2000
	}
	if cfg.ContractSyncInterval <= 0 {
		cfg.ContractSyncInterval = 10 * time.Second
	}
	if cfg.AutoRPCMaxEndpoints <= 0 {
		cfg.AutoRPCMaxEndpoints = 3
	}
	return &Service{
		pool:                 cfg.Pool,
		store:                cfg.Store,
		pollInterval:         cfg.PollInterval,
		batchSize:            cfg.BatchSize,
		contractSyncInterval: cfg.ContractSyncInterval,
		autoRPC:              cfg.AutoRPC,
		autoRPCMaxEndpoints:  cfg.AutoRPCMaxEndpoints,
		indexers:             make(map[string]*managedIndexer),
	}, nil
}

// Start launches all indexers and returns a channel with their errors.
func (s *Service) Start(ctx context.Context) <-chan error {
	errCh := make(chan error, 16)
	go s.run(ctx, errCh)
	return errCh
}

func (s *Service) run(ctx context.Context, errCh chan<- error) {
	s.syncContracts(ctx, errCh)
	ticker := time.NewTicker(s.contractSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.syncContracts(ctx, errCh)
		}
	}
}

func (s *Service) syncContracts(ctx context.Context, errCh chan<- error) {
	records, err := s.store.ListContracts(ctx)
	if err != nil {
		s.sendErr(errCh, fmt.Errorf("list contracts: %w", err))
		return
	}
	now := time.Now().UTC()
	activeKeys := make(map[string]struct{}, len(records))
	purgedAny := false
	for _, record := range records {
		if !common.IsHexAddress(record.Contract) {
			continue
		}
		cfg := ContractInfo{
			ChainID:    record.ChainID,
			Address:    common.HexToAddress(record.Contract),
			StartBlock: record.StartBlock,
			ExpiresAt:  record.ExpiresAt,
		}
		if cfg.IsExpiredAt(now) {
			if err := s.purgeContract(ctx, cfg); err != nil {
				s.sendErr(errCh, err)
			} else {
				purgedAny = true
			}
			continue
		}
		activeKeys[cfg.Key()] = struct{}{}
		if err := s.ensureRegistered(ctx, cfg, errCh); err != nil {
			s.sendErr(errCh, err)
		}
	}
	if err := s.stopInactiveIndexers(ctx, activeKeys); err != nil {
		s.sendErr(errCh, err)
	}
	if purgedAny {
		if err := s.store.Compact(ctx); err != nil {
			s.sendErr(errCh, fmt.Errorf("compact store after purge: %w", err))
		}
	}
}

func (s *Service) ensureRegistered(ctx context.Context, cfg ContractInfo, errCh chan<- error) error {
	if cfg.ChainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if cfg.Address == (common.Address{}) {
		return fmt.Errorf("contract is required")
	}
	key := contractKey(cfg.ChainID, cfg.Address)

	s.mu.Lock()
	if _, exists := s.indexers[key]; exists {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	if err := s.ensureEndpoints(ctx, cfg.ChainID); err != nil {
		return err
	}

	client, err := s.pool.Client(cfg.ChainID)
	if err != nil {
		return fmt.Errorf("create web3 client for chainID %d: %w", cfg.ChainID, err)
	}
	if cfg.StartBlock == 0 {
		head, err := client.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("fetch head block for chainID %d: %w", cfg.ChainID, err)
		}
		startBlock, err := creationBlockInRange(ctx, client, cfg.Address, 0, head)
		if err != nil {
			return fmt.Errorf("find creation block for chainID %d contract %s: %w", cfg.ChainID, cfg.Address.Hex(), err)
		}
		cfg.StartBlock = startBlock
		if err := s.store.SetContractStartBlock(ctx, cfg.ChainID, cfg.Address, cfg.StartBlock); err != nil {
			return fmt.Errorf("persist start block for chainID %d contract %s: %w", cfg.ChainID, cfg.Address.Hex(), err)
		}
		log.Infow("calculated and persisted contract start block",
			"chainID", cfg.ChainID,
			"contract", cfg.Address.Hex(),
			"startBlock", cfg.StartBlock,
		)
	}
	idx, err := New(Config{
		Client:       client,
		Store:        s.store,
		ChainID:      cfg.ChainID,
		Contract:     cfg.Address,
		StartBlock:   cfg.StartBlock,
		PollInterval: s.pollInterval,
		BatchSize:    s.batchSize,
	})
	if err != nil {
		return fmt.Errorf("create indexer: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	entry := &managedIndexer{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	s.mu.Lock()
	if _, exists := s.indexers[key]; exists {
		s.mu.Unlock()
		cancel()
		close(entry.done)
		return nil
	}
	s.indexers[key] = entry
	s.mu.Unlock()

	go func(indexerInstance *Indexer, runEntry *managedIndexer, runKey string) {
		defer close(runEntry.done)
		err := indexerInstance.Run(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.sendErr(errCh, err)
		}
		s.mu.Lock()
		current, exists := s.indexers[runKey]
		if exists && current == runEntry {
			delete(s.indexers, runKey)
		}
		s.mu.Unlock()
	}(idx, entry, key)

	return nil
}

func (s *Service) purgeContract(ctx context.Context, cfg ContractInfo) error {
	key := contractKey(cfg.ChainID, cfg.Address)
	if err := s.stopIndexer(ctx, key); err != nil {
		return fmt.Errorf("stop indexer for chainID %d contract %s: %w", cfg.ChainID, cfg.Address.Hex(), err)
	}
	if err := s.store.DeleteContractData(ctx, cfg.ChainID, cfg.Address); err != nil {
		return fmt.Errorf("purge contract data for chainID %d contract %s: %w", cfg.ChainID, cfg.Address.Hex(), err)
	}
	log.Infow("purged expired contract",
		"chainID", cfg.ChainID,
		"contract", cfg.Address.Hex(),
		"expiresAt", cfg.ExpiresAt,
	)
	return nil
}

func (s *Service) stopInactiveIndexers(ctx context.Context, activeKeys map[string]struct{}) error {
	s.mu.Lock()
	staleKeys := make([]string, 0, len(s.indexers))
	for key := range s.indexers {
		if _, ok := activeKeys[key]; ok {
			continue
		}
		staleKeys = append(staleKeys, key)
	}
	s.mu.Unlock()

	for _, key := range staleKeys {
		if err := s.stopIndexer(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) stopIndexer(ctx context.Context, key string) error {
	s.mu.Lock()
	entry, exists := s.indexers[key]
	if exists {
		delete(s.indexers, key)
	}
	s.mu.Unlock()
	if !exists {
		return nil
	}

	entry.cancel()
	select {
	case <-entry.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) ensureEndpoints(ctx context.Context, chainID uint64) error {
	if s.pool.NumberOfEndpoints(chainID, false) > 0 {
		return nil
	}
	if !s.autoRPC {
		return fmt.Errorf("no RPC endpoints configured for chainID %d", chainID)
	}
	count, err := addChainlistEndpoints(chainID, s.autoRPCMaxEndpoints, s.pool)
	if err != nil {
		return err
	}
	log.Infow("rpc endpoints ready", "chainID", chainID, "count", count)
	return nil
}

func (s *Service) sendErr(errCh chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}

func addChainlistEndpoints(chainID uint64, maxEndpoints int, pool *rpc.Web3Pool) (int, error) {
	chainMap, err := chainlist.ChainList()
	if err != nil {
		return 0, fmt.Errorf("load chainlist: %w", err)
	}
	var shortName string
	for name, id := range chainMap {
		if id == chainID {
			shortName = name
			break
		}
	}
	if shortName == "" {
		return 0, fmt.Errorf("chainID %d not found in chainlist", chainID)
	}
	endpoints, err := chainlist.EndpointList(shortName, maxEndpoints)
	if err != nil {
		return 0, fmt.Errorf("chainlist endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return 0, fmt.Errorf("no healthy endpoints found for chainID %d", chainID)
	}
	added := 0
	for _, endpoint := range endpoints {
		if _, err := pool.AddEndpoint(endpoint); err != nil {
			continue
		}
		added++
	}
	if added == 0 {
		return 0, fmt.Errorf("failed to add endpoints for chainID %d", chainID)
	}
	return added, nil
}

func contractKey(chainID uint64, contract common.Address) string {
	return fmt.Sprintf("%d:%s", chainID, strings.ToLower(contract.Hex()))
}
