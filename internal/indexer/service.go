package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

// ServiceConfig configures the indexer service.
type ServiceConfig struct {
	Pool         *rpc.Web3Pool
	Store        *store.Store
	PollInterval time.Duration
	BatchSize    uint64
}

// ContractConfig defines a contract indexing target.
type ContractConfig struct {
	ChainID    uint64
	Contract   common.Address
	StartBlock uint64
}

// Service manages multiple indexers.
type Service struct {
	pool         *rpc.Web3Pool
	store        *store.Store
	pollInterval time.Duration
	batchSize    uint64
	indexers     []*Indexer
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
	return &Service{
		pool:         cfg.Pool,
		store:        cfg.Store,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
	}, nil
}

// RegisterContract registers a new contract for indexing.
func (s *Service) RegisterContract(cfg ContractConfig) error {
	if cfg.ChainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if cfg.Contract == (common.Address{}) {
		return fmt.Errorf("contract is required")
	}
	client, err := s.pool.Client(cfg.ChainID)
	if err != nil {
		return fmt.Errorf("create web3 client for chainID %d: %w", cfg.ChainID, err)
	}
	idx, err := New(Config{
		Client:       client,
		Store:        s.store,
		ChainID:      cfg.ChainID,
		Contract:     cfg.Contract,
		StartBlock:   cfg.StartBlock,
		PollInterval: s.pollInterval,
		BatchSize:    s.batchSize,
	})
	if err != nil {
		return fmt.Errorf("create indexer: %w", err)
	}
	s.indexers = append(s.indexers, idx)
	return nil
}

// Start launches all indexers and returns a channel with their errors.
func (s *Service) Start(ctx context.Context) <-chan error {
	errCh := make(chan error, len(s.indexers))
	if len(s.indexers) == 0 {
		errCh <- fmt.Errorf("no indexers registered")
		return errCh
	}
	for _, idx := range s.indexers {
		go func(indexerInstance *Indexer) {
			errCh <- indexerInstance.Run(ctx)
		}(idx)
	}
	return errCh
}
