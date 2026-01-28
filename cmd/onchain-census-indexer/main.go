package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/api"
	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

func main() {
	cfg, err := LoadConfig()
	logLevel := log.LogLevelDebug
	if err == nil && cfg.Log.Level != "" {
		logLevel = cfg.Log.Level
	}
	log.Init(logLevel, "stderr", nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Infow("starting onchain census indexer",
		"contracts", cfg.ContractsRaw,
		"dbPath", cfg.DB.Path,
		"listen", cfg.HTTP.ListenAddr,
		"pollInterval", cfg.Indexer.PollInterval.String(),
		"batchSize", cfg.Indexer.BatchSize,
		"rpcs", strings.Join(cfg.RPCs, ","),
	)

	database, err := metadb.New(db.TypePebble, cfg.DB.Path)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer func() {
		if cerr := database.Close(); cerr != nil {
			log.Warnf("close database: %v", cerr)
		}
	}()
	eventStore := store.New(database)

	pool := rpc.NewWeb3Pool()
	chainIDs := make(map[uint64]int)
	for _, endpoint := range cfg.RPCs {
		id, err := pool.AddEndpoint(endpoint)
		if err != nil {
			log.Fatalf("add RPC endpoint %s: %v", endpoint, err)
		}
		chainIDs[id]++
	}

	for chainID := range chainIDs {
		log.Infow("rpc endpoints ready", "chainID", chainID, "count", chainIDs[chainID])
	}

	indexerService, err := indexer.NewService(indexer.ServiceConfig{
		Pool:         pool,
		Store:        eventStore,
		PollInterval: cfg.Indexer.PollInterval,
		BatchSize:    cfg.Indexer.BatchSize,
	})
	if err != nil {
		log.Fatalf("create indexer service: %v", err)
	}
	apiService, err := api.New(eventStore)
	if err != nil {
		log.Fatalf("create api service: %v", err)
	}

	registered := 0
	for _, spec := range cfg.Contracts {
		if _, ok := chainIDs[spec.ChainID]; !ok {
			log.Errorf("no RPC endpoints configured for chainID %d", spec.ChainID)
			continue
		}
		if err := indexerService.RegisterContract(indexer.ContractConfig{
			ChainID:    spec.ChainID,
			Contract:   spec.Contract,
			StartBlock: spec.StartBlock,
		}); err != nil {
			log.Errorf("register indexer for chainID %d contract %s: %v", spec.ChainID, spec.Contract.Hex(), err)
			continue
		}
		if err := apiService.RegisterContract(spec.ChainID, spec.Contract); err != nil {
			log.Errorf("register api for chainID %d contract %s: %v", spec.ChainID, spec.Contract.Hex(), err)
			continue
		}
		registered++
	}
	if registered == 0 {
		log.Fatal("no contracts registered successfully; exiting")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	indexerErr := indexerService.Start(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- apiService.Start(ctx, cfg.HTTP.ListenAddr)
	}()

	log.Infow("http server started", "addr", cfg.HTTP.ListenAddr, "graphql", "/{chainID}/{contract}/graphql", "healthz", "/healthz")

	select {
	case <-ctx.Done():
	case err := <-indexerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("indexer stopped: %v", err)
		}
	case err := <-serverErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("http server stopped: %v", err)
		}
	}

	select {
	case err := <-indexerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("indexer stopped: %v", err)
		}
	default:
	}
}
