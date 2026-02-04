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
		"corsAllowedOrigins", strings.Join(cfg.HTTP.CORSAllowedOrigins, ","),
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

	autoRPC := len(cfg.RPCs) == 0
	var pool *rpc.Web3Pool
	if autoRPC {
		var err error
		pool, err = rpc.NewAutomaticWeb3Pool()
		if err != nil {
			log.Fatalf("create automatic rpc pool: %v", err)
		}
	} else {
		pool = rpc.NewWeb3Pool()
		for _, endpoint := range cfg.RPCs {
			if _, err := pool.AddEndpoint(endpoint); err != nil {
				log.Fatalf("add RPC endpoint %s: %v", endpoint, err)
			}
		}
	}

	indexerService, err := indexer.NewService(indexer.ServiceConfig{
		Pool:                 pool,
		Store:                eventStore,
		PollInterval:         cfg.Indexer.PollInterval,
		BatchSize:            cfg.Indexer.BatchSize,
		ContractSyncInterval: cfg.Indexer.PollInterval,
		AutoRPC:              autoRPC,
		AutoRPCMaxEndpoints:  3,
	})
	if err != nil {
		log.Fatalf("create indexer service: %v", err)
	}
	apiService, err := api.New(eventStore, pool)
	if err != nil {
		log.Fatalf("create api service: %v", err)
	}

	seeded := 0
	for _, spec := range cfg.Contracts {
		if err := eventStore.SaveContract(context.Background(), spec.ChainID, spec.Address, spec.StartBlock); err != nil {
			log.Errorf("store contract %s: %v", spec.Address.Hex(), err)
			continue
		}
		seeded++
	}
	if seeded == 0 {
		log.Infow("no contracts provided on startup; waiting for /contracts registration")
	}

	if err := apiService.SyncFromStore(context.Background()); err != nil {
		log.Fatalf("sync api contracts: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	indexerErr := indexerService.Start(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- apiService.Start(ctx, cfg.HTTP.ListenAddr, cfg.HTTP.ListenPort, cfg.HTTP.CORSAllowedOrigins)
	}()

	log.Infow("http server started",
		"addr", cfg.HTTP.ListenAddr,
		"port", cfg.HTTP.ListenPort,
		"graphql", "/{chainID}/{contract}/graphql",
		"healthz", "/healthz")

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
