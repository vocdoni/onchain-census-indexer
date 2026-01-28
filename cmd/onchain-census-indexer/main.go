package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/graphql-go/handler"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/graphqlapi"
	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

const weightChangedABIJSON = `[
    {
        "name": "WeightChanged",
        "type": "event",
        "inputs": [
            {
                "name": "account",
                "type": "address",
                "indexed": true,
                "internalType": "address"
            },
            {
                "name": "previousWeight",
                "type": "uint88",
                "indexed": false,
                "internalType": "uint88"
            },
            {
                "name": "newWeight",
                "type": "uint88",
                "indexed": false,
                "internalType": "uint88"
            }
        ],
        "anonymous": false
    }
]`

func main() {
	pflag.String("contract", "", "Contract address (0x...)")
	pflag.Uint64("start-block", 0, "Start block number (inclusive)")
	pflag.StringSlice("rpc", nil, "RPC endpoint (repeatable)")
	pflag.String("db-path", "data", "Database path")
	pflag.String("listen", ":8080", "HTTP listen address")
	pflag.Duration("poll-interval", 5*time.Second, "Polling interval")
	pflag.Uint64("batch-size", 2000, "Block batch size per filterLogs")
	pflag.String("log-level", log.LogLevelDebug, "Log level (debug, info, warn, error)")
	pflag.Parse()

	config := viper.New()
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	config.AutomaticEnv()
	_ = config.BindPFlags(pflag.CommandLine)
	_ = config.BindEnv("contract", "CONTRACT", "CONTRACT_ADDRESS")
	_ = config.BindEnv("start-block", "START_BLOCK")
	_ = config.BindEnv("rpc", "RPCS", "RPC_ENDPOINTS")
	_ = config.BindEnv("db-path", "DB_PATH")
	_ = config.BindEnv("listen", "LISTEN_ADDR", "LISTEN")
	_ = config.BindEnv("poll-interval", "POLL_INTERVAL")
	_ = config.BindEnv("batch-size", "BATCH_SIZE")
	_ = config.BindEnv("log-level", "LOG_LEVEL")

	logLevel := strings.TrimSpace(config.GetString("log-level"))
	if logLevel == "" {
		logLevel = log.LogLevelDebug
	}
	log.Init(logLevel, "stderr", nil)

	contractAddr := strings.TrimSpace(config.GetString("contract"))
	if contractAddr == "" {
		log.Fatal("--contract or CONTRACT env var is required")
	}

	startBlock := config.GetUint64("start-block")
	dbPath := config.GetString("db-path")
	if dbPath == "" {
		dbPath = "data"
	}
	listenAddr := config.GetString("listen")
	if listenAddr == "" {
		listenAddr = ":8080"
	}
	pollInterval := config.GetDuration("poll-interval")
	if pollInterval == 0 {
		pollInterval = 5 * time.Second
	}
	batchSize := config.GetUint64("batch-size")
	if batchSize == 0 {
		batchSize = 2000
	}

	if !common.IsHexAddress(contractAddr) {
		log.Fatalf("invalid contract address: %s", contractAddr)
	}

	rpcs := config.GetStringSlice("rpc")
	if len(rpcs) == 0 {
		rpcs = parseRPCs(config.GetString("rpc"))
	}
	if len(rpcs) == 0 {
		log.Fatal("at least one --rpc or RPCS env var is required")
	}

	log.Infow("starting onchain census indexer",
		"contract", contractAddr,
		"startBlock", startBlock,
		"dbPath", dbPath,
		"listen", listenAddr,
		"pollInterval", pollInterval.String(),
		"batchSize", batchSize,
		"rpcs", strings.Join(rpcs, ","),
	)

	parsedABI, err := abi.JSON(strings.NewReader(weightChangedABIJSON))
	if err != nil {
		log.Fatalf("parse ABI: %v", err)
	}

	database, err := metadb.New(db.TypePebble, dbPath)
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
	var chainID uint64
	for _, endpoint := range rpcs {
		id, err := pool.AddEndpoint(endpoint)
		if err != nil {
			log.Fatalf("add RPC endpoint %s: %v", endpoint, err)
		}
		if chainID == 0 {
			chainID = id
		} else if chainID != id {
			log.Fatalf("RPC endpoints have mismatched chain IDs: %d vs %d", chainID, id)
		}
	}
	log.Infow("rpc endpoints ready", "chainID", chainID, "count", len(rpcs))
	client, err := pool.Client(chainID)
	if err != nil {
		log.Fatalf("create web3 client: %v", err)
	}

	idx, err := indexer.New(indexer.Config{
		Client:       client,
		Store:        eventStore,
		Contract:     common.HexToAddress(contractAddr),
		ABI:          parsedABI,
		StartBlock:   startBlock,
		PollInterval: pollInterval,
		BatchSize:    batchSize,
	})
	if err != nil {
		log.Fatalf("create indexer: %v", err)
	}

	schema, err := graphqlapi.NewSchema(eventStore)
	if err != nil {
		log.Fatalf("create graphql schema: %v", err)
	}
	graphqlHandler := handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: true,
	})

	mux := http.NewServeMux()
	mux.Handle("/graphql", graphqlHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	indexerErr := make(chan error, 1)
	go func() {
		indexerErr <- idx.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	log.Infow("http server started", "addr", listenAddr, "graphql", "/graphql", "healthz", "/healthz")

	select {
	case <-ctx.Done():
	case err := <-indexerErr:
		if !errors.Is(err, context.Canceled) {
			log.Warnf("indexer stopped: %v", err)
		}
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warnf("http server stopped: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warnf("shutdown server: %v", err)
	}

	select {
	case err := <-indexerErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("indexer stopped: %v", err)
		}
	default:
	}
}

func parseRPCs(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == ';'
	})
	var out []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
