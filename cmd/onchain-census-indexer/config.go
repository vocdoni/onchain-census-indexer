package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vocdoni/davinci-node/log"

	"github.com/vocdoni/onchain-census-indexer/internal/indexer"
)

type Config struct {
	ContractsRaw string                 `mapstructure:"contracts"`
	Contracts    []indexer.ContractInfo `mapstructure:"-"`
	RPCs         []string               `mapstructure:"rpc"`
	DB           DBConfig               `mapstructure:"db"`
	HTTP         HTTPConfig             `mapstructure:"http"`
	Indexer      IndexerConfig          `mapstructure:"indexer"`
	Log          LogConfig              `mapstructure:"log"`
}

type DBConfig struct {
	Path string `mapstructure:"path"`
}

type HTTPConfig struct {
	ListenAddr         string   `mapstructure:"listen"`
	CORSAllowedOrigins []string `mapstructure:"corsAllowedOrigins"`
}

type IndexerConfig struct {
	PollInterval time.Duration `mapstructure:"pollInterval"`
	BatchSize    uint64        `mapstructure:"batchSize"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}

	pflag.String("contracts", "", "Contracts in format chainID:contractAddress:blockNumber,chainID:contractAddress:blockNumber")
	pflag.String("contract", "", "Deprecated: single contract in format chainID:contractAddress:blockNumber")
	pflag.StringSlice("rpc", nil, "RPC endpoint (repeatable)")
	pflag.String("db.path", "data", "Database path")
	pflag.String("http.listen", ":8080", "HTTP listen address")
	pflag.StringSlice("http.corsAllowedOrigins", []string{"*"}, "Allowed CORS origins (repeatable or comma-separated)")
	pflag.Duration("indexer.pollInterval", 5*time.Second, "Polling interval")
	pflag.Uint64("indexer.batchSize", 2000, "Block batch size per filterLogs")
	pflag.String("log.level", log.LogLevelDebug, "Log level (debug, info, warn, error)")
	pflag.Parse()

	config := viper.New()
	config.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	config.AutomaticEnv()
	if err := config.BindPFlags(pflag.CommandLine); err != nil {
		return nil, fmt.Errorf("bind flags: %w", err)
	}
	_ = config.BindEnv("contracts", "CONTRACTS")
	_ = config.BindEnv("contract", "CONTRACT", "CONTRACT_ADDRESS")
	_ = config.BindEnv("rpc", "RPCS", "RPC_ENDPOINTS")
	_ = config.BindEnv("db.path", "DB_PATH")
	_ = config.BindEnv("http.listen", "LISTEN_ADDR", "LISTEN")
	_ = config.BindEnv("http.corsAllowedOrigins", "CORS_ALLOWED_ORIGINS")
	_ = config.BindEnv("indexer.pollInterval", "POLL_INTERVAL")
	_ = config.BindEnv("indexer.batchSize", "BATCH_SIZE")
	_ = config.BindEnv("log.level", "LOG_LEVEL")

	if err := config.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if cfg.ContractsRaw == "" {
		cfg.ContractsRaw = strings.TrimSpace(config.GetString("contract"))
	}
	if cfg.ContractsRaw != "" {
		contracts, err := parseContractSpecs(cfg.ContractsRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid contracts: %w", err)
		}
		cfg.Contracts = contracts
	}

	if cfg.Log.Level == "" {
		cfg.Log.Level = log.LogLevelDebug
	}
	if cfg.Indexer.PollInterval == 0 {
		cfg.Indexer.PollInterval = 5 * time.Second
	}
	if cfg.Indexer.BatchSize == 0 {
		cfg.Indexer.BatchSize = 2000
	}
	if cfg.DB.Path == "" {
		cfg.DB.Path = "data"
	}
	if cfg.HTTP.ListenAddr == "" {
		cfg.HTTP.ListenAddr = ":8080"
	}
	cfg.HTTP.CORSAllowedOrigins = normalizeCSVList(cfg.HTTP.CORSAllowedOrigins)
	if len(cfg.HTTP.CORSAllowedOrigins) == 0 {
		cfg.HTTP.CORSAllowedOrigins = []string{"*"}
	}

	return cfg, nil
}

func parseContractSpecs(value string) ([]indexer.ContractInfo, error) {
	entries := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == ';'
	})
	if len(entries) == 0 {
		return nil, fmt.Errorf("no contract entries provided")
	}
	out := make([]indexer.ContractInfo, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid contract entry %q (expected chainID:contractAddress:blockNumber)", entry)
		}
		chainID, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || chainID == 0 {
			return nil, fmt.Errorf("invalid chainID in %q", entry)
		}
		address := strings.TrimSpace(parts[1])
		if !common.IsHexAddress(address) {
			return nil, fmt.Errorf("invalid contract address in %q", entry)
		}
		startBlock, err := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid start block in %q", entry)
		}
		out = append(out, indexer.ContractInfo{
			ChainID:    chainID,
			Address:    common.HexToAddress(address),
			StartBlock: startBlock,
		})
	}
	return out, nil
}

func normalizeCSVList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		parts := strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == ';'
		})
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			out = append(out, part)
		}
	}
	return out
}
