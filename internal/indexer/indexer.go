package indexer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

const weightChangedEventName = "WeightChanged"

var errRetryable = errors.New("retryable error")

// Config configures the indexer.
type Config struct {
	Client       *rpc.Client
	Store        *store.Store
	ChainID      uint64
	Contract     common.Address
	StartBlock   uint64
	PollInterval time.Duration
	BatchSize    uint64
}

// Indexer indexes WeightChanged events into the database.
type Indexer struct {
	client       *rpc.Client
	store        *store.Store
	chainID      uint64
	contract     common.Address
	abi          abi.ABI
	eventID      common.Hash
	startBlock   uint64
	pollInterval time.Duration
	batchSize    uint64
}

// New returns a new Indexer with the provided configuration.
func New(cfg Config) (*Indexer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if cfg.ChainID == 0 {
		return nil, fmt.Errorf("chainID is required")
	}
	parsedABI, err := loadABI()
	if err != nil {
		return nil, fmt.Errorf("load ABI: %w", err)
	}
	event, ok := parsedABI.Events[weightChangedEventName]
	if !ok {
		return nil, fmt.Errorf("event %s not found in ABI", weightChangedEventName)
	}
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 2000
	}
	return &Indexer{
		client:       cfg.Client,
		store:        cfg.Store,
		chainID:      cfg.ChainID,
		contract:     cfg.Contract,
		abi:          parsedABI,
		eventID:      event.ID,
		startBlock:   cfg.StartBlock,
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}, nil
}

// Run starts the indexer loop until the context is canceled.
func (i *Indexer) Run(ctx context.Context) error {
	lastBlock, ok, err := i.store.LastIndexedBlock(ctx, i.chainID, i.contract)
	if err != nil {
		return err
	}
	if !ok {
		if i.startBlock > 0 {
			lastBlock = i.startBlock - 1
		}
	} else if i.startBlock > 0 && lastBlock+1 < i.startBlock {
		lastBlock = i.startBlock - 1
	}

	log.Infow("indexer starting",
		"chainID", i.chainID,
		"contract", i.contract.Hex(),
		"startBlock", i.startBlock,
		"lastIndexedBlock", lastBlock,
		"pollInterval", i.pollInterval.String(),
		"batchSize", i.batchSize,
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err = i.syncOnce(ctx, &lastBlock)
		if err != nil {
			if errors.Is(err, errRetryable) {
				log.Warnf("indexer retryable error: %v", err)
			} else {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(i.pollInterval):
		}
	}
}

func (i *Indexer) syncOnce(ctx context.Context, lastBlock *uint64) error {
	head, err := i.client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("%w: fetch head block: %v", errRetryable, err)
	}
	log.Debugw("head block fetched", "head", head, "lastIndexedBlock", *lastBlock)
	if *lastBlock >= head {
		log.Debugw("no new blocks to index", "head", head, "lastIndexedBlock", *lastBlock)
		return nil
	}
	for *lastBlock < head {
		if err := ctx.Err(); err != nil {
			return err
		}
		from := *lastBlock + 1
		to := from + i.batchSize - 1
		if to > head {
			to = head
		}
		log.Debugw("fetching logs batch", "from", from, "to", to)
		logs, err := i.fetchLogs(ctx, from, to)
		if err != nil {
			return err
		}
		events, err := i.parseLogs(logs)
		if err != nil {
			return err
		}
		if err := i.store.SaveEvents(ctx, i.chainID, i.contract, events, to); err != nil {
			return fmt.Errorf("store events: %w", err)
		}
		if len(events) > 0 {
			log.Infow("stored events batch", "from", from, "to", to, "count", len(events))
		} else {
			log.Debugw("stored events batch", "from", from, "to", to, "count", 0)
		}
		*lastBlock = to
	}
	return nil
}

func (i *Indexer) fetchLogs(ctx context.Context, from, to uint64) ([]gethtypes.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(0).SetUint64(from),
		ToBlock:   big.NewInt(0).SetUint64(to),
		Addresses: []common.Address{i.contract},
		Topics:    [][]common.Hash{{i.eventID}},
	}
	logs, err := i.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%w: filter logs from %d to %d: %v", errRetryable, from, to, err)
	}
	log.Debugw("filter logs completed", "from", from, "to", to, "logs", len(logs))
	sort.Slice(logs, func(a, b int) bool {
		if logs[a].BlockNumber == logs[b].BlockNumber {
			return logs[a].Index < logs[b].Index
		}
		return logs[a].BlockNumber < logs[b].BlockNumber
	})
	return logs, nil
}

func (i *Indexer) parseLogs(logs []gethtypes.Log) ([]store.Event, error) {
	results := make([]store.Event, 0, len(logs))
	for _, logEntry := range logs {
		if len(logEntry.Topics) < 2 {
			return nil, fmt.Errorf("log missing indexed account topic")
		}
		if logEntry.Index > math.MaxUint32 {
			return nil, fmt.Errorf("log index overflows uint32")
		}
		var decoded struct {
			PreviousWeight *big.Int
			NewWeight      *big.Int
		}
		if err := i.abi.UnpackIntoInterface(&decoded, weightChangedEventName, logEntry.Data); err != nil {
			return nil, fmt.Errorf("unpack log data: %w", err)
		}
		account := common.HexToAddress(logEntry.Topics[1].Hex())
		results = append(results, store.Event{
			ChainID:        i.chainID,
			Contract:       i.contract.Hex(),
			Account:        account.Hex(),
			PreviousWeight: decoded.PreviousWeight.String(),
			NewWeight:      decoded.NewWeight.String(),
			BlockNumber:    logEntry.BlockNumber,
			LogIndex:       uint32(logEntry.Index),
		})
	}
	return results, nil
}
