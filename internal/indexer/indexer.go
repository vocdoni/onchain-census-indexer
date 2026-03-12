package indexer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	contracts "github.com/vocdoni/davinci-contracts/golang-types"
	"github.com/vocdoni/davinci-node/log"
	"github.com/vocdoni/davinci-node/web3/rpc"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

var errRetryable = errors.New("retryable error")

// Config configures the indexer.
type Config struct {
	Client          *rpc.Client
	Store           *store.Store
	ChainID         uint64
	Contract        common.Address
	StartBlock      uint64
	PollInterval    time.Duration
	BatchSize       uint64
	VerifyBatchSize uint64
	Confirmations   uint64
	TailRescanDepth uint64
}

// Indexer indexes WeightChanged events into the database.
type Indexer struct {
	client          *rpc.Client
	store           *store.Store
	chainID         uint64
	contract        common.Address
	filterer        *contracts.ICensusValidatorFilterer
	startBlock      uint64
	pollInterval    time.Duration
	batchSize       uint64
	verifyBatchSize uint64
	confirmations   uint64
	tailRescanDepth uint64
	headFunc        func(context.Context) (uint64, error)
	eventsFunc      func(context.Context, uint64, uint64) ([]store.Event, error)
}

type progressState struct {
	indexedUntil   uint64
	verifiedUntil  uint64
	tailRescanFrom uint64
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
	filterer, err := contracts.NewICensusValidatorFilterer(cfg.Contract, cfg.Client)
	if err != nil {
		return nil, fmt.Errorf("create contract filterer: %w", err)
	}
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 2000
	}
	verifyBatchSize := cfg.VerifyBatchSize
	if verifyBatchSize == 0 {
		verifyBatchSize = batchSize
	}
	tailRescanDepth := cfg.TailRescanDepth
	if tailRescanDepth == 0 {
		tailRescanDepth = verifyBatchSize
	}
	idx := &Indexer{
		client:          cfg.Client,
		store:           cfg.Store,
		chainID:         cfg.ChainID,
		contract:        cfg.Contract,
		filterer:        filterer,
		startBlock:      cfg.StartBlock,
		pollInterval:    pollInterval,
		batchSize:       batchSize,
		verifyBatchSize: verifyBatchSize,
		confirmations:   cfg.Confirmations,
		tailRescanDepth: tailRescanDepth,
	}
	idx.headFunc = idx.client.BlockNumber
	idx.eventsFunc = idx.fetchEventsFromRPC
	return idx, nil
}

// Run starts the indexer loop until the context is canceled.
func (i *Indexer) Run(ctx context.Context) error {
	state, err := i.loadProgress(ctx)
	if err != nil {
		return err
	}

	log.Infow("indexer starting",
		"chainID", i.chainID,
		"contract", i.contract.Hex(),
		"startBlock", i.startBlock,
		"indexedUntil", state.indexedUntil,
		"verifiedUntil", state.verifiedUntil,
		"pollInterval", i.pollInterval.String(),
		"batchSize", i.batchSize,
		"verifyBatchSize", i.verifyBatchSize,
		"confirmations", i.confirmations,
		"tailRescanDepth", i.tailRescanDepth,
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err = i.syncOnce(ctx, &state)
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

func (i *Indexer) loadProgress(ctx context.Context) (progressState, error) {
	indexedUntil, indexedOK, err := i.store.LastIndexedBlock(ctx, i.chainID, i.contract)
	if err != nil {
		return progressState{}, err
	}
	verifiedUntil, verifiedOK, err := i.store.LastVerifiedBlock(ctx, i.chainID, i.contract)
	if err != nil {
		return progressState{}, err
	}
	startCursor := uint64(0)
	if i.startBlock > 0 {
		startCursor = i.startBlock - 1
	}
	if !indexedOK || (i.startBlock > 0 && indexedUntil+1 < i.startBlock) {
		indexedUntil = startCursor
	}
	if !verifiedOK || (i.startBlock > 0 && verifiedUntil+1 < i.startBlock) {
		verifiedUntil = startCursor
	}
	if verifiedUntil > indexedUntil {
		verifiedUntil = indexedUntil
	}
	return progressState{
		indexedUntil:  indexedUntil,
		verifiedUntil: verifiedUntil,
	}, nil
}

func (i *Indexer) syncOnce(ctx context.Context, state *progressState) error {
	head, err := i.headFunc(ctx)
	if err != nil {
		return fmt.Errorf("%w: fetch head block: %v", errRetryable, err)
	}
	safeHead, hasSafeHead := i.safeHead(head)
	log.Debugw("head block fetched",
		"head", head,
		"safeHead", safeHead,
		"indexedUntil", state.indexedUntil,
		"verifiedUntil", state.verifiedUntil,
	)
	if !hasSafeHead {
		log.Debugw("head below confirmations threshold", "head", head, "confirmations", i.confirmations)
		return nil
	}

	for state.verifiedUntil < safeHead {
		if err := ctx.Err(); err != nil {
			return err
		}
		verifyFrom := state.verifiedUntil + 1
		verifyTo := min(verifyFrom+i.verifyBatchSize-1, safeHead)
		if err := i.indexRange(ctx, state, verifyTo); err != nil {
			return err
		}
		if err := i.verifyRange(ctx, state, verifyFrom, verifyTo); err != nil {
			return err
		}
	}
	if err := i.rescanTail(ctx, state, safeHead); err != nil {
		return err
	}
	return nil
}

func (i *Indexer) indexRange(ctx context.Context, state *progressState, targetTo uint64) error {
	for state.indexedUntil < targetTo {
		if err := ctx.Err(); err != nil {
			return err
		}
		from := state.indexedUntil + 1
		to := min(from+i.batchSize-1, targetTo)
		log.Debugw("first-pass fetch", "from", from, "to", to)
		events, err := i.eventsFunc(ctx, from, to)
		if err != nil {
			return err
		}
		if err := i.store.ReplaceEventsInRange(ctx, i.chainID, i.contract, from, to, events, store.ReplaceOptions{
			IndexedUntil: &to,
		}); err != nil {
			return fmt.Errorf("store first-pass events: %w", err)
		}
		state.indexedUntil = to
		if len(events) > 0 {
			log.Infow("stored first-pass batch", "from", from, "to", to, "count", len(events))
		} else {
			log.Debugw("stored first-pass batch", "from", from, "to", to, "count", 0)
		}
	}
	return nil
}

func (i *Indexer) verifyRange(ctx context.Context, state *progressState, from, to uint64) error {
	log.Debugw("verification fetch", "from", from, "to", to)
	events, err := i.eventsFunc(ctx, from, to)
	if err != nil {
		return err
	}
	if err := i.store.ReplaceEventsInRange(ctx, i.chainID, i.contract, from, to, events, store.ReplaceOptions{
		VerifiedUntil: &to,
	}); err != nil {
		return fmt.Errorf("store verified events: %w", err)
	}
	state.verifiedUntil = to
	if len(events) > 0 {
		log.Infow("verified events batch", "from", from, "to", to, "count", len(events))
	} else {
		log.Debugw("verified events batch", "from", from, "to", to, "count", 0)
	}
	return nil
}

func (i *Indexer) rescanTail(ctx context.Context, state *progressState, safeHead uint64) error {
	if i.tailRescanDepth == 0 {
		return nil
	}
	windowEnd := min(state.verifiedUntil, safeHead)
	windowStart, ok := i.tailWindowStart(windowEnd)
	if !ok {
		return nil
	}
	if state.tailRescanFrom < windowStart || state.tailRescanFrom > windowEnd {
		state.tailRescanFrom = windowStart
	}
	from := state.tailRescanFrom
	to := min(from+i.verifyBatchSize-1, windowEnd)
	log.Debugw("tail rescan fetch", "from", from, "to", to, "windowStart", windowStart, "windowEnd", windowEnd)
	events, err := i.eventsFunc(ctx, from, to)
	if err != nil {
		return err
	}
	if err := i.store.ReplaceEventsInRange(ctx, i.chainID, i.contract, from, to, events, store.ReplaceOptions{}); err != nil {
		return fmt.Errorf("store tail rescan events: %w", err)
	}
	if len(events) > 0 {
		log.Debugw("tail rescan stored", "from", from, "to", to, "count", len(events))
	} else {
		log.Debugw("tail rescan stored", "from", from, "to", to, "count", 0)
	}
	if to >= windowEnd {
		state.tailRescanFrom = windowStart
	} else {
		state.tailRescanFrom = to + 1
	}
	return nil
}

func (i *Indexer) safeHead(head uint64) (uint64, bool) {
	if i.confirmations == 0 {
		return head, true
	}
	if head < i.confirmations {
		return 0, false
	}
	return head - i.confirmations, true
}

func (i *Indexer) tailWindowStart(windowEnd uint64) (uint64, bool) {
	if i.startBlock > 0 && windowEnd < i.startBlock {
		return 0, false
	}
	windowStart := uint64(0)
	if i.startBlock > 0 {
		windowStart = i.startBlock
	}
	if windowEnd+1 > i.tailRescanDepth {
		candidate := windowEnd + 1 - i.tailRescanDepth
		if candidate > windowStart {
			windowStart = candidate
		}
	}
	if windowStart > windowEnd {
		return 0, false
	}
	return windowStart, true
}

func (i *Indexer) fetchEventsFromRPC(ctx context.Context, from, to uint64) ([]store.Event, error) {
	opts := &bind.FilterOpts{
		Start:   from,
		End:     &to,
		Context: ctx,
	}
	iter, err := i.filterer.FilterWeightChanged(opts, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: filter logs from %d to %d: %v", errRetryable, from, to, err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			log.Warnw("close logs iterator error", "err", err)
		}
	}()

	results := make([]store.Event, 0)
	for iter.Next() {
		event := iter.Event
		if event == nil {
			continue
		}
		if event.Raw.Index > math.MaxUint32 {
			return nil, fmt.Errorf("log index overflows uint32")
		}
		results = append(results, store.Event{
			ChainID:        i.chainID,
			Contract:       i.contract.Hex(),
			Account:        event.Account.Hex(),
			PreviousWeight: event.PreviousWeight.String(),
			NewWeight:      event.NewWeight.String(),
			BlockNumber:    event.Raw.BlockNumber,
			LogIndex:       uint32(event.Raw.Index),
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("%w: filter logs from %d to %d: %v", errRetryable, from, to, err)
	}
	sort.Slice(results, func(a, b int) bool {
		if results[a].BlockNumber == results[b].BlockNumber {
			return results[a].LogIndex < results[b].LogIndex
		}
		return results[a].BlockNumber < results[b].BlockNumber
	})
	log.Debugw("filter logs completed", "from", from, "to", to, "events", len(results))
	return results, nil
}
