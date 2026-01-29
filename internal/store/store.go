package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/db"
)

const (
	eventKeyPrefix       = "evt:"
	lastBlockKeyPrefix   = "meta:last_block:"
	contractKeyPrefix    = "meta:contract:"
	contractAddressBytes = 20
)

// Event represents a WeightChanged event stored in the database.
type Event struct {
	ChainID        uint64 `json:"chainId"`
	Contract       string `json:"contract"`
	Account        string `json:"account"`
	PreviousWeight string `json:"previousWeight"`
	NewWeight      string `json:"newWeight"`
	BlockNumber    uint64 `json:"blockNumber"`
	LogIndex       uint32 `json:"logIndex"`
}

// Store provides access to persisted WeightChanged events.
type Store struct {
	db db.Database
}

// New returns a new Store backed by the provided database.
func New(database db.Database) *Store {
	return &Store{db: database}
}

// LastIndexedBlock returns the last indexed block number if present.
func (s *Store) LastIndexedBlock(ctx context.Context, chainID uint64, contract common.Address) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	data, err := s.db.Get(lastBlockKey(chainID, contract))
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("get last indexed block: %w", err)
	}
	block, err := decodeUint64(data)
	if err != nil {
		return 0, false, fmt.Errorf("decode last indexed block: %w", err)
	}
	return block, true, nil
}

// SaveEvents persists the provided events and updates the last indexed block for the contract.
func (s *Store) SaveEvents(ctx context.Context, chainID uint64, contract common.Address, events []Event, lastIndexedBlock uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if contract == (common.Address{}) {
		return fmt.Errorf("contract address is required")
	}
	tx := s.db.WriteTx()
	defer tx.Discard()

	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if event.ChainID == 0 {
			return fmt.Errorf("event chainID is required")
		}
		if !common.IsHexAddress(event.Contract) {
			return fmt.Errorf("event contract is invalid")
		}
		contractAddr := common.HexToAddress(event.Contract)
		key := eventKey(event.ChainID, contractAddr, event.BlockNumber, event.LogIndex)
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		if err := tx.Set(key, payload); err != nil {
			return fmt.Errorf("store event: %w", err)
		}
	}
	if err := tx.Set(lastBlockKey(chainID, contract), encodeUint64(lastIndexedBlock)); err != nil {
		return fmt.Errorf("store last indexed block: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit events: %w", err)
	}
	return nil
}

// SaveContract stores or updates a contract configuration.
func (s *Store) SaveContract(ctx context.Context, chainID uint64, contract common.Address, startBlock uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if contract == (common.Address{}) {
		return fmt.Errorf("contract address is required")
	}
	key := contractKey(chainID, contract)
	if _, err := s.db.Get(key); err == nil {
		return nil
	} else if !errors.Is(err, db.ErrKeyNotFound) {
		return fmt.Errorf("check contract: %w", err)
	}
	record := ContractRecord{
		ChainID:    chainID,
		Contract:   contract.Hex(),
		StartBlock: startBlock,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal contract: %w", err)
	}
	tx := s.db.WriteTx()
	defer tx.Discard()
	if err := tx.Set(key, payload); err != nil {
		if errors.Is(err, db.ErrConflict) {
		}
		return fmt.Errorf("store contract: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit contract: %w", err)
	}
	return nil
}

// ListContracts returns all stored contracts.
func (s *Store) ListContracts(ctx context.Context) ([]ContractRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var (
		results []ContractRecord
		iterErr error
	)
	err := s.db.Iterate([]byte(contractKeyPrefix), func(_, value []byte) bool {
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		var record ContractRecord
		if err := json.Unmarshal(value, &record); err != nil {
			iterErr = fmt.Errorf("decode contract: %w", err)
			return false
		}
		results = append(results, record)
		return true
	})
	if iterErr != nil {
		return nil, iterErr
	}
	if err != nil {
		return nil, fmt.Errorf("iterate contracts: %w", err)
	}
	return results, nil
}

// ListOptions defines pagination and ordering options when listing events.
type ListOptions struct {
	First          int
	Skip           int
	OrderBy        string
	OrderDirection string
	ChainID        uint64
	Contract       common.Address
}

// ListEvents returns events matching the provided options.
func (s *Store) ListEvents(ctx context.Context, opts ListOptions) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.First < 0 || opts.Skip < 0 {
		return nil, fmt.Errorf("first and skip must be non-negative")
	}
	orderBy := opts.OrderBy
	if orderBy == "" {
		orderBy = "blockNumber"
	}
	if orderBy != "blockNumber" {
		return nil, fmt.Errorf("unsupported orderBy: %s", orderBy)
	}
	orderDirection := opts.OrderDirection
	if orderDirection == "" {
		orderDirection = "asc"
	}

	prefix := []byte(eventKeyPrefix)
	if opts.ChainID != 0 || opts.Contract != (common.Address{}) {
		if opts.ChainID == 0 || opts.Contract == (common.Address{}) {
			return nil, fmt.Errorf("both chainID and contract are required for filtering")
		}
		prefix = eventPrefix(opts.ChainID, opts.Contract)
	}

	if orderDirection == "desc" {
		return s.listEventsDesc(ctx, opts, prefix)
	}
	if orderDirection != "asc" {
		return nil, fmt.Errorf("unsupported orderDirection: %s", orderDirection)
	}

	return s.listEventsAsc(ctx, opts, prefix)
}

func (s *Store) listEventsAsc(ctx context.Context, opts ListOptions, prefix []byte) ([]Event, error) {
	var (
		results []Event
		skipped int
		iterErr error
	)
	err := s.db.Iterate(prefix, func(_, value []byte) bool {
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		if skipped < opts.Skip {
			skipped++
			return true
		}
		if opts.First > 0 && len(results) >= opts.First {
			return false
		}
		var event Event
		if err := json.Unmarshal(value, &event); err != nil {
			iterErr = fmt.Errorf("decode event: %w", err)
			return false
		}
		results = append(results, event)
		return true
	})
	if iterErr != nil {
		return nil, iterErr
	}
	if err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return results, nil
}

func (s *Store) listEventsDesc(ctx context.Context, opts ListOptions, prefix []byte) ([]Event, error) {
	var (
		all     []Event
		iterErr error
	)
	err := s.db.Iterate(prefix, func(_, value []byte) bool {
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		var event Event
		if err := json.Unmarshal(value, &event); err != nil {
			iterErr = fmt.Errorf("decode event: %w", err)
			return false
		}
		all = append(all, event)
		return true
	})
	if iterErr != nil {
		return nil, iterErr
	}
	if err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	start := opts.Skip
	if start > len(all) {
		return []Event{}, nil
	}
	end := len(all)
	if opts.First > 0 && start+opts.First < end {
		end = start + opts.First
	}
	return all[start:end], nil
}

func eventKey(chainID uint64, contract common.Address, blockNumber uint64, logIndex uint32) []byte {
	key := make([]byte, len(eventKeyPrefix)+8+contractAddressBytes+8+4)
	copy(key, eventKeyPrefix)
	offset := len(eventKeyPrefix)
	binary.BigEndian.PutUint64(key[offset:], chainID)
	offset += 8
	copy(key[offset:], contract.Bytes())
	offset += contractAddressBytes
	binary.BigEndian.PutUint64(key[offset:], blockNumber)
	offset += 8
	binary.BigEndian.PutUint32(key[offset:], logIndex)
	return key
}

func eventPrefix(chainID uint64, contract common.Address) []byte {
	key := make([]byte, len(eventKeyPrefix)+8+contractAddressBytes)
	copy(key, eventKeyPrefix)
	offset := len(eventKeyPrefix)
	binary.BigEndian.PutUint64(key[offset:], chainID)
	offset += 8
	copy(key[offset:], contract.Bytes())
	return key
}

func encodeUint64(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

func decodeUint64(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid uint64 length: %d", len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

func lastBlockKey(chainID uint64, contract common.Address) []byte {
	key := make([]byte, len(lastBlockKeyPrefix)+8+contractAddressBytes)
	copy(key, lastBlockKeyPrefix)
	offset := len(lastBlockKeyPrefix)
	binary.BigEndian.PutUint64(key[offset:], chainID)
	offset += 8
	copy(key[offset:], contract.Bytes())
	return key
}

// ContractRecord represents a stored contract configuration.
type ContractRecord struct {
	ChainID    uint64 `json:"chainId"`
	Contract   string `json:"contract"`
	StartBlock uint64 `json:"startBlock"`
}

func contractKey(chainID uint64, contract common.Address) []byte {
	key := make([]byte, len(contractKeyPrefix)+8+contractAddressBytes)
	copy(key, contractKeyPrefix)
	offset := len(contractKeyPrefix)
	binary.BigEndian.PutUint64(key[offset:], chainID)
	offset += 8
	copy(key[offset:], contract.Bytes())
	return key
}
