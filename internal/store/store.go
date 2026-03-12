package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/db"
)

const (
	eventKeyPrefix       = "evt:"
	lastBlockKeyPrefix   = "meta:last_block:"
	verifiedBlockKeyPref = "meta:verified_block:"
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

// ReplaceOptions controls which progress cursors are updated when replacing a range.
type ReplaceOptions struct {
	IndexedUntil  *uint64
	VerifiedUntil *uint64
}

// New returns a new Store backed by the provided database.
func New(database db.Database) *Store {
	return &Store{db: database}
}

// LastIndexedBlock returns the last indexed block number if present.
func (s *Store) LastIndexedBlock(ctx context.Context, chainID uint64, contract common.Address) (uint64, bool, error) {
	return s.progressBlock(ctx, lastBlockKey(chainID, contract), "last indexed block")
}

// LastVerifiedBlock returns the last verified block number if present.
func (s *Store) LastVerifiedBlock(ctx context.Context, chainID uint64, contract common.Address) (uint64, bool, error) {
	return s.progressBlock(ctx, verifiedBlockKey(chainID, contract), "last verified block")
}

func (s *Store) progressBlock(ctx context.Context, key []byte, label string) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	data, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("get %s: %w", label, err)
	}
	block, err := decodeUint64(data)
	if err != nil {
		return 0, false, fmt.Errorf("decode %s: %w", label, err)
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
	if err := s.setProgressBlocks(tx, chainID, contract, ReplaceOptions{
		IndexedUntil:  &lastIndexedBlock,
		VerifiedUntil: &lastIndexedBlock,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit events: %w", err)
	}
	return nil
}

// SetIndexedBlock persists the last indexed block for a contract.
func (s *Store) SetIndexedBlock(ctx context.Context, chainID uint64, contract common.Address, block uint64) error {
	return s.setProgressBlock(ctx, lastBlockKey(chainID, contract), block, "indexed")
}

// SetVerifiedBlock persists the last verified block for a contract.
func (s *Store) SetVerifiedBlock(ctx context.Context, chainID uint64, contract common.Address, block uint64) error {
	return s.setProgressBlock(ctx, verifiedBlockKey(chainID, contract), block, "verified")
}

func (s *Store) setProgressBlock(ctx context.Context, key []byte, block uint64, label string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := s.db.WriteTx()
	defer tx.Discard()
	if err := tx.Set(key, encodeUint64(block)); err != nil {
		return fmt.Errorf("store %s block: %w", label, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s block: %w", label, err)
	}
	return nil
}

// ReplaceEventsInRange atomically rewrites all events in the inclusive block range and
// optionally updates indexed and/or verified progress cursors.
func (s *Store) ReplaceEventsInRange(
	ctx context.Context,
	chainID uint64,
	contract common.Address,
	from, to uint64,
	events []Event,
	opts ReplaceOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if contract == (common.Address{}) {
		return fmt.Errorf("contract address is required")
	}
	if from > to {
		return fmt.Errorf("from block must be less than or equal to to block")
	}

	keys, err := s.eventKeysInRange(ctx, chainID, contract, from, to)
	if err != nil {
		return err
	}

	tx := s.db.WriteTx()
	defer tx.Discard()

	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.Delete(key); err != nil {
			return fmt.Errorf("delete event in range: %w", err)
		}
	}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateEventForRange(event, chainID, contract, from, to); err != nil {
			return err
		}
		key := eventKey(event.ChainID, contract, event.BlockNumber, event.LogIndex)
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		if err := tx.Set(key, payload); err != nil {
			return fmt.Errorf("store event in range: %w", err)
		}
	}
	if err := s.setProgressBlocks(tx, chainID, contract, opts); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit range replacement: %w", err)
	}
	return nil
}

// SaveContract stores a contract configuration.
// If the contract already exists, startBlock is preserved and expiresAt is updated.
func (s *Store) SaveContract(ctx context.Context, chainID uint64, contract common.Address, startBlock uint64, expiresAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if contract == (common.Address{}) {
		return fmt.Errorf("contract address is required")
	}
	if expiresAt.IsZero() {
		return fmt.Errorf("expiresAt is required")
	}
	expiresAt = expiresAt.UTC()

	key := contractKey(chainID, contract)
	payload, err := s.db.Get(key)
	if err == nil {
		var record ContractRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("decode contract: %w", err)
		}
		if record.ExpiresAt.Equal(expiresAt) {
			return nil
		}
		record.ExpiresAt = expiresAt
		updated, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal contract: %w", err)
		}
		tx := s.db.WriteTx()
		defer tx.Discard()
		if err := tx.Set(key, updated); err != nil {
			return fmt.Errorf("store contract: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit contract: %w", err)
		}
		return nil
	}
	if !errors.Is(err, db.ErrKeyNotFound) {
		return fmt.Errorf("check contract: %w", err)
	}
	record := ContractRecord{
		ChainID:    chainID,
		Contract:   contract.Hex(),
		StartBlock: startBlock,
		ExpiresAt:  expiresAt,
	}
	payload, err = json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal contract: %w", err)
	}
	tx := s.db.WriteTx()
	defer tx.Discard()
	if err := tx.Set(key, payload); err != nil {
		if errors.Is(err, db.ErrConflict) {
			return nil
		}
		return fmt.Errorf("store contract: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit contract: %w", err)
	}
	return nil
}

// SetContractStartBlock updates the start block for an existing contract only
// when the current stored value is zero.
func (s *Store) SetContractStartBlock(ctx context.Context, chainID uint64, contract common.Address, startBlock uint64) error {
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
	payload, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			return fmt.Errorf("contract not found")
		}
		return fmt.Errorf("get contract: %w", err)
	}
	var record ContractRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return fmt.Errorf("decode contract: %w", err)
	}
	if record.StartBlock != 0 {
		return nil
	}
	record.StartBlock = startBlock
	updated, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal contract: %w", err)
	}
	tx := s.db.WriteTx()
	defer tx.Discard()
	if err := tx.Set(key, updated); err != nil {
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

// DeleteContractData removes contract metadata and all indexed events for that contract.
func (s *Store) DeleteContractData(ctx context.Context, chainID uint64, contract common.Address) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chainID == 0 {
		return fmt.Errorf("chainID is required")
	}
	if contract == (common.Address{}) {
		return fmt.Errorf("contract address is required")
	}

	prefix := eventPrefix(chainID, contract)
	eventKeys := make([][]byte, 0)
	var iterErr error
	err := s.db.Iterate(prefix, func(key, _ []byte) bool {
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		keyCopy := fullIteratedKey(prefix, key)
		eventKeys = append(eventKeys, keyCopy)
		return true
	})
	if iterErr != nil {
		return iterErr
	}
	if err != nil {
		return fmt.Errorf("iterate contract events: %w", err)
	}

	tx := s.db.WriteTx()
	defer tx.Discard()

	for _, key := range eventKeys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.Delete(key); err != nil {
			return fmt.Errorf("delete event: %w", err)
		}
	}
	if err := tx.Delete(lastBlockKey(chainID, contract)); err != nil {
		return fmt.Errorf("delete last indexed block: %w", err)
	}
	if err := tx.Delete(verifiedBlockKey(chainID, contract)); err != nil {
		return fmt.Errorf("delete last verified block: %w", err)
	}
	if err := tx.Delete(contractKey(chainID, contract)); err != nil {
		return fmt.Errorf("delete contract: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit contract purge: %w", err)
	}
	return nil
}

// Compact triggers storage compaction so delete tombstones are reclaimed by the backing DB.
func (s *Store) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Compact(); err != nil {
		return fmt.Errorf("compact store: %w", err)
	}
	return nil
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

func fullIteratedKey(prefix, key []byte) []byte {
	if bytes.HasPrefix(key, prefix) {
		return append([]byte(nil), key...)
	}
	full := make([]byte, 0, len(prefix)+len(key))
	full = append(full, prefix...)
	full = append(full, key...)
	return full
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

func verifiedBlockKey(chainID uint64, contract common.Address) []byte {
	key := make([]byte, len(verifiedBlockKeyPref)+8+contractAddressBytes)
	copy(key, verifiedBlockKeyPref)
	offset := len(verifiedBlockKeyPref)
	binary.BigEndian.PutUint64(key[offset:], chainID)
	offset += 8
	copy(key[offset:], contract.Bytes())
	return key
}

// ContractRecord represents a stored contract configuration.
type ContractRecord struct {
	ChainID    uint64    `json:"chainId"`
	Contract   string    `json:"contract"`
	StartBlock uint64    `json:"startBlock"`
	ExpiresAt  time.Time `json:"expiresAt"`
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

func (s *Store) eventKeysInRange(ctx context.Context, chainID uint64, contract common.Address, from, to uint64) ([][]byte, error) {
	prefix := eventPrefix(chainID, contract)
	keys := make([][]byte, 0)
	var iterErr error
	err := s.db.Iterate(prefix, func(key, _ []byte) bool {
		if err := ctx.Err(); err != nil {
			iterErr = err
			return false
		}
		fullKey := fullIteratedKey(prefix, key)
		blockNumber, err := eventBlockNumber(fullKey)
		if err != nil {
			iterErr = err
			return false
		}
		if blockNumber < from || blockNumber > to {
			return true
		}
		keys = append(keys, fullKey)
		return true
	})
	if iterErr != nil {
		return nil, iterErr
	}
	if err != nil {
		return nil, fmt.Errorf("iterate contract events in range: %w", err)
	}
	return keys, nil
}

func eventBlockNumber(key []byte) (uint64, error) {
	expectedLen := len(eventKeyPrefix) + 8 + contractAddressBytes + 8 + 4
	if len(key) != expectedLen {
		return 0, fmt.Errorf("invalid event key length: %d", len(key))
	}
	offset := len(eventKeyPrefix) + 8 + contractAddressBytes
	return binary.BigEndian.Uint64(key[offset : offset+8]), nil
}

func validateEventForRange(event Event, chainID uint64, contract common.Address, from, to uint64) error {
	if event.ChainID != chainID {
		return fmt.Errorf("event chainID mismatch")
	}
	if !common.IsHexAddress(event.Contract) {
		return fmt.Errorf("event contract is invalid")
	}
	if common.HexToAddress(event.Contract) != contract {
		return fmt.Errorf("event contract mismatch")
	}
	if event.BlockNumber < from || event.BlockNumber > to {
		return fmt.Errorf("event block %d outside replace range [%d,%d]", event.BlockNumber, from, to)
	}
	return nil
}

func (s *Store) setProgressBlocks(tx db.WriteTx, chainID uint64, contract common.Address, opts ReplaceOptions) error {
	if opts.IndexedUntil != nil {
		if err := tx.Set(lastBlockKey(chainID, contract), encodeUint64(*opts.IndexedUntil)); err != nil {
			return fmt.Errorf("store last indexed block: %w", err)
		}
	}
	if opts.VerifiedUntil != nil {
		if err := tx.Set(verifiedBlockKey(chainID, contract), encodeUint64(*opts.VerifiedUntil)); err != nil {
			return fmt.Errorf("store last verified block: %w", err)
		}
	}
	return nil
}
