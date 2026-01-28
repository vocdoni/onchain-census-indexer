package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vocdoni/davinci-node/db"
)

const (
	eventKeyPrefix   = "evt:"
	lastBlockKeyName = "meta:last_block"
)

// Event represents a WeightChanged event stored in the database.
type Event struct {
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
func (s *Store) LastIndexedBlock(ctx context.Context) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	data, err := s.db.Get([]byte(lastBlockKeyName))
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

// SaveEvents persists the provided events and updates the last indexed block.
func (s *Store) SaveEvents(ctx context.Context, events []Event, lastIndexedBlock uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := s.db.WriteTx()
	defer tx.Discard()

	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := eventKey(event.BlockNumber, event.LogIndex)
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		if err := tx.Set(key, payload); err != nil {
			return fmt.Errorf("store event: %w", err)
		}
	}
	if err := tx.Set([]byte(lastBlockKeyName), encodeUint64(lastIndexedBlock)); err != nil {
		return fmt.Errorf("store last indexed block: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit events: %w", err)
	}
	return nil
}

// ListOptions defines pagination and ordering options when listing events.
type ListOptions struct {
	First          int
	Skip           int
	OrderBy        string
	OrderDirection string
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

	if orderDirection == "desc" {
		return s.listEventsDesc(ctx, opts)
	}
	if orderDirection != "asc" {
		return nil, fmt.Errorf("unsupported orderDirection: %s", orderDirection)
	}

	return s.listEventsAsc(ctx, opts)
}

func (s *Store) listEventsAsc(ctx context.Context, opts ListOptions) ([]Event, error) {
	var (
		results []Event
		skipped int
		iterErr error
	)
	err := s.db.Iterate([]byte(eventKeyPrefix), func(_, value []byte) bool {
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

func (s *Store) listEventsDesc(ctx context.Context, opts ListOptions) ([]Event, error) {
	var (
		all     []Event
		iterErr error
	)
	err := s.db.Iterate([]byte(eventKeyPrefix), func(_, value []byte) bool {
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

func eventKey(blockNumber uint64, logIndex uint32) []byte {
	key := make([]byte, len(eventKeyPrefix)+8+4)
	copy(key, eventKeyPrefix)
	binary.BigEndian.PutUint64(key[len(eventKeyPrefix):], blockNumber)
	binary.BigEndian.PutUint32(key[len(eventKeyPrefix)+8:], logIndex)
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
