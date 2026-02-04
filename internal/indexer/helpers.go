package indexer

import (
	"context"
	"math/big"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/vocdoni/davinci-node/web3/rpc"
)

var definitiveErrors = []string{
	"No state available for block",
	"missing trie node",
}

func isDefinitiveError(err error) bool {
	if err == nil {
		return false
	}
	return slices.ContainsFunc(definitiveErrors, func(e string) bool {
		return strings.Contains(err.Error(), e)
	})
}

// creationBlockInRange function finds the block number of a contract between
// the bounds provided as start and end blocks.
func creationBlockInRange(
	ctx context.Context,
	client *rpc.Client,
	addr common.Address,
	start, end uint64,
) (uint64, error) {
	// if both block numbers are equal, return its value as birthblock
	if start == end {
		return start, nil
	}
	// find the middle block between start and end blocks and get the contract
	// code at this block
	midBlock := (start + end) / 2
	codeLen, err := sourceCodeLenAt(ctx, client, addr, midBlock)
	if isDefinitiveError(err) {
		return 0, err
	}
	// if any code is found, keep trying with the lower half of blocks until
	// find the first. if not, keep trying with the upper half
	if codeLen > 2 {
		return creationBlockInRange(ctx, client, addr, start, midBlock)
	} else {
		return creationBlockInRange(ctx, client, addr, midBlock+1, end)
	}
}

// SourceCodeLenAt function returns the length of the current contract bytecode
// at the block number provided.
func sourceCodeLenAt(
	ctx context.Context,
	client *rpc.Client,
	addr common.Address,
	atBlockNumber uint64,
) (int, error) {
	blockNumber := new(big.Int).SetUint64(atBlockNumber)
	sourceCode, err := client.CodeAt(ctx, addr, blockNumber)
	return len(sourceCode), err
}
