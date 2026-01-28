package indexer

import (
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
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

var (
	abiOnce  sync.Once
	abiValue abi.ABI
	abiErr   error
)

func loadABI() (abi.ABI, error) {
	abiOnce.Do(func() {
		abiValue, abiErr = abi.JSON(strings.NewReader(weightChangedABIJSON))
	})
	return abiValue, abiErr
}
