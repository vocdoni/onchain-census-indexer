# Onchain Census Indexer

A small Go service that indexes the `WeightChanged` event from a smart contract using one or more RPC endpoints (with automatic rotation), stores the events in a local Pebble database, and exposes them via a GraphQL API.

The service is configured via CLI flags or environment variables (flags take precedence). It uses:

- `github.com/vocdoni/davinci-node/web3/rpc` for RPC pooling and rotation
- `github.com/vocdoni/davinci-node/db/metadb` with `db.TypePebble` for local persistence
- `github.com/graphql-go/graphql` for the GraphQL endpoint

## Features

- Start indexing from a given block number
- Rotates between multiple RPC endpoints automatically
- Persists events in a local Pebble DB
- GraphQL endpoint compatible with the provided schema/query
- Dockerized with env-var configuration

## GraphQL Schema (reference)

```
type Account @entity(immutable: false) {
  id: String! # address
}

type WeightChangeEvent @entity(immutable: true) {
  account: Account! # address
  previousWeight: BigInt! # uint88
  newWeight: BigInt! # uint88
  blockNumber: BigInt!
}
```

## Example Query (reference)

```
query GetWeightChangeEvents($first: Int!, $skip: Int!) {
    weightChangeEvents(
        first: $first
        skip: $skip
        orderBy: blockNumber
        orderDirection: asc
    ) {
        account {
            id
        }
        previousWeight
        newWeight
    }
}
```

## Contract ABI (embedded in code; reference copy below)

```
[
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
]
```

## Local usage

### Requirements

- Go 1.21+ (the module currently targets Go 1.25.x)
- A working C toolchain if building with cgo (macOS: `xcode-select --install`)

### Run with flags

```
go run ./cmd/onchain-census-indexer \
  --contract 0xYourContract \
  --start-block 123456 \
  --rpc https://rpc1.example \
  --rpc https://rpc2.example \
  --log-level debug
```

GraphQL endpoint: `http://localhost:8080/graphql`  
Health check: `http://localhost:8080/healthz`

### Run with env vars

```
CONTRACT=0xYourContract \
START_BLOCK=123456 \
RPCS="https://rpc1.example,https://rpc2.example" \
LOG_LEVEL=debug \
go run ./cmd/onchain-census-indexer
```

## Docker usage

### .env file

Copy and edit the example:

```
cp .env.example .env
```

### Build and run

```
make run
```

This will:

- Build the Docker image
- Run the container with `.env`
- Mount `./data` into `/data` inside the container

### Manual docker run

```
docker build -t onchain-census-indexer .

docker run --rm -p 8080:8080 \
  -v "$(pwd)/data:/data" \
  --env-file .env \
  onchain-census-indexer
```

## Configuration

Flags override env vars. Defaults shown where available.

| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `--contract` | `CONTRACT` / `CONTRACT_ADDRESS` | required | Contract address to index |
| `--start-block` | `START_BLOCK` | `0` | Start block (inclusive) |
| `--rpc` (repeat) | `RPCS` / `RPC_ENDPOINTS` | required | RPC endpoints |
| `--db-path` | `DB_PATH` | `data` (local) / `/data` (docker) | DB path |
| `--listen` | `LISTEN_ADDR` / `LISTEN` | `:8080` | HTTP listen address |
| `--poll-interval` | `POLL_INTERVAL` | `5s` | Polling interval |
| `--batch-size` | `BATCH_SIZE` | `2000` | Log batch size |
| `--log-level` | `LOG_LEVEL` | `debug` | Log level |

## Notes

- RPC endpoints must all be on the same chain ID.
- The indexer stores the last indexed block in the database to resume safely on restart.
- GraphQL uses a custom `BigInt` scalar that is serialized as a string.
