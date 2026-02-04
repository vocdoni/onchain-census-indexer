# Onchain Census Indexer

A Go service that indexes the `WeightChanged` event from multiple contracts (across multiple chains) using rotating RPC endpoints, stores events in a local Pebble database, and serves them through a GraphQL API.

## What it does

- Indexes `WeightChanged(account, previousWeight, newWeight)` from one or more contracts.
- Supports multiple chains in the same process.
- Persists events locally with resume support per contract.
- Serves GraphQL per contract at `/{chainID}/{contractAddress}/graphql`.
- Root `/` shows a plain‑text list of available GraphQL endpoints.

## Architecture

- **Indexer service**: polls the database for contracts and runs one indexer per contract.
- **API service**: exposes GraphQL endpoints per contract and a registration endpoint.
- Both services only depend on the database; main wires config and services.

Key dependencies:

- `github.com/vocdoni/davinci-node/web3/rpc` (RPC pool + rotation)
- `github.com/vocdoni/davinci-node/db/metadb` with `db.TypePebble`
- `github.com/graphql-go/graphql`

## Contract format

Contracts are provided as a single string with entries separated by commas/spaces/semicolons:

```
chainID:contractAddress:blockNumber
```

Example:

```
42220:0xYourContract:123456,1:0xAnotherContract:987654
```

Each entry defines:
- **chainID**: the EVM chain ID
- **contractAddress**: 0x address
- **blockNumber**: start block (inclusive)

## GraphQL

**Endpoint:** `http://localhost:8080/{chainID}/{contractAddress}/graphql`  
**Health check:** `http://localhost:8080/healthz`  
**Root listing:** `http://localhost:8080/`

### Register new contracts (HTTP)

```
POST /contracts
Content-Type: application/json

{
  "chainId": 11155111,
  "contract": "0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29",
  "startBlock": 10085464
}
```

Response:

```
{
  "chainId": 11155111,
  "contract": "0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29",
  "endpoint": "/11155111/0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29/graphql"
}
```

### Schema (reference)

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

### Example query (reference)

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

## Contract bindings

This service uses generated Go bindings for the census validator contract:

- Source: `github.com/vocdoni/davinci-contracts/golang-types/ICensusValidator.go`
- Event: `WeightChanged(address indexed account, uint88 previousWeight, uint88 newWeight)`

## Configuration

Flags override environment variables. Defaults shown where available.

| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `--contracts` | `CONTRACTS` | optional | Comma/space/semicolon‑separated `chainID:contractAddress:blockNumber` entries |
| `--rpc` (repeat) | `RPCS` / `RPC_ENDPOINTS` | optional | RPC endpoints (can cover multiple chain IDs). If omitted, endpoints are pulled from chainlist automatically |
| `--db.path` | `DB_PATH` | `data` (local) / `/data` (docker) | DB path |
| `--http.listen` | `LISTEN_ADDR` / `LISTEN` | `:8080` | HTTP listen address |
| `--http.corsAllowedOrigins` | `CORS_ALLOWED_ORIGINS` | `*` | Allowed CORS origins (comma/space/semicolon separated) |
| `--indexer.pollInterval` | `POLL_INTERVAL` | `5s` | Polling interval |
| `--indexer.batchSize` | `BATCH_SIZE` | `2000` | Log batch size |
| `--log.level` | `LOG_LEVEL` | `debug` | Log level |

Notes:
- `--contract` is deprecated in favor of `--contracts`.
- For env values, use comma‑separated lists (avoid wrapping in quotes that become part of the value).
- If `RPCS` is omitted, the service uses chainlist.org to auto-discover healthy RPCs for each chain ID.
- New contracts registered via `POST /contracts` are persisted in the DB and picked up by the indexer on the next sync interval (uses `indexer.pollInterval`).

## Local usage

### Requirements

- Go 1.21+ (module targets Go 1.25.x)
- C toolchain if building with cgo (macOS: `xcode-select --install`)

### Run with flags

```
go run ./cmd/onchain-census-indexer \
  --contracts 42220:0xYourContract:123456,1:0xAnotherContract:987654 \
  --rpc https://rpc1.example \
  --rpc https://rpc2.example \
  --log.level debug
```

### Run with env vars

```
CONTRACTS=42220:0xYourContract:123456,1:0xAnotherContract:987654 \
RPCS=https://rpc1.example,https://rpc2.example \
LOG_LEVEL=debug \
go run ./cmd/onchain-census-indexer
```

## Docker usage

### .env file

```
cp .env.example .env
```

### Build and run

```
make run
```

### Manual docker run

```
docker build -t onchain-census-indexer .

docker run --rm -p 8080:8080 \
  -v "$(pwd)/data:/data" \
  --env-file .env \
  onchain-census-indexer
```

## Notes

- RPC endpoints must cover every chain ID listed in `CONTRACTS`.
- The indexer stores the last indexed block per contract to resume safely on restart.
- `BigInt` values are serialized as strings in GraphQL responses.
- Ordering by `blockNumber` follows storage order (chain ID + contract + block number).
