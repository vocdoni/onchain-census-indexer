# Onchain Census Indexer

A Go service that indexes the `WeightChanged` event from multiple contracts (across multiple chains) using rotating RPC endpoints, stores events in a local Pebble database, and serves them through a GraphQL API.

## What it does

- Indexes `WeightChanged(account, previousWeight, newWeight)` from one or more contracts.
- Supports multiple chains in the same process.
- Persists events locally with resume support per contract.
- Verifies indexed ranges with a second pass before marking them synced.
- Continuously rescans a rolling tail window to recover late or previously missed events.
- Serves GraphQL per contract at `/{chainID}/{contractAddress}/graphql`.
- Serves the same indexed event data as JSON at `/{chainID}/{contractAddress}`.
- Root `/` shows a JSON list of available GraphQL and JSON endpoints plus sync status per contract.

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
chainID:contractAddress:blockNumber:expiresAt
```

Example:

```
42220:0xYourContract:123456:2026-03-01T12:00:00Z,1:0xAnotherContract:987654:2026-03-15T00:00:00Z
```

Each entry defines:
- **chainID**: the EVM chain ID
- **contractAddress**: 0x address
- **blockNumber**: start block (inclusive)
- **expiresAt**: RFC3339 timestamp when contract data must be purged

## GraphQL

**GraphQL endpoint:** `http://localhost:8080/{chainID}/{contractAddress}/graphql`  
**JSON endpoint:** `http://localhost:8080/{chainID}/{contractAddress}`  
**Health check:** `http://localhost:8080/healthz`  
**Root listing:** `http://localhost:8080/` (includes `info.synced`)

### Root endpoint example

Request:

```
GET /
```

Example response:

```
[
  {
    "info": {
      "chainId": 11155111,
      "address": "0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29",
      "startBlock": 10085464,
      "expiresAt": "2026-03-01T12:00:00Z",
      "synced": true
    },
    "endpoint": "/11155111/0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29/graphql",
    "jsonEndpoint": "/11155111/0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29"
  }
]
```

### Register new contracts (HTTP)

```
POST /contracts
Content-Type: application/json

{
  "chainId": 11155111,
  "address": "0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29",
  "startBlock": 10085464,
  "expiresAt": "2026-03-01T12:00:00Z"
}
```

Response:

```
{
  "chainId": 11155111,
  "contract": "0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29",
  "endpoint": "/11155111/0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29/graphql",
  "jsonEndpoint": "/11155111/0x2E6C3D4ED7dA2bAD613A3Ea30961db7bF8452b29",
  "expiresAt": "2026-03-01T12:00:00Z"
}
```

### JSON endpoint

Request:

```
GET /{chainID}/{contractAddress}?first=100&skip=0&orderBy=blockNumber&orderDirection=asc
```

Query params:

- `first`: optional non-negative integer. If omitted or `0`, returns all stored events for the contract.
- `skip`: optional non-negative integer. Defaults to `0`.
- `orderBy`: optional. Only `blockNumber` is supported.
- `orderDirection`: optional. `asc` or `desc` (defaults to `asc`).

Example response:

```
{
  "weightChangeEvents": [
    {
      "account": {
        "id": "0x1111111111111111111111111111111111111111"
      },
      "previousWeight": "10",
      "newWeight": "15",
      "blockNumber": "123456"
    }
  ]
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
| `--contracts` | `CONTRACTS` | optional | Comma/space/semicolon‑separated `chainID:contractAddress:blockNumber:expiresAt` entries |
| `--rpc` (repeat) | `RPCS` / `RPC_ENDPOINTS` | optional | RPC endpoints (can cover multiple chain IDs). If omitted, endpoints are pulled from chainlist automatically |
| `--db.path` | `DB_PATH` | `data` (local) / `/data` (docker) | DB path |
| `--http.address` | `LISTEN_ADDR` / `ADDRESS` | `0.0.0.0` | HTTP listen address |
| `--http.port` | `LISTEN_PORT` / `PORT` | `8080` | HTTP listen port |
| `--http.corsAllowedOrigins` | `CORS_ALLOWED_ORIGINS` | `*` | Allowed CORS origins (comma/space/semicolon separated) |
| `--indexer.pollInterval` | `POLL_INTERVAL` | `5s` | Event polling interval |
| `--indexer.contractSyncInterval` | `CONTRACT_SYNC_INTERVAL` | `1s` | Contract reconciliation and expiration purge interval |
| `--indexer.batchSize` | `BATCH_SIZE` | `2000` | Log batch size |
| `--indexer.verifyBatchSize` | `VERIFY_BATCH_SIZE` | `indexer.batchSize` | Verification and tail-rescan batch size |
| `--indexer.confirmations` | `CONFIRMATIONS` | `12` | Number of tip blocks excluded from verification/sync status |
| `--indexer.tailRescanDepth` | `TAIL_RESCAN_DEPTH` | `indexer.verifyBatchSize` | Depth of the verified tail window continuously rescanned |
| `--log.level` | `LOG_LEVEL` | `debug` | Log level |

Notes:
- `--contract` is deprecated in favor of `--contracts`.
- For env values, use comma‑separated lists (avoid wrapping in quotes that become part of the value).
- If `RPCS` is omitted, the service uses chainlist.org to auto-discover healthy RPCs for each chain ID.
- New contracts registered via `POST /contracts` are persisted in the DB and picked up by the indexer on the next contract sync interval (uses `indexer.contractSyncInterval`).
- If a contract is saved with `startBlock: 0` (or omitted in `POST /contracts`), the indexer calculates the contract creation block on first registration and persists it in the DB.
- `expiresAt` is required. The contract remains available until that timestamp (RFC3339). After expiration, the contract metadata, sync state, and indexed events are purged from the DB, and the store is compacted to reclaim disk space.
- The indexer performs a first pass, a verification pass, and then rolling tail rescans. `info.synced` becomes `true` only when verified progress reaches `head - confirmations`.

## Local usage

### Requirements

- Go 1.21+ (module targets Go 1.25.x)
- C toolchain if building with cgo (macOS: `xcode-select --install`)

### Run with flags

```
go run ./cmd/onchain-census-indexer \
  --contracts 42220:0xYourContract:123456:2026-03-01T12:00:00Z,1:0xAnotherContract:987654:2026-03-15T00:00:00Z \
  --rpc https://rpc1.example \
  --rpc https://rpc2.example \
  --log.level debug
```

### Run with env vars

```
CONTRACTS=42220:0xYourContract:123456:2026-03-01T12:00:00Z,1:0xAnotherContract:987654:2026-03-15T00:00:00Z \
RPCS=https://rpc1.example,https://rpc2.example \
LOG_LEVEL=debug \
go run ./cmd/onchain-census-indexer
```

## Docker usage

### .env file

```
cp .env.example .env
```

### Docker Compose (Traefik + Let's Encrypt + Watchtower)

1. Configure runtime and compose variables in `.env`:

```
cp .env.example .env
```

2. Create the Let's Encrypt storage file with strict permissions:

```
mkdir -p letsencrypt
touch letsencrypt/acme.json
chmod 600 letsencrypt/acme.json
```

3. Start the stack:

```
docker compose up -d
```

The indexer will be served through Traefik at `https://$DOMAIN`.
The dev indexer will be served through Traefik at `https://$DEV_DOMAIN`.

Both `DOMAIN` and `DEV_DOMAIN` must resolve to the same host running Traefik, or Let's Encrypt will not issue certificates for them.

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
- The indexer also stores verified progress per contract and keeps rescanning the recent verified tail to repair incomplete RPC responses.
- `BigInt` values are serialized as strings in GraphQL responses.
- Ordering by `blockNumber` follows storage order (chain ID + contract + block number).
