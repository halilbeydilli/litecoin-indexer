# Litecoin Indexer

High-performance UTXO blockchain indexer written in Go. Indexes any Bitcoin-based cryptocurrency into MongoDB for blazing-fast address queries, balance lookups, and transaction history — no Electrum/Fulcrum required.

[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0+-47A248?style=flat&logo=mongodb)](https://www.mongodb.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Donate BTC](https://img.shields.io/badge/Donate-BTC-orange?style=flat&logo=bitcoin)](https://github.com/halilbeydilli/litecoin-indexer#support--donations)

## Why?

Electrum servers (ElectrumX, Fulcrum) are the standard way to query UTXO blockchains by address, but they have critical limitations:

| Problem | Electrum/Fulcrum | Chain Indexer |
|---------|-----------------|---------------|
| Whale addresses (2M+ TXs) | **Timeout / crash** | **< 1ms** |
| Balance + stats query | Multiple round-trips | **Single query < 1ms** |
| Dependency | Requires full Electrum stack | Just a full node + MongoDB |
| Deployment | Complex Python/C++ setup | **Single Go binary** |
| Multi-coin | Separate instance configs | **Same binary, different `.env`** |

## Features

- **Universal**: Works with any Bitcoin-based coin — BTC, LTC, DOGE, DASH, ZEC, XVG, DGB, RVN, and 20+ more
- **Fast**: In-memory UTXO cache eliminates ~80-90% of database reads during sync
- **Crash-safe**: Batch writes with undo data; automatic recovery on restart
- **Reorg-safe**: Detects chain reorganizations and rolls back cleanly (configurable depth)
- **Single binary**: ~10 MB statically compiled Go executable, zero runtime dependencies
- **REST API**: Built-in HTTP API server for address balance, UTXOs, history, mempool
- **Mempool tracking**: Polls and indexes unconfirmed transactions per-address
- **Pre-computed stats**: Balance, total received/sent, tx count — always O(1) lookup
- **Initial sync optimized**: Deferred indexes, batch flushing, UTXO cache cancel-out

## Supported Coins

Any cryptocurrency built on Bitcoin Core's codebase that supports these standard RPC methods:

| RPC Method | Purpose |
|-----------|---------|
| `getblockcount` | Chain height |
| `getblockhash` | Block hash by height |
| `getblock` (verbosity=2) | Full block with all transactions |
| `getrawtransaction` | Raw transaction details |
| `getrawmempool` | Mempool transaction list |
| `getblockchaininfo` | Node status |

### Tested & Built-in Support

P2PKH address version bytes are auto-detected for these coins:

| Coin | Symbol | Default RPC Port |
|------|--------|-----------------|
| Bitcoin | BTC | 8332 |
| Litecoin | LTC | 9332 |
| Dogecoin | DOGE | 22555 |
| Dash | DASH | 9998 |
| Verge | XVG | 20102 |
| Zcash | ZEC | 8232 |
| Bitcoin Cash | BCH | 8332 |
| Bitcoin SV | BSV | 8332 |
| DigiByte | DGB | 14022 |
| Ravencoin | RVN | 8766 |
| Bitcoin Gold | BTG | 8338 |
| PIVX | PIVX | 51473 |
| Firo (Zcoin) | FIRO | 8888 |
| Syscoin | SYS | 8370 |
| Groestlcoin | GRS | 1441 |
| Vertcoin | VTC | 5888 |
| MonaCoin | MONA | 9402 |
| Namecoin | NMC | 8336 |
| Peercoin | PPC | 9902 |
| Feathercoin | FTC | 9337 |
| Qtum | QTUM | 3889 |

**Any other Bitcoin fork** works too — just set `P2PKH_VERSION` in your `.env` file.

## Quick Start

### Prerequisites

- **Go 1.23+** ([install](https://go.dev/dl/))
- **MongoDB 6.0+** ([install](https://www.mongodb.com/docs/manual/installation/))
- **A fully synced Bitcoin-based full node** with:
  - `txindex=1` in the node's config (required for transaction lookups)
  - RPC enabled with username/password

### 1. Clone & Build

```bash
git clone https://github.com/halilbeydilli/litecoin-indexer.git
cd litecoin-indexer

# Build (produces a single binary)
go build -o chain-indexer ./cmd/indexer

# Or use Make
make build
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your node's RPC credentials and coin settings
```


#### Example: Bitcoin

```env
COIN_NAME=bitcoin
COIN_SYMBOL=BTC
RPC_HOST=127.0.0.1
RPC_PORT=8332
RPC_USER=bitcoinrpc
RPC_PASS=your-rpc-password
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DB=btc_index
API_PORT=3101
```

#### Example: Litecoin

```env
COIN_NAME=litecoin
COIN_SYMBOL=LTC
RPC_HOST=127.0.0.1
RPC_PORT=9332
RPC_USER=litecoinrpc
RPC_PASS=your-rpc-password
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DB=ltc_index
API_PORT=3100
```

#### Example: Dogecoin

```env
COIN_NAME=dogecoin
COIN_SYMBOL=DOGE
RPC_HOST=127.0.0.1
RPC_PORT=22555
RPC_USER=dogerpc
RPC_PASS=your-rpc-password
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DB=doge_index
API_PORT=3102
```

#### Example: Dash

```env
COIN_NAME=dash
COIN_SYMBOL=DASH
RPC_HOST=127.0.0.1
RPC_PORT=9998
RPC_USER=dashrpc
RPC_PASS=your-rpc-password
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DB=dash_index
API_PORT=3103
```

#### Example: Custom / Unlisted Coin

```env
COIN_NAME=mycoin
COIN_SYMBOL=MYC
RPC_HOST=127.0.0.1
RPC_PORT=12345
RPC_USER=mycoirpc
RPC_PASS=your-rpc-password
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DB=myc_index
API_PORT=3104
# Set P2PKH version byte manually (decimal) for unlisted coins
# Find it in your coin's chainparams.cpp → base58Prefixes[PUBKEY_ADDRESS]
P2PKH_VERSION=50
```

### 3. Run

```bash
# Full mode: indexer + API + mempool watcher
./chain-indexer

# API only (no indexing)
./chain-indexer --api-only

# Indexer only (no API server)
./chain-indexer --no-api

# Skip mempool watcher
./chain-indexer --no-mempool

# Rebuild address statistics
./chain-indexer --rebuild-stats
```

### 4. Cross-Compile for Linux (VPS Deployment)

```bash
# From Windows/Mac, build a Linux binary
GOOS=linux GOARCH=amd64 go build -o chain-indexer ./cmd/indexer

# Or use Make
make build-linux

# Upload to server and run
scp chain-indexer user@server:/opt/chain-indexer/
scp .env user@server:/opt/chain-indexer/
ssh user@server "cd /opt/chain-indexer && ./chain-indexer"
```

## Architecture

```
┌──────────────────┐           ┌──────────────────────────────┐
│  Bitcoin/Litecoin │  JSON-RPC │       Chain Indexer (Go)      │
│    Full Node      │◄─────────►│                              │
│  (any UTXO coin)  │           │  ┌─────────┐ ┌───────────┐  │
└──────────────────┘           │  │ Indexer  │ │  Mempool   │  │
                                │  │ (sync)  │ │  Watcher   │  │
                                │  └────┬────┘ └─────┬─────┘  │
                                │       │             │        │
                                │       ▼             ▼        │
                                │  ┌─────────────────────────┐ │
                                │  │     MongoDB Driver       │ │
                                │  └────────────┬────────────┘ │
                                │               │              │
                                │  ┌────────────┴────────────┐ │
                                │  │     REST API Server      │ │
                                │  │     (net/http)           │ │
                                │  └─────────────────────────┘ │
                                └──────────────┬───────────────┘
                                               │
                                               ▼
                                ┌──────────────────────────────┐
                                │           MongoDB            │
                                │                              │
                                │  ├─ utxos      (UTXO set)   │
                                │  ├─ addr_tx    (TX history)  │
                                │  ├─ addr_stats (balances)    │
                                │  ├─ sync_state (checkpoint)  │
                                │  ├─ block_undo (reorg data)  │
                                │  └─ mempool_tx (pending TXs) │
                                └──────────────────────────────┘
```

### Project Structure

```
litecoin-indexer/
├── cmd/indexer/main.go          # Entry point, CLI flags, orchestration
├── internal/
│   ├── config/config.go         # .env loading, global configuration
│   ├── logger/logger.go         # Structured logger (zero dependencies)
│   ├── rpc/rpc.go               # JSON-RPC client with retry & connection pool
│   ├── db/db.go                 # MongoDB connection, collections, indexes
│   ├── indexer/
│   │   ├── indexer.go           # Core: 5-phase block processing, flush, reorg
│   │   ├── cache.go             # Two-tier UTXO cache (pending/committed)
│   │   ├── batch.go             # Batch state accumulator
│   │   └── helpers.go           # Address extraction, P2PK derivation, base58
│   ├── mempool/mempool.go       # Mempool watcher (polling + sync)
│   └── api/api.go               # REST API server (net/http)
├── pkg/types/types.go           # All shared type definitions
├── go.mod
├── Makefile
└── .env.example
```

## API Reference

All endpoints return JSON. Amounts are in **satoshis** (1 BTC = 100,000,000 satoshis).

### `GET /api/address/:address/balance`

Returns pre-computed balance and statistics. **O(1) lookup, < 1ms** regardless of address size.

```json
{
  "address": "LcGKj65WFfV8VxL2F1f5dSEUhBQWzWgCRH",
  "balance": 5430000,
  "received": 99000000,
  "sent": 93570000,
  "txCount": 47,
  "firstSeen": 100200,
  "lastSeen": 880012
}
```

### `GET /api/address/:address/utxos`

Returns all unspent transaction outputs for an address.

**Query parameters:** `?limit=1000&offset=0`

```json
{
  "address": "LcGKj65WFfV8VxL2F1f5dSEUhBQWzWgCRH",
  "count": 3,
  "utxos": [
    { "txid": "abc123...", "vout": 0, "value": 1000000, "height": 880010 },
    { "txid": "def456...", "vout": 2, "value": 2430000, "height": 880012 },
    { "txid": "ghi789...", "vout": 1, "value": 2000000, "height": 880012 }
  ]
}
```

### `GET /api/address/:address/history`

Returns paginated transaction history, sorted by block height.

**Query parameters:** `?limit=50&offset=0&order=desc`

```json
{
  "address": "LcGKj65WFfV8VxL2F1f5dSEUhBQWzWgCRH",
  "total": 47,
  "offset": 0,
  "limit": 50,
  "items": [
    { "txid": "abc123...", "delta": 5430000, "height": 880012, "blockIndex": 3 },
    { "txid": "xyz999...", "delta": -1000000, "height": 880010, "blockIndex": 7 }
  ]
}
```

### `GET /api/address/:address/mempool`

Returns pending (unconfirmed) transactions affecting this address.

```json
{
  "address": "LcGKj65WFfV8VxL2F1f5dSEUhBQWzWgCRH",
  "count": 1,
  "items": [
    { "txid": "mem123...", "delta": 500000, "size": 225, "firstSeen": "2026-01-15T10:30:00Z" }
  ]
}
```

### `GET /api/tx/:txid`

Returns indexed address information for a transaction.

```json
{
  "txid": "abc123...",
  "height": 880012,
  "blockIndex": 3,
  "addresses": [
    { "address": "LcGKj65WFfV8VxL2F1f5dSEUhBQWzWgCRH", "delta": 5430000 },
    { "address": "LSenderAddress...", "delta": -5431200 }
  ]
}
```

### `GET /api/status`

Returns sync status and database statistics.

```json
{
  "coin": "LTC",
  "coinName": "litecoin",
  "syncedHeight": 880012,
  "nodeHeight": 880015,
  "progress": "99.99%",
  "isSynced": false,
  "lastUpdate": "2026-02-12T14:30:00Z",
  "database": {
    "utxos": 4523000,
    "addrTx": 89000000,
    "addrStats": 12500000,
    "mempoolTx": 1523
  }
}
```

### `GET /health`

Health check endpoint.

```json
{ "status": "ok", "coin": "LTC" }
```

## How It Works

### 5-Phase Block Processing

Each block is processed through 5 sequential phases for maximum cache efficiency:

```
Phase 1: Collect Outputs
  │  Scan all TX outputs → local UTXO map
  ▼
Phase 2: Classify Inputs (3-tier lookup)
  │  For each input:
  │    1. Same-block output? → resolved locally (zero I/O)
  │    2. In UTXO cache?    → resolved from memory (~80-90% hit rate)
  │    3. Cache miss         → mark for DB fetch
  ▼
Phase 3: Batch DB Fetch (cache misses only)
  │  Single sorted $in query for all misses
  │  RPC fallback for P2PK/early coinbase
  ▼
Phase 4: Build Operations
  │  Accumulate addr_tx entries, stats deltas, undo data
  │  Cache cancel-out: same-batch create+spend = zero DB I/O
  ▼
Phase 5: Accumulate into Batch (no DB writes yet)
  │  Everything buffered in memory
  │  Flush every BATCH_SIZE blocks (initial sync) or every block (live)
```

### Initial Sync vs Live Mode

| Feature | Initial Sync | Live Mode |
|---------|-------------|----------|
| UTXO Cache | Active (5M entries, ~1GB) | Active |
| Batch Size | 350 blocks per flush | 1 block per flush |
| Secondary Indexes | Deferred (faster writes) | Active |
| addr_stats | Deferred (rebuilt at end) | Real-time `$inc` |
| addr_tx writes | `insertMany` (fast) | `upsert` (idempotent) |
| Reorg checks | Skipped | Every cycle |

### Crash Safety

The write order is carefully designed for crash recovery:

1. `block_undo` written first (recovery data)
2. `utxos` inserted
3. `utxos` deleted (spent)
4. `addr_tx` written
5. `addr_stats` updated
6. **`sync_state` updated LAST** (commit marker)

On crash, the indexer detects uncommitted blocks by comparing `sync_state` with `block_undo`, restores spent UTXOs, cleans stray data, and resumes.

### Reorg Handling

1. Every cycle: compare our hash with the node's hash at the same height
2. Mismatch detected → walk back to find the fork point
3. Roll back each block using `block_undo` data (restore UTXOs, reverse stats)
4. Resume indexing from the fork point

Safety depth: last 200 blocks (configurable via `REORG_SAFETY_DEPTH`).

## MongoDB Collections

| Collection | Purpose | Key Index |
|-----------|---------|-----------|
| `utxos` | Live UTXO set | `_id` (outpoint), `a` (address) |
| `addr_tx` | Address↔TX history | `a + h + i` (address, height, index) |
| `addr_stats` | Pre-computed balances | `_id` (address), `b` (balance desc) |
| `sync_state` | Sync checkpoint | `_id` (coin symbol) |
| `block_undo` | Reorg undo data | `_id` (block height) |
| `mempool_tx` | Pending transactions | `addrs.a` (address), TTL 72h |

## Configuration Reference

| Variable | Description | Default |
|----------|------------|---------|
| `COIN_NAME` | Coin full name (display only) | `litecoin` |
| `COIN_SYMBOL` | Coin ticker symbol | `LTC` |
| `RPC_HOST` | Node RPC hostname | `127.0.0.1` |
| `RPC_PORT` | Node RPC port | `9332` |
| `RPC_USER` | Node RPC username | `litecoinrpc` |
| `RPC_PASS` | Node RPC password | (empty) |
| `RPC_TIMEOUT` | RPC call timeout (ms) | `120000` |
| `MONGO_URI` | MongoDB connection URI | `mongodb://127.0.0.1:27017` |
| `MONGO_DB` | MongoDB database name | `ltc_index_database` |
| `API_PORT` | REST API listen port | `3100` |
| `API_HOST` | REST API bind address | `0.0.0.0` |
| `START_HEIGHT` | Block height to start indexing from | `0` |
| `LOG_INTERVAL` | Log progress every N blocks | `100` |
| `REORG_SAFETY_DEPTH` | Keep undo data for last N blocks | `200` |
| `MEMPOOL_POLL_INTERVAL` | Mempool poll interval (ms) | `10000` |
| `BLOCKS_PER_TICK` | Blocks per cycle in live mode | `10` |
| `WRITE_CONCERN` | MongoDB write concern (0=fast, 1=safe) | `0` |
| `BATCH_SIZE` | Blocks per DB flush (initial sync) | `350` |
| `UTXO_CACHE_MAX` | Max UTXO cache entries | `5000000` |
| `P2PKH_VERSION` | P2PKH address version byte (decimal) | auto-detect |

## Performance

### Initial Sync Estimates

| Coin | Blocks | Estimated Time (SSD) | Disk Space |
|------|--------|---------------------|------------|
| Bitcoin | ~880K | 12-48 hours | 200-400 GB |
| Litecoin | ~2.8M | 3-8 hours | 30-50 GB |
| Dogecoin | ~5.2M | 6-16 hours | 50-80 GB |
| Dash | ~2.1M | 2-6 hours | 20-40 GB |

*Times depend on hardware (SSD mandatory), MongoDB cache, and node RPC speed.*

### Query Performance

| Query | Response Time |
|-------|--------------|
| Balance (any address size) | **< 1ms** |
| UTXO list (1000 items) | **< 2ms** |
| TX history (50 items) | **< 3ms** |
| Total received (2M+ TX address) | **< 1ms** |

### System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 core | 2+ cores |
| RAM | 2 GB | 4-8 GB |
| Disk | SSD required | NVMe SSD |
| MongoDB | 6.0+ | 7.0+ |

## Running Multiple Coins

Run separate instances with different `.env` files and API ports:

```bash
# Terminal 1: Litecoin
cp .env.ltc .env && ./chain-indexer

# Terminal 2: Bitcoin
cp .env.btc .env && ./chain-indexer

# Terminal 3: Dogecoin
cp .env.doge .env && ./chain-indexer
```

Or use environment variables directly:

```bash
COIN_SYMBOL=BTC RPC_PORT=8332 MONGO_DB=btc_index API_PORT=3101 ./chain-indexer
```

### Systemd Service (Linux)

```ini
# /etc/systemd/system/chain-indexer-ltc.service
[Unit]
Description=Chain Indexer (Litecoin)
After=mongod.service litecoind.service
Wants=mongod.service

[Service]
Type=simple
User=indexer
WorkingDirectory=/opt/chain-indexer
EnvironmentFile=/opt/chain-indexer/.env.ltc
ExecStart=/opt/chain-indexer/chain-indexer
Restart=always
RestartSec=10
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable chain-indexer-ltc
sudo systemctl start chain-indexer-ltc
sudo journalctl -u chain-indexer-ltc -f  # View logs
```

## Adding Support for a New Coin

1. **Ensure your coin's node supports standard Bitcoin RPC** (`getblock` with verbosity=2)
2. **Find the P2PKH version byte** in your coin's source code:
   ```
   src/chainparams.cpp → base58Prefixes[PUBKEY_ADDRESS]
   ```
3. **Create `.env`**:
   ```env
   COIN_NAME=mycoin
   COIN_SYMBOL=MYC
   RPC_PORT=12345
   RPC_USER=mycoirpc
   RPC_PASS=password
   MONGO_DB=myc_index
   P2PKH_VERSION=50   # decimal value of your coin's version byte
   ```
4. **Run**: `./chain-indexer`

That's it. No code changes needed.

## Troubleshooting

### "Cannot connect to RPC node"
- Verify your node is running and fully synced
- Check `rpcuser`/`rpcpassword` in your node's config file
- Ensure `rpcallowip` includes the indexer's IP
- Test with: `curl --user user:pass --data '{"method":"getblockcount"}' http://host:port/`

### "UTXO not found for input"
- Your node needs `txindex=1` — add it to the node config and run `-reindex`
- This only affects P2PK transactions (early blocks); later blocks work without txindex

### Slow initial sync
- Increase `BATCH_SIZE` (default: 350, try 500-1000)
- Increase `UTXO_CACHE_MAX` if you have spare RAM (5M entries ≈ 1GB)
- Ensure MongoDB is on SSD with adequate WiredTiger cache
- Set `WRITE_CONCERN=0` for faster writes during initial sync

### Rebuild address statistics
If `addr_stats` appears inconsistent after a crash:
```bash
./chain-indexer --rebuild-stats
```

### Reset and re-index from scratch
```bash
# Drop the database
mongosh --eval "use ltc_index; db.dropDatabase()"

# Restart the indexer
./chain-indexer
```

## Contributing

Contributions are welcome. Please open an issue first to discuss proposed changes.

## Support & Donations

If this project saved you time or infrastructure cost, consider buying me a coffee ☕

| Network | Address |
|---------|---------|
| **Bitcoin (BTC)** | `15LdNmHMesfckw3XmEj2dQkENCEuNwDWX8` |
| **Litecoin (LTC)** | `LcVbjF9u3fktY5HPWTtky6NhQBJHwaTvZT` |
| **Ethereum / BNB / USDT (ERC-20 / BEP-20)** | `0x405b04b771A586332C20532e97cffCA8F03AD38a` |

## License

MIT License — see [LICENSE](LICENSE) for details.
