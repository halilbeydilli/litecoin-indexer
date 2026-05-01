// Package types defines all data structures for RPC responses, MongoDB documents,
// and internal working types used throughout the chain indexer.
package types

import "time"

// ============================
// RPC Response Types
// ============================

// RpcBlockchainInfo represents the response from getblockchaininfo.
type RpcBlockchainInfo struct {
	Chain                string  `json:"chain"`
	Blocks               int64   `json:"blocks"`
	Headers              int64   `json:"headers"`
	BestBlockHash        string  `json:"bestblockhash"`
	Difficulty           float64 `json:"difficulty"`
	MedianTime           int64   `json:"mediantime"`
	VerificationProgress float64 `json:"verificationprogress"`
	InitialBlockDownload bool    `json:"initialblockdownload"`
	Pruned               bool    `json:"pruned"`
	SizeOnDisk           int64   `json:"size_on_disk"`
}

// RpcScriptPubKey represents a transaction output script.
type RpcScriptPubKey struct {
	Asm       string   `json:"asm"`
	Hex       string   `json:"hex"`
	Type      string   `json:"type"`
	Address   string   `json:"address,omitempty"`   // Modern Core (single address)
	Addresses []string `json:"addresses,omitempty"` // Older Core or multisig
}

// RpcVout represents a transaction output.
type RpcVout struct {
	Value        float64         `json:"value"`
	N            int             `json:"n"`
	ScriptPubKey RpcScriptPubKey `json:"scriptPubKey"`
}

// RpcVin represents a transaction input.
type RpcVin struct {
	TxID        string        `json:"txid,omitempty"`
	Vout        *int          `json:"vout,omitempty"`
	Coinbase    string        `json:"coinbase,omitempty"`
	ScriptSig   *RpcScriptSig `json:"scriptSig,omitempty"`
	TxInWitness []string      `json:"txinwitness,omitempty"`
	Sequence    uint32        `json:"sequence"`
	Prevout     *RpcVout      `json:"prevout,omitempty"` // verbosity=3 only
}

// RpcScriptSig represents the script signature of a transaction input.
type RpcScriptSig struct {
	Asm string `json:"asm"`
	Hex string `json:"hex"`
}

// RpcTransaction represents a full transaction from getblock verbosity=2.
type RpcTransaction struct {
	TxID     string    `json:"txid"`
	Hash     string    `json:"hash"`
	Version  int       `json:"version"`
	Size     int       `json:"size"`
	VSize    int       `json:"vsize"`
	Weight   int       `json:"weight"`
	LockTime uint32    `json:"locktime"`
	Vin      []RpcVin  `json:"vin"`
	Vout     []RpcVout `json:"vout"`
	Hex      string    `json:"hex,omitempty"`
}

// RpcBlock represents a full block from getblock verbosity=2.
type RpcBlock struct {
	Hash              string           `json:"hash"`
	Confirmations     int64            `json:"confirmations"`
	Size              int              `json:"size"`
	Weight            int              `json:"weight"`
	Height            int64            `json:"height"`
	Version           int              `json:"version"`
	MerkleRoot        string           `json:"merkleroot"`
	Tx                []RpcTransaction `json:"tx"`
	Time              int64            `json:"time"`
	MedianTime        int64            `json:"mediantime"`
	Nonce             uint32           `json:"nonce"`
	Bits              string           `json:"bits"`
	Difficulty        float64          `json:"difficulty"`
	NTx               int              `json:"nTx"`
	PreviousBlockHash string           `json:"previousblockhash,omitempty"`
	NextBlockHash     string           `json:"nextblockhash,omitempty"`
}

// ============================
// MongoDB Document Types
// ============================

// UtxoDoc represents an unspent transaction output in MongoDB.
// _id = "txid:vout" (outpoint)
type UtxoDoc struct {
	ID      string `bson:"_id"` // outpoint: "txid:vout"
	Address string `bson:"a"`   // address
	Value   int64  `bson:"s"`   // value in satoshis
	Height  int64  `bson:"h"`   // block height (created)
}

// AddrTxDoc represents an address-transaction relationship in MongoDB.
type AddrTxDoc struct {
	ID       string `bson:"_id,omitempty"` // compound: "address:txid" (live) or auto ObjectId (initial sync)
	Address  string `bson:"a"`             // address
	TxID     string `bson:"x"`             // txid
	Height   int64  `bson:"h"`             // block height
	TxIndex  int    `bson:"i"`             // tx index within block
	Delta    int64  `bson:"d"`             // net delta in satoshis (+ received, - sent)
	Received int64  `bson:"r"`             // total received in this tx (always >= 0)
	Sent     int64  `bson:"s"`             // total sent in this tx (always >= 0)
}

// AddrStatsDoc represents pre-computed address statistics in MongoDB.
type AddrStatsDoc struct {
	ID        string `bson:"_id"` // address
	Balance   int64  `bson:"b"`   // current balance (satoshis)
	Received  int64  `bson:"r"`   // total received (satoshis)
	Sent      int64  `bson:"s"`   // total sent (satoshis)
	TxCount   int64  `bson:"c"`   // unique tx count
	FirstSeen int64  `bson:"f"`   // first seen block height
	LastSeen  int64  `bson:"l"`   // last seen block height
}

// SyncStateDoc tracks the indexer synchronization state.
type SyncStateDoc struct {
	ID        string    `bson:"_id"`       // coin symbol
	Height    int64     `bson:"height"`    // last fully processed block height
	Hash      string    `bson:"hash"`      // last fully processed block hash
	StartedAt time.Time `bson:"startedAt"` // indexer start time
	UpdatedAt time.Time `bson:"updatedAt"` // last update time
}

// SpentRecord represents a spent UTXO for undo data.
type SpentRecord struct {
	Outpoint string `bson:"o"` // outpoint "txid:vout"
	Address  string `bson:"a"` // address
	Value    int64  `bson:"s"` // value satoshis
	Height   int64  `bson:"h"` // original create height
}

// StatsDelta represents addr_stats delta for undo data.
type StatsDelta struct {
	Address  string `bson:"a"` // address
	Balance  int64  `bson:"b"` // balance delta
	Received int64  `bson:"r"` // received delta
	Sent     int64  `bson:"s"` // sent delta
	TxCount  int64  `bson:"c"` // tx count delta
}

// BlockUndoDoc stores undo data for reorg handling.
type BlockUndoDoc struct {
	ID     int64         `bson:"_id"`    // block height
	Hash   string        `bson:"hash"`   // block hash
	Spent  []SpentRecord `bson:"spent"`  // UTXOs spent in this block
	Deltas []StatsDelta  `bson:"deltas"` // addr_stats deltas applied
}

// RpcMempoolEntry represents an entry from getmempoolentry.
type RpcMempoolEntry struct {
	Fees struct {
		Base float64 `json:"base"`
	} `json:"fees"`
	Fee   float64 `json:"fee"` // fallback for older nodes
	VSize int     `json:"vsize"`
}

// MempoolAddrDelta represents an address delta in a mempool transaction.
type MempoolAddrDelta struct {
	Address string `bson:"a"` // address
	Delta   int64  `bson:"d"` // delta in satoshis
}

// MempoolTxDoc represents a mempool transaction in MongoDB.
type MempoolTxDoc struct {
	ID        string             `bson:"_id"`       // txid
	Addrs     []MempoolAddrDelta `bson:"addrs"`     // affected addresses with deltas
	Fee       int64              `bson:"fee"`       // fee in satoshis
	Size      int                `bson:"size"`      // tx vsize
	FirstSeen time.Time          `bson:"firstSeen"` // first seen time
}

// ============================
// Internal Working Types
// ============================

// ResolvedUtxo represents resolved UTXO info during block processing.
type ResolvedUtxo struct {
	Address string
	Value   int64
	Height  int64
}

// AddrTxAccum accumulates per-address per-tx data during block processing.
type AddrTxAccum struct {
	Address  string
	TxID     string
	Height   int64
	TxIndex  int
	Delta    int64
	Received int64
	Sent     int64
}

// BlockStatsAccum accumulates per-address stats during block processing.
type BlockStatsAccum struct {
	Balance  int64
	Received int64
	Sent     int64
	TxIDs    map[string]struct{} // set of unique txids
}

// BatchStatsAccum accumulates per-address stats across a batch.
type BatchStatsAccum struct {
	Balance  int64
	Received int64
	Sent     int64
	TxIDs    map[string]struct{}
	MinH     int64
	MaxH     int64
}
