package indexer

import (
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"
)

// BatchState accumulates operations across multiple blocks before flushing to DB.
type BatchState struct {
	// Outpoints to delete from DB (UTXOs spent that were already committed)
	UtxoDeletes []string

	// addr_tx entries to insert
	AddrTxEntries []types.AddrTxDoc

	// Undo entries per block
	UndoEntries []types.BlockUndoDoc

	// Accumulated addr_stats deltas (live mode only)
	StatsDeltas map[string]*types.BatchStatsAccum

	BlockCount int
	LastHeight int64
	LastHash   string
}

// NewBatch creates an empty batch state.
func NewBatch() *BatchState {
	return &BatchState{
		UtxoDeletes:   make([]string, 0, 1024),
		AddrTxEntries: make([]types.AddrTxDoc, 0, 4096),
		UndoEntries:   make([]types.BlockUndoDoc, 0, 64),
		StatsDeltas:   make(map[string]*types.BatchStatsAccum),
		BlockCount:    0,
		LastHeight:    -1,
		LastHash:      "",
	}
}
