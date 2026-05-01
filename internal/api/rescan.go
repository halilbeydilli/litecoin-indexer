// Package api - Address rescan functionality.
// Scans blocks to find and insert missing addr_tx entries for a specific address.
package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/halilbeydilli/litecoin-indexer/internal/db"
	"github.com/halilbeydilli/litecoin-indexer/internal/indexer"
	"github.com/halilbeydilli/litecoin-indexer/internal/logger"
	"github.com/halilbeydilli/litecoin-indexer/internal/rpc"
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ============================
// Rescan State Tracking
// ============================

type rescanStatus struct {
	Address     string    `json:"address"`
	Status      string    `json:"status"` // "running", "completed", "failed"
	StartedAt   time.Time `json:"startedAt"`
	FinishedAt  time.Time `json:"finishedAt,omitempty"`
	FirstHeight int64     `json:"firstHeight"`
	LastHeight  int64     `json:"lastHeight"`
	CurrentH    int64     `json:"currentHeight"`
	Scanned     int64     `json:"blocksScanned"`
	Found       int64     `json:"txsFound"`
	Inserted    int64     `json:"txsInserted"`
	Error       string    `json:"error,omitempty"`
}

var (
	activeRescans   sync.Map // address -> *rescanStatus
	rescanHistory   sync.Map // address -> *rescanStatus (last completed)
)

// ============================
// HTTP Handlers
// ============================

// POST /api/address/{address}/rescan
// Starts a background rescan for the given address.
// Does not delete existing data — only inserts missing addr_tx entries.
func handleRescan(w http.ResponseWriter, r *http.Request) {
	address := r.PathValue("address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "address is required")
		return
	}

	// Check if already running
	if _, running := activeRescans.Load(address); running {
		writeError(w, http.StatusConflict, "rescan already in progress for this address")
		return
	}

	// Get height range from existing addr_tx
	ctx := r.Context()

	var firstDoc, lastDoc struct {
		Height int64 `bson:"h"`
	}
	opts := options.FindOne().SetProjection(bson.M{"h": 1})

	// Find first seen height
	err := db.Instance.AddrTx.FindOne(ctx, bson.M{"a": address},
		opts.SetSort(bson.D{{Key: "h", Value: 1}}),
	).Decode(&firstDoc)
	if err != nil {
		writeError(w, http.StatusNotFound, "no addr_tx entries found for this address")
		return
	}

	// Find last seen height
	_ = db.Instance.AddrTx.FindOne(ctx, bson.M{"a": address},
		opts.SetSort(bson.D{{Key: "h", Value: -1}}),
	).Decode(&lastDoc)

	// Also consider current chain tip (may have new blocks since last addr_tx)
	chainTip, _ := rpc.GetBlockCount(ctx)
	if chainTip > lastDoc.Height {
		lastDoc.Height = chainTip
	}

	status := &rescanStatus{
		Address:     address,
		Status:      "running",
		StartedAt:   time.Now(),
		FirstHeight: firstDoc.Height,
		LastHeight:  lastDoc.Height,
	}
	activeRescans.Store(address, status)

	// Start background rescan
	go func() {
		defer activeRescans.Delete(address)

		bgCtx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
		defer cancel()

		err := runRescan(bgCtx, address, status)
		if err != nil {
			status.Error = err.Error()
			status.Status = "failed"
			logger.Warnf(fmt.Sprintf("Rescan failed for %s: %s", address, err.Error()))
		} else {
			status.Status = "completed"
			logger.Infof(fmt.Sprintf("Rescan completed for %s: scanned=%d, found=%d, inserted=%d",
				address, status.Scanned, status.Found, status.Inserted))
		}
		status.FinishedAt = time.Now()
		rescanHistory.Store(address, status)
	}()

	writeJSON(w, map[string]interface{}{
		"message":     "rescan started",
		"address":     address,
		"firstHeight": firstDoc.Height,
		"lastHeight":  lastDoc.Height,
		"totalBlocks": lastDoc.Height - firstDoc.Height + 1,
	})
}

// GET /api/rescan/status/{address}
// Returns the current or most recent rescan status for an address.
func handleRescanStatus(w http.ResponseWriter, r *http.Request) {
	address := r.PathValue("address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "address is required")
		return
	}

	// Check active first
	if val, ok := activeRescans.Load(address); ok {
		writeJSON(w, val)
		return
	}

	// Check history
	if val, ok := rescanHistory.Load(address); ok {
		writeJSON(w, val)
		return
	}

	writeError(w, http.StatusNotFound, "no rescan found for this address")
}

// ============================
// Rescan Logic
// ============================

// runRescan scans all blocks in [firstHeight, lastHeight] for transactions
// involving the given address. Uses a forward UTXO-tracking approach:
// - Track outpoints belonging to this address as we encounter outputs
// - When an input spends a known outpoint, record it as a send
// - Upsert any missing addr_tx entries
func runRescan(ctx context.Context, address string, status *rescanStatus) error {
	first := status.FirstHeight
	last := status.LastHeight

	logger.Infof(fmt.Sprintf("Starting rescan for %s: blocks %d → %d (%d blocks)",
		address, first, last, last-first+1))

	// Track outpoints for this address (built up as we scan forward)
	// key: "txid:vout" → value in satoshis
	knownOutpoints := make(map[string]int64)

	// Seed with current UTXOs from DB (these are confirmed unspent)
	utxoCursor, err := db.Instance.Utxos.Find(ctx, bson.M{"a": address})
	if err == nil {
		var utxos []types.UtxoDoc
		if err := utxoCursor.All(ctx, &utxos); err == nil {
			for _, u := range utxos {
				knownOutpoints[u.ID] = u.Value
			}
		}
		utxoCursor.Close(ctx)
	}
	logger.Infof(fmt.Sprintf("Seeded %d known outpoints from UTXOs for %s", len(knownOutpoints), address))

	// Collect all existing addr_tx txids for quick duplicate check
	existingTxids := make(map[string]bool)
	atCursor, err := db.Instance.AddrTx.Find(ctx,
		bson.M{"a": address},
		options.Find().SetProjection(bson.M{"x": 1}),
	)
	if err == nil {
		for atCursor.Next(ctx) {
			var doc struct {
				TxID string `bson:"x"`
			}
			if atCursor.Decode(&doc) == nil {
				existingTxids[doc.TxID] = true
			}
		}
		atCursor.Close(ctx)
	}
	logger.Infof(fmt.Sprintf("Loaded %d existing addr_tx entries for %s", len(existingTxids), address))

	// Scan blocks
	var totalInserted int64
	var totalFound int64

	for height := first; height <= last; height++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		block, err := rpc.GetBlock(ctx, height, 2)
		if err != nil {
			logger.Warnf(fmt.Sprintf("Rescan: failed to get block %d: %s", height, err.Error()))
			continue
		}

		for txIdx, tx := range block.Tx {
			var received, sent int64

			// Check inputs — consume known outpoints
			for _, vin := range tx.Vin {
				if vin.Coinbase != "" || vin.TxID == "" || vin.Vout == nil {
					continue
				}
				outpoint := fmt.Sprintf("%s:%d", vin.TxID, *vin.Vout)
				if value, ok := knownOutpoints[outpoint]; ok {
					sent += value
					delete(knownOutpoints, outpoint)
				}
			}

			// Check outputs — build outpoints
			for _, vout := range tx.Vout {
				addr := indexer.ExtractAddress(vout.ScriptPubKey)
				if addr == address {
					value := indexer.ToSatoshis(vout.Value)
					received += value
					outpoint := fmt.Sprintf("%s:%d", tx.TxID, vout.N)
					knownOutpoints[outpoint] = value
				}
			}

			// If this address is involved in this TX
			if received > 0 || sent > 0 {
				totalFound++
				status.Found = totalFound

				// Check if we already have this entry
				if existingTxids[tx.TxID] {
					continue // already exists, skip
				}

				// Insert missing entry
				delta := received - sent
				id := fmt.Sprintf("%s:%s", address, tx.TxID)
				_, uErr := db.Instance.AddrTx.UpdateOne(ctx,
					bson.M{"_id": id},
					bson.M{"$set": bson.M{
						"a": address,
						"x": tx.TxID,
						"h": height,
						"i": txIdx,
						"d": delta,
						"r": received,
						"s": sent,
					}},
					options.UpdateOne().SetUpsert(true),
				)
				if uErr != nil {
					logger.Warnf(fmt.Sprintf("Rescan: failed to upsert addr_tx %s:%s: %s", address, tx.TxID, uErr.Error()))
				} else {
					totalInserted++
					status.Inserted = totalInserted
					existingTxids[tx.TxID] = true
					logger.Infof(fmt.Sprintf("Rescan: inserted missing addr_tx for %s TX %s at height %d (delta=%d)",
						address, tx.TxID[:16], height, delta))
				}
			}
		}

		status.Scanned++
		status.CurrentH = height

		// Progress logging every 10000 blocks
		if status.Scanned%10000 == 0 {
			pct := float64(height-first) / float64(last-first+1) * 100
			logger.Infof(fmt.Sprintf("Rescan %s: %.1f%% (height=%d, scanned=%d, found=%d, inserted=%d)",
				address[:10], pct, height, status.Scanned, totalFound, totalInserted))
		}
	}

	// Recompute addr_stats after rescan
	logger.Infof(fmt.Sprintf("Rescan complete for %s, recomputing addr_stats...", address))

	// Delete existing addr_stats so it gets recomputed fresh
	_, _ = db.Instance.AddrStats.DeleteOne(ctx, bson.M{"_id": address})
	_, err = computeAndSaveAddrStats(ctx, address)
	if err != nil {
		logger.Warnf(fmt.Sprintf("Rescan: failed to recompute addr_stats for %s: %s", address, err.Error()))
	}

	return nil
}
