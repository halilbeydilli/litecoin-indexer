// Package mempool monitors the node's mempool and maintains a local mempool_tx collection.
// Enables per-address mempool queries which Bitcoin/Litecoin Core doesn't support natively.
package mempool

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/halilbeydilli/litecoin-indexer/internal/config"
	"github.com/halilbeydilli/litecoin-indexer/internal/db"
	"github.com/halilbeydilli/litecoin-indexer/internal/logger"
	"github.com/halilbeydilli/litecoin-indexer/internal/rpc"
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Watcher polls the node's mempool and syncs it to MongoDB.
type Watcher struct {
	running    bool
	knownTxIDs map[string]struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewWatcher creates a new mempool watcher.
func NewWatcher() *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{
		knownTxIDs: make(map[string]struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start begins watching the mempool.
func (w *Watcher) Start() error {
	w.running = true

	// Load existing tracked txids from DB
	cursor, err := db.Instance.MempoolTx.Find(w.ctx, bson.M{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return fmt.Errorf("load existing mempool txids: %w", err)
	}

	var existing []struct {
		ID string `bson:"_id"`
	}
	if err := cursor.All(w.ctx, &existing); err != nil {
		return fmt.Errorf("decode existing mempool txids: %w", err)
	}

	for _, doc := range existing {
		w.knownTxIDs[doc.ID] = struct{}{}
	}

	logger.Infof("Mempool watcher started", logger.F("tracked", len(w.knownTxIDs)))

	// Initial poll
	w.poll()

	// Periodic polling
	pollInterval := time.Duration(config.C.Indexer.MempoolPollMs) * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			logger.Infof("Mempool watcher stopped")
			return nil
		case <-ticker.C:
			if w.running {
				w.poll()
			}
		}
	}
}

// Stop stops the mempool watcher.
func (w *Watcher) Stop() {
	w.running = false
	w.cancel()
	logger.Infof("Mempool watcher stopped")
}

// poll performs a single mempool sync cycle.
func (w *Watcher) poll() {
	// Get current mempool from node
	mempoolTxIDs, err := rpc.GetRawMempool(w.ctx)
	if err != nil {
		logger.Errorf("Mempool poll error", logger.F("error", err.Error()))
		return
	}

	mempoolSet := make(map[string]struct{}, len(mempoolTxIDs))
	for _, txid := range mempoolTxIDs {
		mempoolSet[txid] = struct{}{}
	}

	// Find new txids
	var newTxIDs []string
	for _, txid := range mempoolTxIDs {
		if _, known := w.knownTxIDs[txid]; !known {
			newTxIDs = append(newTxIDs, txid)
		}
	}

	// Find removed txids
	var removedTxIDs []string
	for txid := range w.knownTxIDs {
		if _, inMempool := mempoolSet[txid]; !inMempool {
			removedTxIDs = append(removedTxIDs, txid)
		}
	}

	// Process new transactions (in batches)
	const batchSize = 50
	for i := 0; i < len(newTxIDs); i += batchSize {
		end := int(math.Min(float64(i+batchSize), float64(len(newTxIDs))))
		w.processNewTransactions(newTxIDs[i:end])
	}

	// Remove stale transactions
	if len(removedTxIDs) > 0 {
		_, err := db.Instance.MempoolTx.DeleteMany(w.ctx, bson.M{"_id": bson.M{"$in": removedTxIDs}})
		if err != nil {
			logger.Errorf("Failed to remove stale mempool txs", logger.F("error", err.Error()))
		}
		for _, txid := range removedTxIDs {
			delete(w.knownTxIDs, txid)
		}
	}

	if len(newTxIDs) > 0 || len(removedTxIDs) > 0 {
		logger.Debugf("Mempool sync", logger.F(
			"total", len(mempoolSet),
			"new", len(newTxIDs),
			"removed", len(removedTxIDs),
		))
	}
}

// processNewTransactions fetches and stores a batch of new mempool transactions.
func (w *Watcher) processNewTransactions(txids []string) {
	var docs []types.MempoolTxDoc

	for _, txid := range txids {
		tx, err := rpc.GetRawTransaction(w.ctx, txid, true)
		if err != nil {
			// TX might have been confirmed between getRawMempool and getRawTransaction
			continue
		}

		// Fetch fee from mempool entry (best-effort; TX may have just been confirmed)
		var fee int64
		if entry, feeErr := rpc.GetMempoolEntry(w.ctx, txid); feeErr == nil {
			feeBTC := entry.Fees.Base
			if feeBTC == 0 {
				feeBTC = entry.Fee
			}
			fee = int64(feeBTC*1e8 + 0.5)
		}

		doc := parseMempoolTx(tx, fee)
		if doc != nil {
			docs = append(docs, *doc)
		}
		w.knownTxIDs[txid] = struct{}{}
	}

	if len(docs) > 0 {
		models := make([]mongo.WriteModel, 0, len(docs))
		for _, doc := range docs {
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": doc.ID}).
				SetUpdate(bson.M{"$setOnInsert": doc}).
				SetUpsert(true),
			)
		}
		_, err := db.Instance.MempoolTx.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(false))
		if err != nil {
			logger.Errorf("Failed to write mempool txs", logger.F("error", err.Error()))
		}
	}
}

// parseMempoolTx converts a raw transaction into a MempoolTxDoc.
func parseMempoolTx(tx *types.RpcTransaction, fee int64) *types.MempoolTxDoc {
	addrDeltas := make(map[string]int64)

	// Process outputs (positive deltas)
	for _, vout := range tx.Vout {
		address := extractAddress(vout.ScriptPubKey)
		if address == "" {
			continue
		}
		valueSat := int64(vout.Value*1e8 + 0.5)
		addrDeltas[address] += valueSat
	}

	if len(addrDeltas) == 0 {
		return nil
	}

	addrs := make([]types.MempoolAddrDelta, 0, len(addrDeltas))
	for addr, delta := range addrDeltas {
		addrs = append(addrs, types.MempoolAddrDelta{Address: addr, Delta: delta})
	}

	size := tx.VSize
	if size == 0 {
		size = tx.Size
	}

	return &types.MempoolTxDoc{
		ID:        tx.TxID,
		Addrs:     addrs,
		Fee:       fee,
		Size:      size,
		FirstSeen: time.Now(),
	}
}

// extractAddress extracts an address from a scriptPubKey (simplified for mempool).
func extractAddress(spk types.RpcScriptPubKey) string {
	if spk.Address != "" {
		return spk.Address
	}
	if len(spk.Addresses) == 1 {
		return spk.Addresses[0]
	}
	return ""
}

// RemoveConfirmed removes confirmed transactions from mempool tracking.
func RemoveConfirmed(ctx context.Context, txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	_, err := db.Instance.MempoolTx.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": txids}})
	return err
}
