// Package indexer implements the core block indexer with 5-phase processing,
// in-memory UTXO cache, batch flushing, crash recovery, and reorg handling.
package indexer

import (
	"context"
	"fmt"
	"sort"
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

// Indexer orchestrates block processing, caching, batching, and DB writes.
type Indexer struct {
	running         bool
	syncedHeight    int64
	syncedHash      string
	startTime       time.Time
	blocksProcessed int64
	initialSync     bool
	utxoCache       *UtxoCache
	batch           *BatchState
	ctx             context.Context
	cancel          context.CancelFunc
}

// New creates a new Indexer instance.
func New() *Indexer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Indexer{
		utxoCache: NewUtxoCache(config.C.Indexer.UTXOCacheMax),
		batch:     NewBatch(),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start begins the indexer. Runs until Stop() is called.
func (idx *Indexer) Start() error {
	idx.running = true
	idx.startTime = time.Now()
	idx.blocksProcessed = 0

	// Step 1: Cleanup any partial data from a previous crash
	if err := idx.cleanupOnStartup(); err != nil {
		return fmt.Errorf("cleanup on startup: %w", err)
	}

	// Step 2: Load current sync state
	state, err := db.Instance.GetSyncState(idx.ctx)
	if err != nil {
		return fmt.Errorf("get sync state: %w", err)
	}
	idx.syncedHeight = state.Height
	idx.syncedHash = state.Hash

	// Prune stale block_undo entries from previous sessions
	if err := db.Instance.PruneUndoData(idx.ctx, idx.syncedHeight); err != nil {
		logger.Warnf("Startup prune block_undo failed", logger.F("error", err.Error()))
	}

	// Step 3: Determine sync mode
	nodeHeight, err := rpc.GetBlockCount(idx.ctx)
	if err != nil {
		return fmt.Errorf("get block count: %w", err)
	}
	idx.initialSync = idx.syncedHeight < nodeHeight-10

	if idx.initialSync {
		logger.Infof("Starting INITIAL SYNC mode (optimized)", logger.F(
			"syncedHeight", idx.syncedHeight,
			"nodeHeight", nodeHeight,
			"batchSize", config.C.Indexer.BatchSize,
			"cacheMax", fmt.Sprintf("%.1fM", float64(config.C.Indexer.UTXOCacheMax)/1_000_000),
		))
	} else {
		logger.Infof("Starting LIVE mode", logger.F("syncedHeight", idx.syncedHeight))
	}

	// Step 4: Main sync loop
	return idx.syncLoop()
}

// Stop gracefully stops the indexer.
// Does NOT cancel context immediately — lets the sync loop finish its current
// flush before exiting. Context is canceled at the end of syncLoop.
func (idx *Indexer) Stop() {
	idx.running = false
	logger.Infof("Indexer stop requested")
}

// syncLoop processes blocks until caught up, then polls for new blocks.
func (idx *Indexer) syncLoop() error {
	for idx.running {
		if err := idx.ctx.Err(); err != nil {
			break
		}

		nodeHeight, err := rpc.GetBlockCount(idx.ctx)
		if err != nil {
			logger.Errorf("Failed to get block count", logger.F("error", err.Error()))
			time.Sleep(5 * time.Second)
			continue
		}

		if idx.syncedHeight >= nodeHeight {
			// Caught up — switch to live mode if still in initial sync
			if idx.initialSync {
				if err := idx.finishInitialSync(); err != nil {
					logger.Errorf("Failed to finish initial sync", logger.F("error", err.Error()))
				}
			}
			time.Sleep(3 * time.Second)
			continue
		}

		// During live mode, check for chain reorganization
		if !idx.initialSync && idx.syncedHeight >= 0 {
			reorgDepth, err := idx.detectReorg()
			if err != nil {
				logger.Errorf("Reorg detection error", logger.F("error", err.Error()))
				time.Sleep(5 * time.Second)
				continue
			}
			if reorgDepth > 0 {
				if err := idx.handleReorg(reorgDepth); err != nil {
					logger.Errorf("Reorg handling failed", logger.F("error", err.Error()))
				}
				continue
			}
		}

		// Process blocks in batches
		batchSize := config.C.Indexer.BatchSize
		if !idx.initialSync {
			batchSize = config.C.Indexer.BlocksPerTick
		}
		targetHeight := idx.syncedHeight + int64(batchSize)
		if targetHeight > nodeHeight {
			targetHeight = nodeHeight
		}

		// Block prefetch: fetch next block while processing current one
		var nextBlock *types.RpcBlock
		var nextBlockErr error
		prefetchDone := make(chan struct{})

		if err := func() error {
			for h := idx.syncedHeight + 1; h <= targetHeight && idx.running; h++ {
				var block *types.RpcBlock

				// Use prefetched block or fetch fresh
				if nextBlock != nil && nextBlockErr == nil {
					block = nextBlock
					nextBlock = nil
				} else {
					var fetchErr error
					block, fetchErr = rpc.GetBlock(idx.ctx, h, 2)
					if fetchErr != nil {
						return fmt.Errorf("fetch block %d: %w", h, fetchErr)
					}
				}

				// Start prefetching next block (overlap with processBlock)
				if h+1 <= targetHeight && idx.running {
					nextH := h + 1
					go func() {
						nextBlock, nextBlockErr = rpc.GetBlock(idx.ctx, nextH, 2)
						close(prefetchDone)
					}()
				}

				// Process block
				if err := idx.processBlock(block, h); err != nil {
					return fmt.Errorf("process block %d: %w", h, err)
				}
				idx.blocksProcessed++

				// Flush: every BATCH_SIZE blocks during initial sync; every block in live mode
				if !idx.initialSync || idx.batch.BlockCount >= config.C.Indexer.BatchSize {
					if err := idx.flushBatch(); err != nil {
						return fmt.Errorf("flush batch at height %d: %w", h, err)
					}
				}

				// Progress logging
				if h%config.C.Indexer.LogInterval == 0 || h == nodeHeight {
					elapsed := time.Since(idx.startTime).Seconds()
					bps := float64(idx.blocksProcessed) / elapsed
					remaining := nodeHeight - h
					eta := int64(0)
					if remaining > 0 && bps > 0 {
						eta = int64(float64(remaining) / bps)
					}
					etaStr := formatETA(eta)

					fields := logger.F(
						"bps", fmt.Sprintf("%.1f", bps),
						"eta", etaStr,
					)

					if idx.initialSync {
						cs := idx.utxoCache.Stats()
						fields["cache"] = fmt.Sprintf("%dK", cs.Size/1000)
						fields["hit"] = cs.HitRate
					}

					pct := float64(h) / float64(nodeHeight) * 100
					logger.Infof(fmt.Sprintf("Block %d/%d (%.2f%%)", h, nodeHeight, pct), fields)
				}

				// Wait for prefetch if we started one
				if h+1 <= targetHeight && idx.running {
					<-prefetchDone
					prefetchDone = make(chan struct{})
				}
			}
			return nil
		}(); err != nil {
			// If shutting down, don't log error or retry
			if !idx.running {
				break
			}
			// Discard partial batch and reset from last committed state
			idx.batch = NewBatch()
			idx.utxoCache = NewUtxoCache(config.C.Indexer.UTXOCacheMax)
			state, stateErr := db.Instance.GetSyncState(idx.ctx)
			if stateErr == nil {
				idx.syncedHeight = state.Height
				idx.syncedHash = state.Hash
			}
			logger.Errorf("Sync error — reset to last checkpoint", logger.F(
				"height", idx.syncedHeight,
				"error", err.Error(),
			))
			time.Sleep(5 * time.Second)
			continue
		}

		// Flush remaining accumulated data (skip if shutting down — shutdown flush handles it)
		if idx.batch.BlockCount > 0 && idx.running {
			if err := idx.flushBatch(); err != nil {
				logger.Errorf("Flush remaining batch error", logger.F("error", err.Error()))
			}
		}

		// Periodic maintenance
		if idx.blocksProcessed%500 == 0 {
			_ = db.Instance.PruneUndoData(idx.ctx, idx.syncedHeight)
			idx.utxoCache.EvictIfNeeded()
		}
	}

	// Flush remaining data on shutdown (use fresh context, not the canceled one)
	if idx.batch.BlockCount > 0 {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 180*time.Second)
		origCtx := idx.ctx
		idx.ctx = shutdownCtx
		if err := idx.flushBatch(); err != nil {
			logger.Warnf("Flush on shutdown error", logger.F("error", err.Error()))
		}
		idx.ctx = origCtx
		shutdownCancel()
	}

	// Cancel the context
	idx.cancel()
	logger.Infof("Indexer stopped")
	return nil
}

// finishInitialSync transitions from initial sync to live mode.
// addr_stats entries are created on-demand when users query an address via the API.
// Live delta updates keep existing entries current as new blocks arrive.
func (idx *Indexer) finishInitialSync() error {
	idx.initialSync = false

	elapsed := time.Since(idx.startTime).Hours()
	logger.Infof(fmt.Sprintf("Initial sync complete in %.2fh! Finalizing...", elapsed))

	logger.Infof("Creating secondary indexes (idempotent, may take several minutes)...")
	if err := db.Instance.CreateAllIndexes(idx.ctx); err != nil {
		return fmt.Errorf("create all indexes: %w", err)
	}

	logger.Infof("Switched to LIVE mode — monitoring for new blocks")
	return nil
}

// ============================
// Block Processing (5-Phase, Cache-Optimized)
// ============================

// inputRef tracks an input reference during classification.
type inputRef struct {
	outpoint string
	txid     string
	txIdx    int
}

// cacheHit tracks a cache-resolved input.
type cacheHit struct {
	inputRef
	utxo     types.ResolvedUtxo
	dbDelete bool
}

// processBlock processes a single block with the 5-phase approach.
func (idx *Indexer) processBlock(block *types.RpcBlock, height int64) error {

	// ---- Phase 1: Collect all new outputs into a local map ----
	localUtxos := make(map[string]types.ResolvedUtxo, len(block.Tx)*2)

	for _, tx := range block.Tx {
		for _, vout := range tx.Vout {
			stype := vout.ScriptPubKey.Type
			if stype == "nulldata" || stype == "nonstandard" {
				continue
			}
			address := ExtractAddress(vout.ScriptPubKey)
			outpoint := fmt.Sprintf("%s:%d", tx.TxID, vout.N)
			localUtxos[outpoint] = types.ResolvedUtxo{
				Address: address,
				Value:   ToSatoshis(vout.Value),
				Height:  height,
			}
		}
	}

	// ---- Phase 2: Classify inputs ----
	var dbNeeded []inputRef
	var localResolved []struct {
		inputRef
		types.ResolvedUtxo
	}
	var cacheResolved []cacheHit

	for txIdx, tx := range block.Tx {
		// Skip coinbase
		if len(tx.Vin) == 1 && tx.Vin[0].Coinbase != "" {
			continue
		}

		for _, vin := range tx.Vin {
			if vin.TxID == "" || vin.Vout == nil {
				continue
			}
			outpoint := fmt.Sprintf("%s:%d", vin.TxID, *vin.Vout)
			ref := inputRef{outpoint: outpoint, txid: tx.TxID, txIdx: txIdx}

			// 1. Check intra-block (same block creates and spends)
			if local, ok := localUtxos[outpoint]; ok {
				localResolved = append(localResolved, struct {
					inputRef
					types.ResolvedUtxo
				}{ref, local})
				continue
			}

			// 2. Check UTXO cache (very fast, in-memory)
			if cached := idx.utxoCache.Spend(outpoint); cached != nil {
				cacheResolved = append(cacheResolved, cacheHit{
					inputRef: ref,
					utxo:     cached.Utxo,
					dbDelete: cached.DBDelete,
				})
				continue
			}

			// 3. Cache miss — needs DB fetch
			dbNeeded = append(dbNeeded, ref)
		}
	}

	// ---- Phase 3: Batch-fetch referenced UTXOs from MongoDB (cache misses) ----
	dbResolved := make(map[string]types.ResolvedUtxo)

	if len(dbNeeded) > 0 {
		const chunkSize = 10000
		for i := 0; i < len(dbNeeded); i += chunkSize {
			end := i + chunkSize
			if end > len(dbNeeded) {
				end = len(dbNeeded)
			}
			chunk := dbNeeded[i:end]

			// Collect outpoints and sort for B-tree page locality
			outpoints := make([]string, len(chunk))
			for j, ref := range chunk {
				outpoints[j] = ref.outpoint
			}
			sort.Strings(outpoints)

			cursor, err := db.Instance.Utxos.Find(idx.ctx,
				bson.M{"_id": bson.M{"$in": outpoints}},
				options.Find().SetProjection(bson.M{"a": 1, "s": 1, "h": 1}),
			)
			if err != nil {
				return fmt.Errorf("batch fetch UTXOs: %w", err)
			}

			var docs []types.UtxoDoc
			if err := cursor.All(idx.ctx, &docs); err != nil {
				return fmt.Errorf("decode UTXOs: %w", err)
			}

			for _, doc := range docs {
				dbResolved[doc.ID] = types.ResolvedUtxo{
					Address: doc.Address,
					Value:   doc.Value,
					Height:  doc.Height,
				}
			}
		}

		// RPC fallback for missing UTXOs (P2PK, pre-indexed, etc.)
		for _, ref := range dbNeeded {
			if _, ok := dbResolved[ref.outpoint]; !ok {
				logger.Debugf(fmt.Sprintf("Fetching missing UTXO via RPC: %s at height %d", ref.outpoint, height))

				// Parse outpoint
				var prevTxid string
				var prevVout int
				fmt.Sscanf(ref.outpoint, "%64s", &prevTxid)
				// Need to split properly
				colonIdx := len(ref.outpoint) - 1
				for colonIdx >= 0 && ref.outpoint[colonIdx] != ':' {
					colonIdx--
				}
				if colonIdx < 0 {
					return fmt.Errorf("invalid outpoint format: %s", ref.outpoint)
				}
				prevTxid = ref.outpoint[:colonIdx]
				fmt.Sscanf(ref.outpoint[colonIdx+1:], "%d", &prevVout)

				prevTx, err := rpc.GetRawTransaction(idx.ctx, prevTxid, true)
				if err != nil {
					return fmt.Errorf("UTXO not found for input: %s (spent in tx %s at height %d). RPC fallback failed: %w",
						ref.outpoint, ref.txid, height, err)
				}

				if prevVout >= len(prevTx.Vout) {
					return fmt.Errorf("output index %d not found in tx %s", prevVout, prevTxid)
				}

				output := prevTx.Vout[prevVout]
				addr := ExtractAddress(output.ScriptPubKey)
				val := ToSatoshis(output.Value)
				dbResolved[ref.outpoint] = types.ResolvedUtxo{Address: addr, Value: val, Height: 0}
			}
		}
	}

	// ---- Phase 4: Build all operations ----
	intraBlockSpent := make(map[string]struct{})

	// Per-address per-tx accumulator
	type addrTxKey struct {
		address string
		txid    string
	}
	addrTxAccum := make(map[addrTxKey]*types.AddrTxAccum)

	// Per-block stats deltas
	blockStatsDeltas := make(map[string]*types.BlockStatsAccum)

	accumulateAddrTx := func(address, txid string, txIdx int, delta, received, sent int64) {
		key := addrTxKey{address, txid}
		existing, ok := addrTxAccum[key]
		if ok {
			existing.Delta += delta
			existing.Received += received
			existing.Sent += sent
			if txIdx < existing.TxIndex {
				existing.TxIndex = txIdx
			}
		} else {
			addrTxAccum[key] = &types.AddrTxAccum{
				Address:  address,
				TxID:     txid,
				Height:   height,
				TxIndex:  txIdx,
				Delta:    delta,
				Received: received,
				Sent:     sent,
			}
		}
	}

	accumulateBlockStats := func(address, txid string, bDelta, rDelta, sDelta int64) {
		existing, ok := blockStatsDeltas[address]
		if ok {
			existing.Balance += bDelta
			existing.Received += rDelta
			existing.Sent += sDelta
			existing.TxIDs[txid] = struct{}{}
		} else {
			blockStatsDeltas[address] = &types.BlockStatsAccum{
				Balance:  bDelta,
				Received: rDelta,
				Sent:     sDelta,
				TxIDs:    map[string]struct{}{txid: {}},
			}
		}
	}

	// Collect spent UTXO records for undo data
	var spentRecords []types.SpentRecord

	// 4a. Process all OUTPUTS (positive deltas)
	for txIdx, tx := range block.Tx {
		for _, vout := range tx.Vout {
			address := ExtractAddress(vout.ScriptPubKey)
			if address == "" {
				continue
			}
			valueSat := ToSatoshis(vout.Value)
			accumulateAddrTx(address, tx.TxID, txIdx, valueSat, valueSat, 0)
			accumulateBlockStats(address, tx.TxID, valueSat, valueSat, 0)
		}
	}

	// 4b. Process INPUTS — intra-block spends
	for _, input := range localResolved {
		intraBlockSpent[input.outpoint] = struct{}{}
		if input.Address != "" {
			accumulateAddrTx(input.Address, input.txid, input.txIdx, -input.Value, 0, input.Value)
			accumulateBlockStats(input.Address, input.txid, -input.Value, 0, input.Value)
		}
	}

	// 4c. Process INPUTS — cache hits
	for _, hit := range cacheResolved {
		if hit.dbDelete {
			idx.batch.UtxoDeletes = append(idx.batch.UtxoDeletes, hit.outpoint)
		}
		spentRecords = append(spentRecords, types.SpentRecord{
			Outpoint: hit.outpoint, Address: hit.utxo.Address,
			Value: hit.utxo.Value, Height: hit.utxo.Height,
		})
		if hit.utxo.Address != "" {
			accumulateAddrTx(hit.utxo.Address, hit.txid, hit.txIdx, -hit.utxo.Value, 0, hit.utxo.Value)
			accumulateBlockStats(hit.utxo.Address, hit.txid, -hit.utxo.Value, 0, hit.utxo.Value)
		}
	}

	// 4d. Process INPUTS — DB lookups
	for _, ref := range dbNeeded {
		resolved := dbResolved[ref.outpoint]
		idx.batch.UtxoDeletes = append(idx.batch.UtxoDeletes, ref.outpoint)
		spentRecords = append(spentRecords, types.SpentRecord{
			Outpoint: ref.outpoint, Address: resolved.Address,
			Value: resolved.Value, Height: resolved.Height,
		})
		if resolved.Address != "" {
			accumulateAddrTx(resolved.Address, ref.txid, ref.txIdx, -resolved.Value, 0, resolved.Value)
			accumulateBlockStats(resolved.Address, ref.txid, -resolved.Value, 0, resolved.Value)
		}
	}

	// 4e. Add surviving UTXOs to cache (not intra-block spent)
	for outpoint, info := range localUtxos {
		if _, spent := intraBlockSpent[outpoint]; !spent {
			idx.utxoCache.AddNew(outpoint, info)
		}
	}

	// ---- Phase 5: Accumulate into batch ----

	// addr_tx entries
	for _, entry := range addrTxAccum {
		doc := types.AddrTxDoc{
			Address:  entry.Address,
			TxID:     entry.TxID,
			Height:   entry.Height,
			TxIndex:  entry.TxIndex,
			Delta:    entry.Delta,
			Received: entry.Received,
			Sent:     entry.Sent,
		}
		if !idx.initialSync {
			// Live mode: compound _id for upsert idempotency
			doc.ID = fmt.Sprintf("%s:%s", entry.Address, entry.TxID)
		}
		idx.batch.AddrTxEntries = append(idx.batch.AddrTxEntries, doc)
	}

	// Undo data
	deltas := make([]types.StatsDelta, 0, len(blockStatsDeltas))
	for addr, d := range blockStatsDeltas {
		deltas = append(deltas, types.StatsDelta{
			Address:  addr,
			Balance:  d.Balance,
			Received: d.Received,
			Sent:     d.Sent,
			TxCount:  int64(len(d.TxIDs)),
		})
	}
	idx.batch.UndoEntries = append(idx.batch.UndoEntries, types.BlockUndoDoc{
		ID:     height,
		Hash:   block.Hash,
		Spent:  spentRecords,
		Deltas: deltas,
	})

	// Live mode: accumulate stats deltas across the batch
	if !idx.initialSync {
		for addr, d := range blockStatsDeltas {
			existing, ok := idx.batch.StatsDeltas[addr]
			if ok {
				existing.Balance += d.Balance
				existing.Received += d.Received
				existing.Sent += d.Sent
				for txid := range d.TxIDs {
					existing.TxIDs[txid] = struct{}{}
				}
				if height < existing.MinH {
					existing.MinH = height
				}
				if height > existing.MaxH {
					existing.MaxH = height
				}
			} else {
				txids := make(map[string]struct{}, len(d.TxIDs))
				for txid := range d.TxIDs {
					txids[txid] = struct{}{}
				}
				idx.batch.StatsDeltas[addr] = &types.BatchStatsAccum{
					Balance:  d.Balance,
					Received: d.Received,
					Sent:     d.Sent,
					TxIDs:    txids,
					MinH:     height,
					MaxH:     height,
				}
			}
		}
	}

	idx.batch.BlockCount++
	idx.batch.LastHeight = height
	idx.batch.LastHash = block.Hash
	return nil
}

// ============================
// Batch Flush
// ============================

// flushBatch writes all accumulated batch data to MongoDB.
// Write order (crash-safe):
//  1. Undo data (for recovery)
//  2. UTXO inserts
//  3. UTXO deletes
//  4. addr_tx entries
//  5. addr_stats (live mode only)
//  6. sync_state (LAST — marks batch as committed)
func (idx *Indexer) flushBatch() error {
	if idx.batch.BlockCount == 0 {
		return nil
	}

	ctx := idx.ctx

	// Get pending UTXO inserts from cache
	utxoInserts := idx.utxoCache.DrainPendingInserts()

	// 1. Write undo data (chunked to avoid oversized BulkWrite messages)
	if len(idx.batch.UndoEntries) > 0 {
		const undoChunk = 25
		for i := 0; i < len(idx.batch.UndoEntries); i += undoChunk {
			end := i + undoChunk
			if end > len(idx.batch.UndoEntries) {
				end = len(idx.batch.UndoEntries)
			}
			chunk := idx.batch.UndoEntries[i:end]
			models := make([]mongo.WriteModel, 0, len(chunk))
			for _, e := range chunk {
				models = append(models, mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": e.ID}).
					SetUpdate(bson.M{"$set": bson.M{"hash": e.Hash, "spent": e.Spent, "deltas": e.Deltas}}).
					SetUpsert(true),
				)
			}
			_, err := db.Instance.BlockUndo.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			if err != nil && !db.IsDuplicateKeyError(err) {
				return fmt.Errorf("write undo data: %w", err)
			}
		}
	}

	// 2. Insert new UTXOs (sorted by _id for B-tree page locality)
	if len(utxoInserts) > 0 {
		sort.Slice(utxoInserts, func(i, j int) bool {
			return utxoInserts[i].ID < utxoInserts[j].ID
		})

		docs := make([]interface{}, len(utxoInserts))
		for i, d := range utxoInserts {
			docs[i] = d
		}
		_, err := db.Instance.Utxos.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		if err != nil && !db.IsDuplicateKeyError(err) {
			return fmt.Errorf("insert UTXOs: %w", err)
		}
	}

	// 3. Delete spent UTXOs (sorted for sequential B-tree traversal)
	if len(idx.batch.UtxoDeletes) > 0 {
		sort.Strings(idx.batch.UtxoDeletes)
		const delChunk = 25000
		for i := 0; i < len(idx.batch.UtxoDeletes); i += delChunk {
			end := i + delChunk
			if end > len(idx.batch.UtxoDeletes) {
				end = len(idx.batch.UtxoDeletes)
			}
			chunk := idx.batch.UtxoDeletes[i:end]
			_, err := db.Instance.Utxos.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": chunk}})
			if err != nil {
				return fmt.Errorf("delete spent UTXOs: %w", err)
			}
		}
	}

	// 4. Write addr_tx entries
	if len(idx.batch.AddrTxEntries) > 0 {
		if idx.initialSync {
			// insertMany: fastest for initial sync
			docs := make([]interface{}, len(idx.batch.AddrTxEntries))
			for i, e := range idx.batch.AddrTxEntries {
				docs[i] = e
			}
			_, err := db.Instance.AddrTx.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
			if err != nil && !db.IsDuplicateKeyError(err) {
				return fmt.Errorf("insert addr_tx: %w", err)
			}
		} else {
			// Upsert for live mode (idempotent re-processing)
			models := make([]mongo.WriteModel, 0, len(idx.batch.AddrTxEntries))
			for _, entry := range idx.batch.AddrTxEntries {
				models = append(models, mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": entry.ID}).
					SetUpdate(bson.M{"$set": bson.M{
						"a": entry.Address, "x": entry.TxID, "h": entry.Height,
						"i": entry.TxIndex, "d": entry.Delta, "r": entry.Received, "s": entry.Sent,
					}}).
					SetUpsert(true),
				)
			}
			_, err := db.Instance.AddrTx.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return fmt.Errorf("upsert addr_tx: %w", err)
			}
		}
	}

	// 5. Update addr_stats (live mode only; initial sync rebuilds at end)
	if !idx.initialSync && len(idx.batch.StatsDeltas) > 0 {
		models := make([]mongo.WriteModel, 0, len(idx.batch.StatsDeltas))
		for addr, delta := range idx.batch.StatsDeltas {
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": addr}).
				SetUpdate(bson.M{
					"$inc": bson.M{
						"b": delta.Balance,
						"r": delta.Received,
						"s": delta.Sent,
						"c": int64(len(delta.TxIDs)),
					},
					"$min": bson.M{"f": delta.MinH},
					"$max": bson.M{"l": delta.MaxH},
				}).
				SetUpsert(false),
			)
		}
		_, err := db.Instance.AddrStats.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
		if err != nil {
			return fmt.Errorf("update addr_stats: %w", err)
		}
	}

	// 6. Update sync state LAST (marks batch as fully committed)
	if err := db.Instance.UpdateSyncState(ctx, idx.batch.LastHeight, idx.batch.LastHash); err != nil {
		return fmt.Errorf("update sync state: %w", err)
	}
	idx.syncedHeight = idx.batch.LastHeight
	idx.syncedHash = idx.batch.LastHash

	// Reset batch
	idx.batch = NewBatch()

	// Evict committed UTXO cache entries if over limit
	idx.utxoCache.EvictIfNeeded()

	return nil
}

// ============================
// Reorg Detection & Handling
// ============================

// detectReorg compares our stored block hash with the node's hash.
// Returns the number of blocks to roll back (0 = no reorg).
func (idx *Indexer) detectReorg() (int, error) {
	if idx.syncedHeight < 0 || idx.syncedHash == "" {
		return 0, nil
	}

	nodeHash, err := rpc.GetBlockHash(idx.ctx, idx.syncedHeight)
	if err != nil {
		// Block height out of range — not a reorg
		return 0, nil
	}

	if nodeHash == idx.syncedHash {
		return 0, nil
	}

	logger.Warnf("Chain reorganization detected!", logger.F(
		"height", idx.syncedHeight,
		"ourHash", idx.syncedHash[:16],
		"nodeHash", nodeHash[:16],
	))

	depth := 1
	maxDepth := int(config.C.Indexer.ReorgSafetyDepth)

	for depth < maxDepth && (idx.syncedHeight-int64(depth)) >= 0 {
		var undo types.BlockUndoDoc
		err := db.Instance.BlockUndo.FindOne(idx.ctx, bson.M{"_id": idx.syncedHeight - int64(depth)}).Decode(&undo)
		if err != nil {
			return 0, fmt.Errorf("undo data not found at height %d, cannot determine reorg depth", idx.syncedHeight-int64(depth))
		}

		hashAtDepth, err := rpc.GetBlockHash(idx.ctx, idx.syncedHeight-int64(depth))
		if err != nil {
			return 0, fmt.Errorf("get block hash at depth: %w", err)
		}

		if hashAtDepth == undo.Hash {
			break
		}
		depth++
	}

	if depth >= maxDepth {
		return 0, fmt.Errorf("reorg deeper than safety depth (%d). Manual re-index required", maxDepth)
	}

	logger.Warnf(fmt.Sprintf("Reorg depth: %d blocks", depth))
	return depth, nil
}

// handleReorg rolls back the specified number of blocks using undo data.
func (idx *Indexer) handleReorg(depth int) error {
	logger.Warnf(fmt.Sprintf("Rolling back %d blocks...", depth))

	for i := 0; i < depth; i++ {
		if err := idx.rollbackBlock(idx.syncedHeight); err != nil {
			return fmt.Errorf("rollback block %d: %w", idx.syncedHeight, err)
		}
	}

	logger.Infof("Reorg rollback complete", logger.F("newHeight", idx.syncedHeight))
	return nil
}

// rollbackBlock rolls back a single block using its undo data.
func (idx *Indexer) rollbackBlock(height int64) error {
	var undo types.BlockUndoDoc
	err := db.Instance.BlockUndo.FindOne(idx.ctx, bson.M{"_id": height}).Decode(&undo)
	if err != nil {
		return fmt.Errorf("no undo data for block %d: %w", height, err)
	}

	logger.Infof(fmt.Sprintf("Rolling back block %d", height), logger.F("hash", undo.Hash[:16]))

	ctx := idx.ctx

	// 1. Restore spent UTXOs
	if len(undo.Spent) > 0 {
		docs := make([]interface{}, len(undo.Spent))
		for i, s := range undo.Spent {
			docs[i] = types.UtxoDoc{ID: s.Outpoint, Address: s.Address, Value: s.Value, Height: s.Height}
		}
		_, err := db.Instance.Utxos.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		if err != nil && !db.IsDuplicateKeyError(err) {
			return fmt.Errorf("restore spent UTXOs: %w", err)
		}
	}

	// 2. Delete UTXOs created at this height
	_, err = db.Instance.Utxos.DeleteMany(ctx, bson.M{"h": height})
	if err != nil {
		return fmt.Errorf("delete UTXOs at height %d: %w", height, err)
	}

	// 3. Delete addr_tx entries at this height
	_, err = db.Instance.AddrTx.DeleteMany(ctx, bson.M{"h": height})
	if err != nil {
		return fmt.Errorf("delete addr_tx at height %d: %w", height, err)
	}

	// 4. Reverse addr_stats deltas
	if len(undo.Deltas) > 0 {
		models := make([]mongo.WriteModel, 0, len(undo.Deltas))
		for _, d := range undo.Deltas {
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": d.Address}).
				SetUpdate(bson.M{"$inc": bson.M{
					"b": -d.Balance, "r": -d.Received, "s": -d.Sent, "c": -d.TxCount,
				}}),
			)
		}
		_, err := db.Instance.AddrStats.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
		if err != nil {
			return fmt.Errorf("reverse addr_stats: %w", err)
		}
	}

	// 5. Delete undo data for this height
	_, err = db.Instance.BlockUndo.DeleteOne(ctx, bson.M{"_id": height})
	if err != nil {
		return fmt.Errorf("delete undo data: %w", err)
	}

	// 6. Update sync state to previous block
	prevHeight := height - 1
	if prevHeight >= 0 {
		var prevUndo types.BlockUndoDoc
		prevErr := db.Instance.BlockUndo.FindOne(ctx, bson.M{"_id": prevHeight}).Decode(&prevUndo)
		prevHash := ""
		if prevErr == nil {
			prevHash = prevUndo.Hash
		}
		if err := db.Instance.UpdateSyncState(ctx, prevHeight, prevHash); err != nil {
			return fmt.Errorf("update sync state after rollback: %w", err)
		}
		idx.syncedHeight = prevHeight
		idx.syncedHash = prevHash
	} else {
		if err := db.Instance.UpdateSyncState(ctx, -1, ""); err != nil {
			return fmt.Errorf("update sync state to genesis: %w", err)
		}
		idx.syncedHeight = -1
		idx.syncedHash = ""
	}

	// Invalidate UTXO cache after rollback
	idx.utxoCache = NewUtxoCache(config.C.Indexer.UTXOCacheMax)

	return nil
}

// ============================
// Crash Recovery
// ============================

// cleanupOnStartup detects and cleans up any partially flushed batch.
func (idx *Indexer) cleanupOnStartup() error {
	ctx := idx.ctx

	state, err := db.Instance.GetSyncState(ctx)
	if err != nil {
		return err
	}
	baseHeight := state.Height

	// Find all undo data above the committed height (partial batch)
	cursor, err := db.Instance.BlockUndo.Find(ctx,
		bson.M{"_id": bson.M{"$gt": baseHeight}},
		options.Find().SetSort(bson.M{"_id": -1}),
	)
	if err != nil {
		return fmt.Errorf("find stray undo data: %w", err)
	}

	var strayUndos []types.BlockUndoDoc
	if err := cursor.All(ctx, &strayUndos); err != nil {
		return fmt.Errorf("decode stray undo data: %w", err)
	}

	if len(strayUndos) > 0 {
		logger.Warnf(fmt.Sprintf("Found %d uncommitted blocks — cleaning up...", len(strayUndos)))

		// Restore spent UTXOs from undo data (highest blocks first)
		for _, undo := range strayUndos {
			if len(undo.Spent) > 0 {
				docs := make([]interface{}, len(undo.Spent))
				for i, s := range undo.Spent {
					docs[i] = types.UtxoDoc{ID: s.Outpoint, Address: s.Address, Value: s.Value, Height: s.Height}
				}
				_, err := db.Instance.Utxos.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
				if err != nil && !db.IsDuplicateKeyError(err) {
					return fmt.Errorf("restore undo UTXOs: %w", err)
				}
			}
		}

		// Delete stray UTXOs created in uncommitted blocks
		_, err = db.Instance.Utxos.DeleteMany(ctx, bson.M{"h": bson.M{"$gt": baseHeight}})
		if err != nil {
			return fmt.Errorf("delete stray UTXOs: %w", err)
		}

		// Delete all stray undo data
		_, err = db.Instance.BlockUndo.DeleteMany(ctx, bson.M{"_id": bson.M{"$gt": baseHeight}})
		if err != nil {
			return fmt.Errorf("delete stray undo: %w", err)
		}

		logger.Infof("Cleanup complete — resuming from committed state", logger.F("height", baseHeight))
	} else {
		// Quick check for stray data without undo (edge case)
		count, _ := db.Instance.Utxos.CountDocuments(ctx, bson.M{"h": bson.M{"$gt": baseHeight}})
		if count > 0 {
			logger.Warnf("Found stray UTXOs without undo data — cleaning...")
			_, _ = db.Instance.Utxos.DeleteMany(ctx, bson.M{"h": bson.M{"$gt": baseHeight}})
		}
	}

	// Clean stray addr_tx entries from partial batches
	result, err := db.Instance.AddrTx.DeleteMany(ctx, bson.M{"h": bson.M{"$gt": baseHeight}})
	if err != nil {
		return fmt.Errorf("clean stray addr_tx: %w", err)
	}
	if result.DeletedCount > 0 {
		logger.Infof(fmt.Sprintf("Cleaned %d stray addr_tx entries", result.DeletedCount))
	}

	return nil
}

// ============================
// Helpers
// ============================

func formatETA(seconds int64) string {
	if seconds <= 0 {
		return "synced"
	}
	if seconds > 3600 {
		return fmt.Sprintf("%.1fh", float64(seconds)/3600)
	}
	if seconds > 60 {
		return fmt.Sprintf("%dm", seconds/60)
	}
	return fmt.Sprintf("%ds", seconds)
}
