// Package api provides a high-performance REST API server for querying indexed blockchain data.
// Uses Go's standard net/http (production-ready, no framework needed).
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/halilbeydilli/litecoin-indexer/internal/config"
	"github.com/halilbeydilli/litecoin-indexer/internal/db"
	"github.com/halilbeydilli/litecoin-indexer/internal/logger"
	"github.com/halilbeydilli/litecoin-indexer/internal/rpc"
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// computingAddresses tracks addresses currently being computed in background
// goroutines to prevent duplicate computations from concurrent API calls.
var computingAddresses sync.Map

// Server holds the HTTP server instance.
type Server struct {
	httpServer *http.Server
}

// NewServer creates a new API server.
func NewServer() *Server {
	mux := http.NewServeMux()

	// Register routes
	mux.HandleFunc("GET /api/address/{address}/balance", handleAddressBalance)
	mux.HandleFunc("GET /api/address/{address}/utxos", handleAddressUtxos)
	mux.HandleFunc("GET /api/address/{address}/history", handleAddressHistory)
	mux.HandleFunc("GET /api/address/{address}/mempool", handleAddressMempool)
	mux.HandleFunc("GET /api/mempool/recent", handleMempoolRecent)
	mux.HandleFunc("GET /api/mempool/nextblock", handleMempoolNextBlock)
	mux.HandleFunc("POST /api/address/{address}/rescan", handleRescan)
	mux.HandleFunc("GET /api/rescan/status/{address}", handleRescanStatus)
	mux.HandleFunc("GET /api/tx/{txid}", handleTx)
	mux.HandleFunc("GET /api/status", handleStatus)
	mux.HandleFunc("GET /health", handleHealth)

	// Wrap with CORS and logging middleware
	handler := corsMiddleware(loggingMiddleware(mux))

	addr := fmt.Sprintf("%s:%d", config.C.API.Host, config.C.API.Port)
	return &Server{
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

// Start begins listening for HTTP requests.
func (s *Server) Start() error {
	logger.Infof(fmt.Sprintf("API server listening on %s", s.httpServer.Addr))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("api server: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// ============================
// Middleware
// ============================

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Debugf(fmt.Sprintf("%s %s", r.Method, r.URL.Path))
		next.ServeHTTP(w, r)
	})
}

// ============================
// Handlers
// ============================

// GET /api/address/:address/balance
// Returns pre-computed balance, totalReceived, totalSent, txCount.
// If addr_stats entry doesn't exist, kicks off a background computation
// (independent of the HTTP request context) and returns basic balance from UTXOs
// immediately with computing=true. Live delta updates keep the entry current
// once it is created.
func handleAddressBalance(w http.ResponseWriter, r *http.Request) {
	address := r.PathValue("address")

	ctx := r.Context()
	var stats types.AddrStatsDoc
	err := db.Instance.AddrStats.FindOne(ctx, bson.M{"_id": address}).Decode(&stats)

	if err == nil {
		// addr_stats exists — kept up-to-date by live deltas, return immediately
		writeJSON(w, map[string]interface{}{
			"address":   address,
			"balance":   stats.Balance,
			"received":  stats.Received,
			"sent":      stats.Sent,
			"txCount":   stats.TxCount,
			"firstSeen": stats.FirstSeen,
			"lastSeen":  stats.LastSeen,
		})
		return
	}

	// addr_stats not found — kick off background computation with an independent
	// context so it survives HTTP client disconnects (e.g. Node.js 60s timeout).
	// Use sync.Map to prevent duplicate goroutines for the same address.
	if _, alreadyComputing := computingAddresses.LoadOrStore(address, true); !alreadyComputing {
		go func() {
			defer computingAddresses.Delete(address)
			bgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()
			computed, bgErr := computeAndSaveAddrStats(bgCtx, address)
			if bgErr != nil {
				logger.Warnf(fmt.Sprintf("Background addr_stats computation failed for %s: %s", address, bgErr.Error()))
			} else if computed != nil {
				logger.Infof(fmt.Sprintf("Background addr_stats ready for %s (txCount=%d, balance=%d)", address, computed.TxCount, computed.Balance))
			}
		}()
	}

	// Quick balance from UTXO sum for immediate response
	var utxoBalance int64
	utxoPipeline := bson.A{
		bson.M{"$match": bson.M{"a": address}},
		bson.M{"$group": bson.M{"_id": nil, "total": bson.M{"$sum": "$s"}}},
	}
	if utxoCursor, aggErr := db.Instance.Utxos.Aggregate(ctx, utxoPipeline); aggErr == nil {
		defer utxoCursor.Close(ctx)
		if utxoCursor.Next(ctx) {
			var aggResult struct {
				Total int64 `bson:"total"`
			}
			if utxoCursor.Decode(&aggResult) == nil {
				utxoBalance = aggResult.Total
			}
		}
	}

	txCount, _ := db.Instance.AddrTx.CountDocuments(ctx, bson.M{"a": address})

	writeJSON(w, map[string]interface{}{
		"address":   address,
		"balance":   utxoBalance,
		"received":  nil,
		"sent":      nil,
		"txCount":   txCount,
		"firstSeen": nil,
		"lastSeen":  nil,
		"computing": true,
	})
}

// GET /api/address/:address/utxos
// Returns all unspent outputs for an address.
func handleAddressUtxos(w http.ResponseWriter, r *http.Request) {
	address := r.PathValue("address")
	limit := clampInt(queryInt(r, "limit", 1000), 1, 10000)
	offset := queryInt(r, "offset", 0)

	ctx := r.Context()
	cursor, err := db.Instance.Utxos.Find(ctx,
		bson.M{"a": address},
		options.Find().SetSkip(int64(offset)).SetLimit(int64(limit)),
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var utxoDocs []types.UtxoDoc
	if err := cursor.All(ctx, &utxoDocs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := make([]map[string]interface{}, 0, len(utxoDocs))
	for _, u := range utxoDocs {
		parts := strings.SplitN(u.ID, ":", 2)
		txid := parts[0]
		vout := 0
		if len(parts) == 2 {
			vout, _ = strconv.Atoi(parts[1])
		}
		items = append(items, map[string]interface{}{
			"txid":   txid,
			"vout":   vout,
			"value":  u.Value,
			"height": u.Height,
		})
	}

	writeJSON(w, map[string]interface{}{
		"address": address,
		"count":   len(items),
		"utxos":   items,
	})
}

// GET /api/address/:address/history
// Returns transaction history for an address, sorted newest first.
func handleAddressHistory(w http.ResponseWriter, r *http.Request) {
	address := r.PathValue("address")
	limit := clampInt(queryInt(r, "limit", 50), 1, 1000)
	offset := queryInt(r, "offset", 0)
	order := -1
	if r.URL.Query().Get("order") == "asc" {
		order = 1
	}

	ctx := r.Context()
	cursor, err := db.Instance.AddrTx.Find(ctx,
		bson.M{"a": address},
		options.Find().
			SetProjection(bson.M{"_id": 0, "a": 1, "x": 1, "h": 1, "i": 1, "d": 1, "r": 1, "s": 1}).
			SetSort(bson.D{{Key: "h", Value: order}, {Key: "i", Value: order}}).
			SetSkip(int64(offset)).
			SetLimit(int64(limit)),
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var entries []types.AddrTxDoc
	if err := cursor.All(ctx, &entries); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		items = append(items, map[string]interface{}{
			"txid":       e.TxID,
			"delta":      e.Delta,
			"height":     e.Height,
			"blockIndex": e.TxIndex,
		})
	}

	// Get total count: try addr_stats first (fast), fall back to counting addr_tx entries
	var stats struct {
		TxCount int64 `bson:"c"`
	}
	_ = db.Instance.AddrStats.FindOne(ctx, bson.M{"_id": address},
		options.FindOne().SetProjection(bson.M{"c": 1}),
	).Decode(&stats)
	total := stats.TxCount
	if total <= 0 {
		if c, err := db.Instance.AddrTx.CountDocuments(ctx, bson.M{"a": address}); err == nil {
			total = c
		}
	}

	writeJSON(w, map[string]interface{}{
		"address": address,
		"total":   total,
		"offset":  offset,
		"limit":   limit,
		"items":   items,
	})
}

// GET /api/address/:address/mempool
// Returns pending (unconfirmed) transactions affecting this address.
func handleAddressMempool(w http.ResponseWriter, r *http.Request) {
	address := r.PathValue("address")

	ctx := r.Context()
	cursor, err := db.Instance.MempoolTx.Find(ctx, bson.M{"addrs.a": address})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var mempoolTxs []types.MempoolTxDoc
	if err := cursor.All(ctx, &mempoolTxs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := make([]map[string]interface{}, 0, len(mempoolTxs))
	for _, m := range mempoolTxs {
		delta := int64(0)
		for _, a := range m.Addrs {
			if a.Address == address {
				delta = a.Delta
				break
			}
		}
		items = append(items, map[string]interface{}{
			"txid":      m.ID,
			"delta":     delta,
			"size":      m.Size,
			"firstSeen": m.FirstSeen,
		})
	}

	writeJSON(w, map[string]interface{}{
		"address": address,
		"count":   len(items),
		"items":   items,
	})
}

// GET /api/mempool/recent?limit=N
// Returns the N most recently seen mempool transactions (default 20, max 100).
// Used by the explorer's live mempool list. Data is sourced directly from MongoDB
// (much faster than calling getrawmempool + getrawtransaction per TX via RPC).
func handleMempoolRecent(w http.ResponseWriter, r *http.Request) {
	limit := clampInt(queryInt(r, "limit", 20), 1, 500)

	ctx := r.Context()
	cursor, err := db.Instance.MempoolTx.Find(ctx,
		bson.M{},
		options.Find().
			SetSort(bson.D{{Key: "firstSeen", Value: -1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var docs []types.MempoolTxDoc
	if err := cursor.All(ctx, &docs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := make([]map[string]interface{}, 0, len(docs))
	for _, m := range docs {
		// Value = sum of all output deltas stored in addrs
		var value int64
		for _, a := range m.Addrs {
			if a.Delta > 0 {
				value += a.Delta
			}
		}
		size := m.Size
		if size == 0 {
			size = 1
		}
		items = append(items, map[string]interface{}{
			"txid":      m.ID,
			"fee":       m.Fee,
			"feeRate":   math.Round(float64(m.Fee)/float64(size)*100) / 100,
			"vsize":     size,
			"value":     value,
			"firstSeen": m.FirstSeen,
		})
	}

	writeJSON(w, map[string]interface{}{
		"count": len(items),
		"items": items,
	})
}

// GET /api/mempool/nextblock
// Returns all mempool transactions that would fit in the next block (~1 MB vsize),
// sorted by fee rate descending. Includes value (sum of positive address deltas).
// Used by the Bubble Cloud visualisation on the explorer.
func handleMempoolNextBlock(w http.ResponseWriter, r *http.Request) {
	const maxBlockVsize = 1_000_000

	ctx := r.Context()

	// Fetch all mempool TXs from MongoDB (typically < 100k docs, fast)
	cursor, err := db.Instance.MempoolTx.Find(ctx, bson.M{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var docs []types.MempoolTxDoc
	if err := cursor.All(ctx, &docs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Build entries with feeRate and value
	type entry struct {
		txid      string
		fee       int64
		vsize     int
		feeRate   float64
		value     int64
		firstSeen time.Time
	}
	entries := make([]entry, 0, len(docs))
	for _, m := range docs {
		size := m.Size
		if size == 0 {
			size = 1
		}
		var value int64
		for _, a := range m.Addrs {
			if a.Delta > 0 {
				value += a.Delta
			}
		}
		entries = append(entries, entry{
			txid:      m.ID,
			fee:       m.Fee,
			vsize:     size,
			feeRate:   math.Round(float64(m.Fee)/float64(size)*100) / 100,
			value:     value,
			firstSeen: m.FirstSeen,
		})
	}

	// Sort by feeRate descending (highest first = most likely to be in next block)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].feeRate > entries[j].feeRate
	})

	// Take entries until cumulative vsize fills one block
	var totalVsize int
	items := make([]map[string]interface{}, 0, 4000)
	for _, e := range entries {
		if totalVsize+e.vsize > maxBlockVsize {
			break
		}
		totalVsize += e.vsize
		items = append(items, map[string]interface{}{
			"txid":      e.txid,
			"fee":       e.fee,
			"feeRate":   e.feeRate,
			"vsize":     e.vsize,
			"value":     e.value,
			"firstSeen": e.firstSeen,
		})
	}

	writeJSON(w, map[string]interface{}{
		"count":       len(items),
		"totalVsize":  totalVsize,
		"mempoolSize": len(docs),
		"items":       items,
	})
}

// GET /api/tx/:txid
// Returns indexed information about a transaction.
func handleTx(w http.ResponseWriter, r *http.Request) {
	txid := r.PathValue("txid")

	ctx := r.Context()
	cursor, err := db.Instance.AddrTx.Find(ctx, bson.M{"x": txid})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var entries []types.AddrTxDoc
	if err := cursor.All(ctx, &entries); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if len(entries) == 0 {
		writeError(w, http.StatusNotFound, "Transaction not found in index")
		return
	}

	addresses := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		addresses = append(addresses, map[string]interface{}{
			"address": e.Address,
			"delta":   e.Delta,
		})
	}

	writeJSON(w, map[string]interface{}{
		"txid":       txid,
		"height":     entries[0].Height,
		"blockIndex": entries[0].TxIndex,
		"addresses":  addresses,
	})
}

// GET /api/status
// Returns sync status and database statistics.
func handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	state, err := db.Instance.GetSyncState(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	nodeHeight, _ := rpc.GetBlockCount(ctx)
	dbStats, _ := db.Instance.GetStats(ctx)

	progress := "0.00"
	if nodeHeight > 0 {
		progress = fmt.Sprintf("%.2f", float64(state.Height)/float64(nodeHeight)*100)
	}

	writeJSON(w, map[string]interface{}{
		"coin":         config.C.Coin,
		"coinName":     config.C.CoinName,
		"syncedHeight": state.Height,
		"nodeHeight":   nodeHeight,
		"progress":     progress + "%",
		"isSynced":     state.Height >= nodeHeight,
		"lastUpdate":   state.UpdatedAt,
		"database":     dbStats,
	})
}

// GET /health
func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"status": "ok",
		"coin":   config.C.Coin,
	})
}

// ============================
// On-Demand Stats Calculation
// ============================

// computeAndSaveAddrStats calculates address statistics on-demand from addr_tx
// and saves the result to addr_stats. Once saved, live delta updates from the
// indexer will keep it current as new blocks arrive.
func computeAndSaveAddrStats(ctx context.Context, address string) (*types.AddrStatsDoc, error) {
	pipeline := bson.A{
		bson.M{"$match": bson.M{"a": address}},
		bson.M{"$group": bson.M{
			"_id": "$a",
			"r":   bson.M{"$sum": "$r"},
			"s":   bson.M{"$sum": "$s"},
			"c":   bson.M{"$sum": 1},
			"f":   bson.M{"$min": "$h"},
			"l":   bson.M{"$max": "$h"},
		}},
	}

	cursor, err := db.Instance.AddrTx.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate addr_tx: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		// No transactions found for this address
		return nil, nil
	}

	// Use float64 for r/s: MongoDB $sum returns double when result exceeds int64 max.
	var result struct {
		Address  string  `bson:"_id"`
		Received float64 `bson:"r"`
		Sent     float64 `bson:"s"`
		TxCount  int64   `bson:"c"`
		First    int64   `bson:"f"`
		Last     int64   `bson:"l"`
	}
	if err := cursor.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode aggregation result: %w", err)
	}

	clampInt64 := func(f float64) int64 {
		if f >= float64(math.MaxInt64) {
			return math.MaxInt64
		}
		if f <= float64(math.MinInt64) {
			return math.MinInt64
		}
		return int64(f)
	}
	receivedInt := clampInt64(result.Received)
	sentInt := clampInt64(result.Sent)

	balance := receivedInt - sentInt

	// Save to addr_stats (upsert) so live deltas can update it going forward
	_, err = db.Instance.AddrStats.UpdateOne(ctx,
		bson.M{"_id": address},
		bson.M{"$set": bson.M{
			"b": balance,
			"r": receivedInt,
			"s": sentInt,
			"c": result.TxCount,
			"f": result.First,
			"l": result.Last,
		}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		logger.Warnf(fmt.Sprintf("Failed to save computed addr_stats for %s: %s", address, err.Error()))
	} else {
		logger.Infof(fmt.Sprintf("On-demand addr_stats computed and saved for %s (txCount=%d, balance=%d)", address, result.TxCount, balance))
	}

	// Race-condition guard: if new blocks arrived during computation,
	// addr_tx will have more entries than we aggregated (flushBatch skipped
	// them because addr_stats didn't exist yet). Re-aggregate until stable.
	for attempt := 0; attempt < 3; attempt++ {
		finalCount, cntErr := db.Instance.AddrTx.CountDocuments(ctx, bson.M{"a": address})
		if cntErr != nil || finalCount == result.TxCount {
			break // stable or error — stop
		}
		logger.Infof(fmt.Sprintf("addr_tx changed during computation for %s (%d→%d), re-aggregating", address, result.TxCount, finalCount))

		cursor2, err2 := db.Instance.AddrTx.Aggregate(ctx, pipeline)
		if err2 != nil {
			break
		}
		if cursor2.Next(ctx) {
			if decErr := cursor2.Decode(&result); decErr == nil {
				receivedInt = clampInt64(result.Received)
				sentInt = clampInt64(result.Sent)
				balance = receivedInt - sentInt
				_, _ = db.Instance.AddrStats.UpdateOne(ctx,
					bson.M{"_id": address},
					bson.M{"$set": bson.M{
						"b": balance,
						"r": receivedInt,
						"s": sentInt,
						"c": result.TxCount,
						"f": result.First,
						"l": result.Last,
					}},
					options.UpdateOne().SetUpsert(true),
				)
				logger.Infof(fmt.Sprintf("Re-aggregated addr_stats for %s (txCount=%d, balance=%d)", address, result.TxCount, balance))
			}
		}
		cursor2.Close(ctx)
	}

	return &types.AddrStatsDoc{
		ID:        address,
		Balance:   balance,
		Received:  receivedInt,
		Sent:      sentInt,
		TxCount:   result.TxCount,
		FirstSeen: result.First,
		LastSeen:  result.Last,
	}, nil
}

// ============================
// Helpers
// ============================

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func queryInt(r *http.Request, key string, defaultVal int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

func clampInt(val, min, max int) int {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}
