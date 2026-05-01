// Package indexer contains the UTXO cache implementation.
// Two-tier cache: "pending" (not yet flushed to DB) and "committed" (in DB).
// Pending entries are NEVER evicted. Committed entries can be evicted under pressure.
package indexer

import (
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"
	"fmt"
)

// UtxoCache keeps recently created UTXOs in memory to avoid MongoDB reads.
// During initial sync, 80-90% of UTXO spends hit the cache because
// most UTXOs are consumed within a few hundred blocks of creation.
type UtxoCache struct {
	cache       map[string]types.ResolvedUtxo
	pendingKeys map[string]struct{} // keys that haven't been flushed to DB yet
	maxSize     int

	hits   int64
	misses int64
}

// NewUtxoCache creates a new UTXO cache with the given max size.
func NewUtxoCache(maxSize int) *UtxoCache {
	return &UtxoCache{
		cache:       make(map[string]types.ResolvedUtxo, maxSize/2),
		pendingKeys: make(map[string]struct{}, maxSize/4),
		maxSize:     maxSize,
	}
}

// AddNew adds a newly created UTXO (pending = not yet written to DB).
func (c *UtxoCache) AddNew(outpoint string, utxo types.ResolvedUtxo) {
	c.cache[outpoint] = utxo
	c.pendingKeys[outpoint] = struct{}{}
}

// AddFromDB adds a UTXO fetched from DB to cache for future lookups.
func (c *UtxoCache) AddFromDB(outpoint string, utxo types.ResolvedUtxo) {
	c.cache[outpoint] = utxo
}

// SpendResult contains the result of spending a cached UTXO.
type SpendResult struct {
	Utxo     types.ResolvedUtxo
	DBDelete bool // true = was committed (needs DB delete), false = was pending (cancel out)
}

// Spend removes a UTXO from cache. Returns nil on cache miss.
// On hit: returns the UTXO data and whether a DB delete is needed.
func (c *UtxoCache) Spend(outpoint string) *SpendResult {
	utxo, ok := c.cache[outpoint]
	if !ok {
		c.misses++
		return nil
	}
	c.hits++
	delete(c.cache, outpoint)
	_, wasPending := c.pendingKeys[outpoint]
	delete(c.pendingKeys, outpoint)
	return &SpendResult{
		Utxo:     utxo,
		DBDelete: !wasPending,
	}
}

// DrainPendingInserts returns all pending UTXOs as UtxoDoc slice for DB insertion.
// After this call, remaining cache entries are considered committed.
func (c *UtxoCache) DrainPendingInserts() []types.UtxoDoc {
	docs := make([]types.UtxoDoc, 0, len(c.pendingKeys))
	for outpoint := range c.pendingKeys {
		utxo, ok := c.cache[outpoint]
		if ok {
			docs = append(docs, types.UtxoDoc{
				ID:      outpoint,
				Address: utxo.Address,
				Value:   utxo.Value,
				Height:  utxo.Height,
			})
		}
	}
	// Clear all pending keys (they're now committed)
	c.pendingKeys = make(map[string]struct{}, c.maxSize/4)
	return docs
}

// EvictIfNeeded removes committed (non-pending) entries when cache exceeds maxSize.
func (c *UtxoCache) EvictIfNeeded() {
	if len(c.cache) <= c.maxSize {
		return
	}
	toEvict := len(c.cache) - c.maxSize + 100_000
	count := 0
	for key := range c.cache {
		if count >= toEvict {
			break
		}
		if _, isPending := c.pendingKeys[key]; isPending {
			continue // never evict pending
		}
		delete(c.cache, key)
		count++
	}
}

// Size returns the total number of entries in the cache.
func (c *UtxoCache) Size() int {
	return len(c.cache)
}

// PendingCount returns the number of pending (unflushed) entries.
func (c *UtxoCache) PendingCount() int {
	return len(c.pendingKeys)
}

// Stats returns cache statistics.
type CacheStats struct {
	Size    int
	Pending int
	HitRate string
}

func (c *UtxoCache) Stats() CacheStats {
	total := c.hits + c.misses
	rate := 0.0
	if total > 0 {
		rate = float64(c.hits) / float64(total) * 100
	}
	return CacheStats{
		Size:    len(c.cache),
		Pending: len(c.pendingKeys),
		HitRate: fmt.Sprintf("%.1f%%", rate),
	}
}

// ResetStats clears hit/miss counters.
func (c *UtxoCache) ResetStats() {
	c.hits = 0
	c.misses = 0
}
