// Package db manages MongoDB connections, collection references, and index creation.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/halilbeydilli/litecoin-indexer/internal/config"
	"github.com/halilbeydilli/litecoin-indexer/internal/logger"
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// DB holds the MongoDB client and collection references.
type DB struct {
	client    *mongo.Client
	database  *mongo.Database
	Utxos     *mongo.Collection
	AddrTx    *mongo.Collection
	AddrStats *mongo.Collection
	SyncState *mongo.Collection
	BlockUndo *mongo.Collection
	MempoolTx *mongo.Collection
}

// Global singleton
var Instance *DB

// Connect establishes the MongoDB connection and initializes collection references.
func Connect(ctx context.Context) error {
	cfg := config.C.Mongo

	wc := writeconcern.Unacknowledged()
	if config.C.Indexer.WriteConcern == 1 {
		wc = writeconcern.Majority()
	}

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetWriteConcern(wc).
		SetMaxPoolSize(30).
		SetMinPoolSize(5).
		SetConnectTimeout(300 * time.Second).
		SetMaxConnIdleTime(10 * time.Minute).
		SetRetryWrites(true).
		SetServerSelectionTimeout(30 * time.Second)
		// SetCompressors([]string{"zstd", "snappy", "zlib"}).

	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}

	// Verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongo ping: %w", err)
	}

	database := client.Database(cfg.DB)

	Instance = &DB{
		client:    client,
		database:  database,
		Utxos:     database.Collection("utxos"),
		AddrTx:    database.Collection("addr_tx"),
		AddrStats: database.Collection("addr_stats"),
		SyncState: database.Collection("sync_state"),
		BlockUndo: database.Collection("block_undo"),
		MempoolTx: database.Collection("mempool_tx"),
	}

	logger.Infof("MongoDB connected", logger.F("db", cfg.DB))
	return nil
}

// Close terminates the MongoDB connection.
func (d *DB) Close(ctx context.Context) error {
	if d.client != nil {
		err := d.client.Disconnect(ctx)
		d.client = nil
		d.database = nil
		logger.Infof("MongoDB connection closed")
		return err
	}
	return nil
}

// ============================
// Index Management
// ============================

// CreateSyncIndexes creates minimal indexes needed during initial block sync.
// Only utxo height + addr_tx height for crash recovery cleanup.
func (d *DB) CreateSyncIndexes(ctx context.Context) error {
	logger.Infof("Creating minimal sync indexes...")

	// utxo height index
	_, err := d.Utxos.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "h", Value: 1}},
		Options: options.Index().SetName("idx_utxo_height"),
	})
	if err != nil {
		return fmt.Errorf("create utxo height index: %w", err)
	}

	// addr_tx height index for crash recovery
	logger.Infof("Building addr_tx height index (may take a few minutes on existing data)...")
	_, err = d.AddrTx.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "h", Value: 1}},
		Options: options.Index().SetName("idx_addrtx_height"),
	})
	if err != nil {
		return fmt.Errorf("create addr_tx height index: %w", err)
	}

	logger.Infof("Sync indexes ready")
	return nil
}

// CreateAllIndexes creates all required secondary indexes.
// Should be called AFTER initial sync for best performance.
// MongoDB createIndex is idempotent — safe to call multiple times.
func (d *DB) CreateAllIndexes(ctx context.Context) error {
	logger.Infof("Creating all secondary indexes (this may take several minutes)...")

	// ---- utxos ----
	if _, err := d.Utxos.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "a", Value: 1}},
		Options: options.Index().SetName("idx_utxo_address"),
	}); err != nil {
		return fmt.Errorf("create utxo address index: %w", err)
	}

	if _, err := d.Utxos.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "h", Value: 1}},
		Options: options.Index().SetName("idx_utxo_height"),
	}); err != nil {
		return fmt.Errorf("create utxo height index: %w", err)
	}

	// ---- addr_tx ----
	if _, err := d.AddrTx.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "a", Value: 1}, {Key: "h", Value: -1}, {Key: "i", Value: -1}},
		Options: options.Index().SetName("idx_addrtx_history"),
	}); err != nil {
		return fmt.Errorf("create addr_tx history index: %w", err)
	}

	if _, err := d.AddrTx.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "h", Value: 1}},
		Options: options.Index().SetName("idx_addrtx_height"),
	}); err != nil {
		return fmt.Errorf("create addr_tx height index: %w", err)
	}

	// ---- addr_stats ----
	if _, err := d.AddrStats.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "b", Value: -1}},
		Options: options.Index().SetName("idx_addrstats_balance"),
	}); err != nil {
		return fmt.Errorf("create addr_stats balance index: %w", err)
	}

	// ---- mempool_tx ----
	if _, err := d.MempoolTx.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "addrs.a", Value: 1}},
		Options: options.Index().SetName("idx_mempool_address"),
	}); err != nil {
		return fmt.Errorf("create mempool address index: %w", err)
	}

	// TTL index: auto-expire mempool entries after 72 hours
	ttlSec := int32(72 * 3600)
	if _, err := d.MempoolTx.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "firstSeen", Value: 1}},
		Options: options.Index().SetName("idx_mempool_ttl").SetExpireAfterSeconds(ttlSec),
	}); err != nil {
		return fmt.Errorf("create mempool ttl index: %w", err)
	}

	logger.Infof("All indexes created successfully")
	return nil
}

// ============================
// Sync State Methods
// ============================

// GetSyncState returns the current sync state, or a default (height = -1).
func (d *DB) GetSyncState(ctx context.Context) (*types.SyncStateDoc, error) {
	var state types.SyncStateDoc
	err := d.SyncState.FindOne(ctx, bson.M{"_id": config.C.Coin}).Decode(&state)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &types.SyncStateDoc{
				ID:        config.C.Coin,
				Height:    -1,
				Hash:      "",
				StartedAt: time.Now(),
				UpdatedAt: time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("get sync state: %w", err)
	}
	return &state, nil
}

// UpdateSyncState updates the sync state after processing a block.
func (d *DB) UpdateSyncState(ctx context.Context, height int64, hash string) error {
	now := time.Now()
	_, err := d.SyncState.UpdateOne(ctx,
		bson.M{"_id": config.C.Coin},
		bson.M{
			"$set":         bson.M{"height": height, "hash": hash, "updatedAt": now},
			"$setOnInsert": bson.M{"startedAt": now},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("update sync state: %w", err)
	}
	return nil
}

// PruneUndoData removes block_undo entries older than the safety depth.
func (d *DB) PruneUndoData(ctx context.Context, currentHeight int64) error {
	cutoff := currentHeight - config.C.Indexer.ReorgSafetyDepth
	if cutoff > 0 {
		_, err := d.BlockUndo.DeleteMany(ctx, bson.M{"_id": bson.M{"$lt": cutoff}})
		if err != nil {
			return fmt.Errorf("prune undo data: %w", err)
		}
	}
	return nil
}

// GetStats returns document count estimates for monitoring.
func (d *DB) GetStats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64, 4)

	utxos, err := d.Utxos.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, err
	}
	stats["utxos"] = utxos

	addrTx, err := d.AddrTx.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, err
	}
	stats["addrTx"] = addrTx

	addrStats, err := d.AddrStats.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, err
	}
	stats["addrStats"] = addrStats

	mempoolTx, err := d.MempoolTx.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, err
	}
	stats["mempoolTx"] = mempoolTx

	return stats, nil
}

// IsDuplicateKeyError checks if a MongoDB error is a duplicate key error (code 11000).
func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if we, ok := err.(mongo.WriteException); ok {
		for _, e := range we.WriteErrors {
			if e.Code != 11000 {
				return false
			}
		}
		return len(we.WriteErrors) > 0
	}
	if be, ok := err.(mongo.BulkWriteException); ok {
		for _, e := range be.WriteErrors {
			if e.Code != 11000 {
				return false
			}
		}
		return len(be.WriteErrors) > 0
	}
	return false
}
