// Chain Indexer — Entry Point (Go)
//
// High-performance UTXO chain indexer for Bitcoin, Litecoin, and compatible coins.
// Rewritten from TypeScript for maximum performance, stability, and lower resource usage.
//
// Orchestrates all components:
//  1. Load configuration
//  2. Connect to MongoDB
//  3. Test RPC connection
//  4. Create indexes
//  5. Start indexer (block sync)
//  6. Start mempool watcher
//  7. Start API server
//
// CLI flags:
//
//	--api-only         Only start the API server (no indexing)
//	--no-api           Only run the indexer (no API server)
//	--no-mempool       Skip mempool watcher
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/halilbeydilli/litecoin-indexer/internal/api"
	"github.com/halilbeydilli/litecoin-indexer/internal/config"
	"github.com/halilbeydilli/litecoin-indexer/internal/db"
	"github.com/halilbeydilli/litecoin-indexer/internal/indexer"
	"github.com/halilbeydilli/litecoin-indexer/internal/logger"
	mempoolpkg "github.com/halilbeydilli/litecoin-indexer/internal/mempool"
	"github.com/halilbeydilli/litecoin-indexer/internal/rpc"
)

const version = "2.0.0"

func main() {
	// Parse CLI flags
	args := os.Args[1:]
	flagAPIOnly := hasFlag(args, "--api-only")
	flagNoAPI := hasFlag(args, "--no-api")
	flagNoMempool := hasFlag(args, "--no-mempool")

	// Step 1: Load configuration
	config.Load()
	rpc.Init()

	logger.Infof(strings.Repeat("=", 60))
	logger.Infof(fmt.Sprintf("Chain Indexer v%s (Go) — %s (%s)", version, strings.ToUpper(config.C.CoinName), config.C.Coin))
	logger.Infof(strings.Repeat("=", 60))
	logger.Infof("Configuration", logger.F(
		"rpc", fmt.Sprintf("%s:%d", config.C.RPC.Host, config.C.RPC.Port),
		"mongo", fmt.Sprintf("%s/%s", config.C.Mongo.URI, config.C.Mongo.DB),
		"api", fmt.Sprintf("%s:%d", config.C.API.Host, config.C.API.Port),
	))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Step 2: Connect to MongoDB
	if err := db.Connect(ctx); err != nil {
		logger.Errorf("Cannot connect to MongoDB", logger.F("error", err.Error()))
		os.Exit(1)
	}
	defer db.Instance.Close(context.Background())

	// Step 3: Test RPC connection
	if !rpc.TestConnection(ctx) {
		logger.Errorf("Cannot connect to RPC node. Check your .env configuration.")
		os.Exit(1)
	}

	// Step 4: Determine sync mode and create appropriate indexes
	if flagAPIOnly {
		if err := db.Instance.CreateAllIndexes(ctx); err != nil {
			logger.Errorf("Failed to create indexes", logger.F("error", err.Error()))
			os.Exit(1)
		}
	} else {
		state, err := db.Instance.GetSyncState(ctx)
		if err != nil {
			logger.Errorf("Failed to get sync state", logger.F("error", err.Error()))
			os.Exit(1)
		}
		nodeHeight, _ := rpc.GetBlockCount(ctx)
		isInitialSync := state.Height < nodeHeight-10

		if isInitialSync {
			if err := db.Instance.CreateSyncIndexes(ctx); err != nil {
				logger.Errorf("Failed to create sync indexes", logger.F("error", err.Error()))
				os.Exit(1)
			}
		} else {
			if err := db.Instance.CreateAllIndexes(ctx); err != nil {
				logger.Errorf("Failed to create indexes", logger.F("error", err.Error()))
				os.Exit(1)
			}
		}
	}

	// Handle --api-only flag
	if flagAPIOnly {
		server := api.NewServer()
		logger.Infof("Running in API-only mode. No indexing will be performed.")

		// Graceful shutdown
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			<-sigCh
			logger.Infof("Shutting down API server...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			server.Shutdown(shutdownCtx)
		}()

		if err := server.Start(); err != nil {
			logger.Errorf("API server error", logger.F("error", err.Error()))
			os.Exit(1)
		}
		return
	}

	// Step 5: Start API server (unless --no-api)
	var apiServer *api.Server
	if !flagNoAPI {
		apiServer = api.NewServer()
		go func() {
			if err := apiServer.Start(); err != nil {
				logger.Errorf("API server error", logger.F("error", err.Error()))
			}
		}()
	}

	// Step 6: Start indexer
	idx := indexer.New()

	// Step 7: Start mempool watcher (unless --no-mempool)
	var mempoolWatcher *mempoolpkg.Watcher
	var wg sync.WaitGroup
	if !flagNoMempool {
		mempoolWatcher = mempoolpkg.NewWatcher()
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Small delay to let indexer start first
			time.Sleep(10 * time.Second)
			if err := mempoolWatcher.Start(); err != nil {
				logger.Errorf("Mempool watcher error", logger.F("error", err.Error()))
			}
		}()
	}

	// Graceful shutdown handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Infof(fmt.Sprintf("Received %s, shutting down gracefully...", sig))

		// 1. Stop accepting new API requests and drain active ones
		if apiServer != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = apiServer.Shutdown(shutdownCtx)
			shutdownCancel()
			logger.Infof("API server stopped")
		}

		// 2. Stop mempool watcher
		if mempoolWatcher != nil {
			mempoolWatcher.Stop()
		}

		// 3. Stop indexer (flushes remaining batch)
		idx.Stop()
	}()

	// Start indexer (runs the sync loop, blocks until stop)
	if err := idx.Start(); err != nil {
		logger.Errorf("Indexer fatal error", logger.F("error", err.Error()))
		os.Exit(1)
	}

	// Wait for mempool watcher goroutine to fully exit before closing DB
	wg.Wait()
	logger.Infof("All goroutines stopped, closing DB...")
}

func hasFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}
