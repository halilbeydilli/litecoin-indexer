// Package rpc provides a JSON-RPC client for Bitcoin/Litecoin Core nodes.
// Uses persistent HTTP connections with keep-alive for maximum throughput.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/halilbeydilli/litecoin-indexer/internal/config"
	"github.com/halilbeydilli/litecoin-indexer/internal/logger"
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"
)

var requestID atomic.Int64

// httpClient is a persistent HTTP client with connection pooling.
var httpClient *http.Client

// Init initializes the RPC HTTP client. Must call after config.Load().
func Init() {
	clients := config.C.RPC.Clients
	if clients <= 0 {
		clients = 20
	}
	transport := &http.Transport{
		MaxIdleConns:        clients,
		MaxIdleConnsPerHost: clients,
		MaxConnsPerHost:     clients,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	httpClient = &http.Client{
		Transport: transport,
		Timeout:   time.Duration(config.C.RPC.Timeout) * time.Millisecond,
	}
}

// rpcRequest represents a JSON-RPC 1.0 request.
type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int64         `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

// rpcResponse represents a JSON-RPC response.
type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
	ID     int64           `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// call makes a raw JSON-RPC call to the node.
func call(ctx context.Context, method string, params ...interface{}) (json.RawMessage, error) {
	if params == nil {
		params = []interface{}{}
	}

	reqBody := rpcRequest{
		JSONRPC: "1.0",
		ID:      requestID.Add(1),
		Method:  method,
		Params:  params,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("RPC marshal error [%s]: %w", method, err)
	}

	url := fmt.Sprintf("http://%s:%d", config.C.RPC.Host, config.C.RPC.Port)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("RPC request create error [%s]: %w", method, err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(config.C.RPC.User, config.C.RPC.Pass)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("RPC connection error [%s]: %w", method, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("RPC read error [%s]: %w", method, err)
	}

	var rpcResp rpcResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("RPC parse error [%s]: %w", method, err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC [%s]: code=%d msg=%s", method, rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

// callWithRetry makes an RPC call with automatic retry and exponential backoff.
func callWithRetry(ctx context.Context, method string, maxRetries int, params ...interface{}) (json.RawMessage, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := call(ctx, method, params...)
		if err == nil {
			return result, nil
		}
		lastErr = err

		// Don't retry on RPC-level errors (only retry connection/timeout errors)
		errMsg := err.Error()
		if !isRetryableError(errMsg) {
			return nil, err
		}

		if attempt < maxRetries {
			delay := time.Duration(math.Min(float64(1000*int(math.Pow(2, float64(attempt)))), 10000)) * time.Millisecond
			logger.Warnf(fmt.Sprintf("RPC retry %d/%d for %s", attempt+1, maxRetries, method),
				logger.F("delay", delay.String()))

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return nil, lastErr
}

func isRetryableError(msg string) bool {
	// Only retry on connection/timeout errors, not RPC logic errors
	for _, keyword := range []string{"connection error", "timeout", "EOF", "connection refused", "connection reset"} {
		if contains(msg, keyword) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================
// Public RPC Methods
// ============================

// GetBlockchainInfo returns blockchain info (chain, height, sync status).
func GetBlockchainInfo(ctx context.Context) (*types.RpcBlockchainInfo, error) {
	raw, err := callWithRetry(ctx, "getblockchaininfo", 3)
	if err != nil {
		return nil, err
	}
	var info types.RpcBlockchainInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("unmarshal blockchaininfo: %w", err)
	}
	return &info, nil
}

// GetBlockCount returns the current block count (best chain tip height).
func GetBlockCount(ctx context.Context) (int64, error) {
	raw, err := callWithRetry(ctx, "getblockcount", 3)
	if err != nil {
		return 0, err
	}
	var height int64
	if err := json.Unmarshal(raw, &height); err != nil {
		return 0, fmt.Errorf("unmarshal blockcount: %w", err)
	}
	return height, nil
}

// GetBlockHash returns the block hash at the given height.
func GetBlockHash(ctx context.Context, height int64) (string, error) {
	raw, err := callWithRetry(ctx, "getblockhash", 3, height)
	if err != nil {
		return "", err
	}
	var hash string
	if err := json.Unmarshal(raw, &hash); err != nil {
		return "", fmt.Errorf("unmarshal blockhash: %w", err)
	}
	return hash, nil
}

// GetBlock returns full block data with all transaction details.
// verbosity=2 includes full tx objects with vin/vout.
func GetBlock(ctx context.Context, hashOrHeight interface{}, verbosity int) (*types.RpcBlock, error) {
	var hash string
	switch v := hashOrHeight.(type) {
	case int64:
		var err error
		hash, err = GetBlockHash(ctx, v)
		if err != nil {
			return nil, err
		}
	case string:
		hash = v
	default:
		return nil, fmt.Errorf("GetBlock: invalid hashOrHeight type")
	}

	raw, err := callWithRetry(ctx, "getblock", 3, hash, verbosity)
	if err != nil {
		return nil, err
	}
	var block types.RpcBlock
	if err := json.Unmarshal(raw, &block); err != nil {
		return nil, fmt.Errorf("unmarshal block: %w", err)
	}
	return &block, nil
}

// GetRawTransaction fetches a raw transaction with full details.
func GetRawTransaction(ctx context.Context, txid string, verbose bool) (*types.RpcTransaction, error) {
	raw, err := callWithRetry(ctx, "getrawtransaction", 3, txid, verbose)
	if err != nil {
		return nil, err
	}
	var tx types.RpcTransaction
	if err := json.Unmarshal(raw, &tx); err != nil {
		return nil, fmt.Errorf("unmarshal rawtx: %w", err)
	}
	return &tx, nil
}

// GetRawMempool returns all txids currently in the mempool.
func GetRawMempool(ctx context.Context) ([]string, error) {
	raw, err := callWithRetry(ctx, "getrawmempool", 3, false)
	if err != nil {
		return nil, err
	}
	var txids []string
	if err := json.Unmarshal(raw, &txids); err != nil {
		return nil, fmt.Errorf("unmarshal rawmempool: %w", err)
	}
	return txids, nil
}

// GetMempoolEntry returns fee and size data for a single mempool transaction.
func GetMempoolEntry(ctx context.Context, txid string) (*types.RpcMempoolEntry, error) {
	raw, err := call(ctx, "getmempoolentry", txid)
	if err != nil {
		return nil, err
	}
	var entry types.RpcMempoolEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, fmt.Errorf("unmarshal mempoolentry: %w", err)
	}
	return &entry, nil
}

// TestConnection tests the RPC connection by fetching blockchain info.
func TestConnection(ctx context.Context) bool {
	info, err := GetBlockchainInfo(ctx)
	if err != nil {
		logger.Errorf("RPC connection FAILED", logger.F("error", err.Error()))
		return false
	}
	logger.Infof("RPC connection OK", logger.F(
		"chain", info.Chain,
		"blocks", info.Blocks,
		"headers", info.Headers,
		"progress", fmt.Sprintf("%.2f%%", info.VerificationProgress*100),
	))
	return true
}
