// Package config loads and provides configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all configuration values for the chain indexer.
type Config struct {
	Coin     string // Coin symbol (BTC, LTC, etc.)
	CoinName string // Coin full name

	RPC     RPCConfig
	Mongo   MongoConfig
	API     APIConfig
	Indexer IndexerConfig
}

// RPCConfig holds Bitcoin/Litecoin Core RPC settings.
type RPCConfig struct {
	Host    string
	Port    int
	User    string
	Pass    string
	Timeout int // milliseconds
	Clients int // Number of parallel RPC connections (default 4)
}

// MongoConfig holds MongoDB connection settings.
type MongoConfig struct {
	URI string
	DB  string
}

// APIConfig holds REST API server settings.
type APIConfig struct {
	Port int
	Host string
}

// IndexerConfig holds indexer behavior tuning parameters.
type IndexerConfig struct {
	StartHeight      int64
	LogInterval      int64
	ReorgSafetyDepth int64
	MempoolPollMs    int
	BlocksPerTick    int
	WriteConcern     int
	BatchSize        int
	UTXOCacheMax     int
	P2PKHVersion     int // P2PKH address version byte (for P2PK → address derivation)
}

var C Config

// Load reads .env file and populates the global Config.
func Load() {
	// Load .env file (ignore error if not found)
	_ = godotenv.Load()

	C = Config{
		Coin:     envStr("COIN_SYMBOL", "BTC"),
		CoinName: envStr("COIN_NAME", "bitcoin"),
		RPC: RPCConfig{
			Host:    envStr("RPC_HOST", "127.0.0.1"),
			Port:    envInt("RPC_PORT", 8332),
			User:    envStr("RPC_USER", "bitcoinrpc"),
			Pass:    envStr("RPC_PASS", ""),
			Timeout: envInt("RPC_TIMEOUT", 120000),
		},
		Mongo: MongoConfig{
			URI: envStr("MONGO_URI", "mongodb://127.0.0.1:27017"),
			DB:  envStr("MONGO_DB", "btc_index_database"),
		},
		API: APIConfig{
			Port: envInt("API_PORT", 3101),
			Host: envStr("API_HOST", "0.0.0.0"),
		},
		Indexer: IndexerConfig{
			StartHeight:      int64(envInt("START_HEIGHT", 0)),
			LogInterval:      int64(envInt("LOG_INTERVAL", 100)),
			ReorgSafetyDepth: int64(envInt("REORG_SAFETY_DEPTH", 200)),
			MempoolPollMs:    envInt("MEMPOOL_POLL_INTERVAL", 10000),
			BlocksPerTick:    envInt("BLOCKS_PER_TICK", 10),
			WriteConcern:     envInt("WRITE_CONCERN", 0),
			BatchSize:        envInt("BATCH_SIZE", 350),
			UTXOCacheMax:     envInt("UTXO_CACHE_MAX", 5000000),
			P2PKHVersion:     envInt("P2PKH_VERSION", -1), // -1 = auto-detect from COIN_SYMBOL
		},
	}
}

func envStr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if s == "" {
		return fallback
	}
	return s == "true" || s == "1" || s == "yes"
}

func envInt(key string, fallback int) int {
	s := os.Getenv(key)
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return fallback
	}
	return v
}
