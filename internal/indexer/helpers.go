package indexer

import (
	"crypto/sha256"
	"math/big"
	"strings"

	"github.com/halilbeydilli/litecoin-indexer/internal/config"
	"github.com/halilbeydilli/litecoin-indexer/pkg/types"

	"golang.org/x/crypto/ripemd160"
)

// ExtractAddress extracts an address from a scriptPubKey.
// Handles P2PKH, P2SH, P2WPKH, P2WSH, P2TR (via node's address field)
// and P2PK (raw pubkey → derive legacy address using HASH160 + base58check).
func ExtractAddress(spk types.RpcScriptPubKey) string {
	if spk.Address != "" {
		return spk.Address
	}
	if len(spk.Addresses) == 1 {
		return spk.Addresses[0]
	}

	// P2PK (pay-to-pubkey): common in early coinbase transactions
	if spk.Type == "pubkey" && spk.Asm != "" {
		parts := strings.Split(spk.Asm, " ")
		if len(parts) >= 1 {
			pubkeyHex := parts[0]
			if (len(pubkeyHex) == 130 || len(pubkeyHex) == 66) && isHex(pubkeyHex) {
				addr, err := pubkeyToP2PKHAddress(pubkeyHex)
				if err == nil {
					return addr
				}
			}
		}
	}

	return ""
}

// ToSatoshis converts a BTC/LTC float value to satoshis.
func ToSatoshis(value float64) int64 {
	return int64(value*1e8 + 0.5) // Round to nearest
}

// isHex checks if a string is valid hexadecimal.
func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// pubkeyToP2PKHAddress derives a legacy P2PKH address from a raw public key.
// pubkey → SHA256 → RIPEMD160 → version byte + hash160 → double SHA256 checksum → base58
func pubkeyToP2PKHAddress(pubkeyHex string) (string, error) {
	pubkeyBytes, err := hexDecode(pubkeyHex)
	if err != nil {
		return "", err
	}

	// SHA256
	sha := sha256.Sum256(pubkeyBytes)

	// RIPEMD160
	ripeHasher := ripemd160.New()
	ripeHasher.Write(sha[:])
	hash160 := ripeHasher.Sum(nil)

	// Version byte + hash160
	versioned := make([]byte, 21)
	versioned[0] = getP2PKHVersionByte()
	copy(versioned[1:], hash160)

	// Double SHA256 checksum
	first := sha256.Sum256(versioned)
	second := sha256.Sum256(first[:])
	checksum := second[:4]

	// Base58 encode
	payload := make([]byte, 25)
	copy(payload, versioned)
	copy(payload[21:], checksum)

	return base58Encode(payload), nil
}

// getP2PKHVersionByte returns the P2PKH address version byte.
// If P2PKH_VERSION is set in .env, that value is used directly.
// Otherwise, auto-detects from COIN_SYMBOL for all known Bitcoin forks.
func getP2PKHVersionByte() byte {
	// User override via .env takes priority
	if config.C.Indexer.P2PKHVersion >= 0 {
		return byte(config.C.Indexer.P2PKHVersion)
	}

	// Auto-detect from coin symbol (covers all major Bitcoin forks)
	switch strings.ToUpper(config.C.Coin) {
	case "BTC", "BCH", "BSV": // Bitcoin, Bitcoin Cash, Bitcoin SV
		return 0x00
	case "LTC": // Litecoin
		return 0x30
	case "DOGE": // Dogecoin
		return 0x1e
	case "DASH": // Dash
		return 0x4c
	case "XVG": // Verge
		return 0x1e
	case "ZEC": // Zcash
		return 0x1c
	case "DGB": // DigiByte
		return 0x1e
	case "RVN": // Ravencoin
		return 0x3c
	case "BTG": // Bitcoin Gold
		return 0x26
	case "PIVX": // PIVX
		return 0x1e
	case "FIRO", "XZC": // Firo (ex-Zcoin)
		return 0x52
	case "SYS": // Syscoin
		return 0x3f
	case "GRS": // Groestlcoin
		return 0x24
	case "VTC": // Vertcoin
		return 0x47
	case "MONA": // MonaCoin
		return 0x32
	case "NMC": // Namecoin
		return 0x34
	case "PPC": // Peercoin
		return 0x37
	case "FTC": // Feathercoin
		return 0x0e
	case "QTUM": // Qtum
		return 0x3a
	default:
		// Unknown coin — default to Bitcoin (0x00).
		// Set P2PKH_VERSION in .env to override for unlisted coins.
		return 0x00
	}
}

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func base58Encode(input []byte) string {
	// Count leading zeros
	leadingZeros := 0
	for _, b := range input {
		if b != 0 {
			break
		}
		leadingZeros++
	}

	// Convert to big.Int
	num := new(big.Int).SetBytes(input)
	base := big.NewInt(58)
	mod := new(big.Int)
	zero := big.NewInt(0)

	var result []byte
	for num.Cmp(zero) > 0 {
		num.DivMod(num, base, mod)
		result = append(result, base58Alphabet[mod.Int64()])
	}

	// Add leading '1's for leading zero bytes
	for i := 0; i < leadingZeros; i++ {
		result = append(result, '1')
	}

	// Reverse
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return string(result)
}

// hexDecode converts a hex string to bytes.
func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		s = "0" + s
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		high := hexVal(s[i])
		low := hexVal(s[i+1])
		if high == 255 || low == 255 {
			return nil, errInvalidHex
		}
		b[i/2] = high<<4 | low
	}
	return b, nil
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 255
	}
}

var errInvalidHex = &hexError{}

type hexError struct{}

func (e *hexError) Error() string { return "invalid hex character" }
