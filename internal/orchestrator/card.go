package orchestrator

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var errBadCardNumber = errors.New("card_number must be 12-19 digits")

// CardHasher turns a card number into a stable keyed fingerprint.
type CardHasher struct {
	key []byte
}

// NewCardHasher builds a CardHasher; the key must be at least 32 bytes.
func NewCardHasher(key []byte) (CardHasher, error) {
	if len(key) < 32 {
		return CardHasher{}, errors.New("card hash key must be at least 32 bytes")
	}
	return CardHasher{key: key}, nil
}

// Hash validates the number and returns its HMAC-SHA256 in hex.
func (h CardHasher) Hash(number string) (string, error) {
	if len(number) < 12 || len(number) > 19 {
		return "", errBadCardNumber
	}
	for _, c := range number {
		if c < '0' || c > '9' {
			return "", errBadCardNumber
		}
	}
	mac := hmac.New(sha256.New, h.key)
	mac.Write([]byte(number))
	return hex.EncodeToString(mac.Sum(nil)), nil
}
