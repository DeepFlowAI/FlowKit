package calls

import (
	cryptorand "crypto/rand"
	hexpkg "encoding/hex"
)

func cryptoRandReadImpl(b []byte) (int, error) { return cryptorand.Read(b) }

// hexEncode returns the hex string for b — used by newShortID.
func hexEncode(b []byte) string { return hexpkg.EncodeToString(b) }

// hex is the symbol used in handlers.go (kept as a var so tests can swap).
var hex = hexEncode
