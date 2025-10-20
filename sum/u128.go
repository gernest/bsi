package sum

import (
	"github.com/zeebo/xxh3"
)

type U128 = xxh3.Uint128

// Hash returns 128 bit hash.
func Hash(data []byte) U128 {
	return xxh3.Hash128(data)
}
