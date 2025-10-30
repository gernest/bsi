package checksum

import (
	"github.com/cespare/xxhash/v2"
)

// Hash returns 128 bit hash.
func Hash(data []byte) uint64 {
	return xxhash.Sum64(data)
}
