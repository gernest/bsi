package checksum

import (
	"github.com/cespare/xxhash/v2"
)

// Hash returns uint64 xxhash checksum of data. This is the only hash function used.
// We try to be as close as we can with prometheus, which also uses xxhash.
func Hash(data []byte) uint64 {
	return xxhash.Sum64(data)
}
