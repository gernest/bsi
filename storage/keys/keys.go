package keys

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"

	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/prefix"
)

// common keys used in storage.
var (
	DataIndex   = []byte("d")
	SearchIndex = []byte("s")

	MetricsHistogram = []byte("h")
	MetricsValue     = []byte("v")
	MetricsTimestamp = []byte("t")
	MetricsLabels    = []byte("l")
)

// View builds ISO 8601 year and week view. We encode year in 4 digits and week in two
// digits.
func View(b *buffer.B, y, m int) []byte {
	b.B = slices.Grow(b.B, 6)[:6]
	if y < 1000 {
		_ = fmt.Appendf(b.B[:0], "%04d", y)
	} else if y >= 10000 {
		_ = fmt.Appendf(b.B[:0], "%04d", y%1000)
	} else {
		strconv.AppendInt(b.B[:0], int64(y), 10)
	}
	b.B[4] = '0' + byte(m/10)
	b.B[5] = '0' + byte(m%10)
	return b.B
}

// Key constructs  a key we use for bitmaps in rbf storage.
func Key(w *buffer.B, index, field, view []byte, shard uint64) []byte {
	w.B = w.B[:0]

	// Index is encoded as length prefix
	w.B = prefix.Encode(w.B, index)
	// Field is encoded as length prefixed
	w.B = prefix.Encode(w.B, field)

	// View is encoded as length prefixed
	w.B = prefix.Encode(w.B, view)

	// we use last 8 bytes as shard without length prefix. We use big endian to make
	// sure the strings are sorted.
	w.B = binary.BigEndian.AppendUint64(w.B, shard)
	return w.B
}

func KeyViewPrefix(w *buffer.B, index, field, view []byte) []byte {
	w.B = w.B[:0]

	// Index is encoded as length prefix
	w.B = prefix.Encode(w.B, index)
	// Field is encoded as length prefixed
	w.B = prefix.Encode(w.B, field)

	// View is encoded as length prefixed
	w.B = prefix.Encode(w.B, view)

	return w.B
}

func GetShard(key string) uint64 {
	b := magic.Slice(key)
	return binary.BigEndian.Uint64(b[len(b)-8:])
}
