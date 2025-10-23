package keys

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"

	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/prefix"
)

// common keys used in storage.
var (
	MetricsHistogram = checksum.Hash([]byte("\x00__metrics_histogram\x00"))
	MetricsValue     = checksum.Hash([]byte("\x00__metrics_value\x00"))
	MetricsTimestamp = checksum.Hash([]byte("\x00__metrics_timestamp\x00"))
	MetricsLabels    = checksum.Hash([]byte("\x00__metrics_labels\x00"))
)

// View builds ISO 8601 year and week view. We encode year in 4 digits and week in two
// digits.
func View(b *buffer.B, year, week int) []byte {
	b.B = slices.Grow(b.B, 6)[:6]
	if year < 1000 {
		_ = fmt.Appendf(b.B[:0], "%04d", year)
	} else if year >= 10000 {
		_ = fmt.Appendf(b.B[:0], "%04d", year%1000)
	} else {
		strconv.AppendInt(b.B[:0], int64(year), 10)
	}
	b.B[4] = '0' + byte(week/10)
	b.B[5] = '0' + byte(week%10)
	return b.B
}

// Key constructs  a key we use for bitmaps in rbf storage.
func Key(w *buffer.B, index, field []byte, shard uint64) []byte {
	w.B = w.B[:0]

	// Index is encoded as length prefix
	w.B = prefix.Encode(w.B, index)
	// Field is encoded as length prefixed
	w.B = prefix.Encode(w.B, field)

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
