package keys

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/storage/buffer"
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
