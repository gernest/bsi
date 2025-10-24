package keys

import (
	"fmt"
	"strconv"

	"github.com/gernest/u128/checksum"
)

// common keys used in storage.
var (
	Root             = checksum.Hash([]byte("\x00__root\x00"))
	MetricsHistogram = checksum.Hash([]byte("\x00__metrics_histogram\x00"))
	MetricsValue     = checksum.Hash([]byte("\x00__metrics_value\x00"))
	MetricsTimestamp = checksum.Hash([]byte("\x00__metrics_timestamp\x00"))
	MetricsLabels    = checksum.Hash([]byte("\x00__metrics_labels\x00"))
)

// View builds ISO 8601 year and week view. We encode year in 4 digits and week in two
// digits.
func View(year, week int) string {
	b := make([]byte, 6)
	if year < 1000 {
		_ = fmt.Appendf(b[:0], "%04d", year)
	} else if year >= 10000 {
		_ = fmt.Appendf(b[:0], "%04d", year%1000)
	} else {
		strconv.AppendInt(b[:0], int64(year), 10)
	}
	b[4] = '0' + byte(week/10)
	b[5] = '0' + byte(week%10)
	return string(b)
}
