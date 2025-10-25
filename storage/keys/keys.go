package keys

import (
	"encoding/binary"

	"github.com/gernest/u128/checksum"
)

// common keys used in storage.
var (
	Root             = sum(0)
	MetricsTimestamp = sum(1)
	MetricsHistogram = sum(2)
	MetricsExemplar  = sum(3)
	MetricsMetadata  = sum(4)
	MetricsValue     = sum(5)
	MetricsType      = sum(6)
	MetricsLabels    = sum(7)
)

type Kind byte

const (
	Float Kind = iota + 1
	Histogram
	Exemplar
	Metadata
)

func sum(lo uint64) (h checksum.U128) {
	binary.BigEndian.PutUint64(h[8:], lo)
	return
}
