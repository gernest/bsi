package keys

// common keys used in storage.
var (
	MetricsTimestamp = uint64(1)
	MetricsHistogram = uint64(2)
	MetricsExemplar  = uint64(3)
	MetricsMetadata  = uint64(4)
	MetricsValue     = uint64(5)
	MetricsType      = uint64(6)
	MetricsLabels    = uint64(7)
)

type Kind byte

const (
	None Kind = iota
	Float
	Histogram
	FloatHistogram
	Exemplar
	Metadata
)
