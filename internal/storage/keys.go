package storage

// common keys used in storage.
const (
	MetricsTimestamp uint64 = iota + 1
	MetricsHistogram
	MetricsExemplar
	MetricsMetadata
	MetricsValue
	MetricsType
	MetricsLabels
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
