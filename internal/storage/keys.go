package storage

// common keys used in storage.
const (
	MetricsTimestamp uint64 = iota + 1
	MetricsValue
	MetricsType
	MetricsLabels
	MetricsLabelsExistence
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
