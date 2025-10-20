package storage

type Row struct {
	Labels    []byte
	Timestamp int64
	Value     float64
}
