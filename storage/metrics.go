package storage

type Rows struct {
	Labels    [][]byte
	Timestamp []int64
	Value     []uint64
	Histogram [][]byte
}
