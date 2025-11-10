package buffer

import (
	"iter"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// local used for plumbing labels.labels.
type local struct {
	data string
}

// UnwrapLabel returns the backing la string as slice. This avoids copying.
func UnwrapLabel(la *labels.Labels) []byte {
	lo := (*local)(unsafe.Pointer(la))
	return unsafe.Slice(unsafe.StringData(lo.data), len(lo.data))
}

// WrapLabel returns labels.labels from the backing data without copy.
func WrapLabel(data []byte) labels.Labels {
	lo := local{data: string(data)}
	return *(*labels.Labels)(unsafe.Pointer(&lo))
}

// RangeLabels iterates over labels.Labels without allocation. Yields name and value.
func RangeLabels(data []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		var name, value []byte
		for len(data) > 0 {
			name, data = consumeString(data)
			value, data = consumeString(data)
			if !yield(name, value) {
				return
			}
		}
	}
}

func consumeString(data []byte) (value, left []byte) {
	size, index := decodeSize(data, 0)
	return data[index : index+size], data[index+size:]
}

func decodeSize(data []byte, index int) (int, int) {
	b := data[index]
	index++
	if b == 255 {
		// Larger numbers are encoded as 3 bytes little-endian.
		// Just panic if we go of the end of data, since all Labels strings are constructed internally and
		// malformed data indicates a bug, or memory corruption.
		return int(data[index]) + (int(data[index+1]) << 8) + (int(data[index+2]) << 16), index + 3
	}
	// More common case of a single byte, value 0..254.
	return int(b), index
}
