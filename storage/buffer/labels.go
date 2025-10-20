package buffer

import (
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
