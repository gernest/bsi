package array

import (
	"slices"
)

type Uint64 struct {
	B []uint64
}

func (a *Uint64) Reset() {
	clear(a.B)
	a.B = a.B[:0]
}

func (a *Uint64) Len() int {
	return len(a.B)
}

func (a *Uint64) Allocate(size int) []uint64 {
	off := len(a.B)
	a.B = slices.Grow(a.B, size)[:off+size]
	return a.B[off:]
}
