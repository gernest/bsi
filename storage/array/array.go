package array

import (
	"slices"
)

type Uint64 struct {
	data []uint64
}

func (a *Uint64) Reset() {
	clear(a.data)
	a.data = a.data[:0]
}

func (a *Uint64) Len() int {
	return len(a.data)
}

func (a *Uint64) Allocate(size int) []uint64 {
	off := len(a.data)
	a.data = slices.Grow(a.data, size)[:off+size]
	return a.data[off:]
}

type Bool struct {
	data []bool
}

func (a *Bool) Reset() {
	clear(a.data)
	a.data = a.data[:0]
}

func (a *Bool) Len() int {
	return len(a.data)
}

func (a *Bool) Allocate(size int) []bool {
	off := len(a.data)
	a.data = slices.Grow(a.data, size)[:off+size]
	return a.data[off:]
}
