package storage

import (
	"math/bits"
	"sort"

	"github.com/gernest/bsi/internal/storage/magic"
)

type minMax struct {
	min, max int64
}

func (m *minMax) set(v int64) {
	m.min = min(m.min, v)
	m.max = max(m.max, v)
}

func (m *minMax) depth() uint8 {
	return uint8(bits.Len64(max(uint64(m.min), uint64(m.max)))) + 1
}

func (m *minMax) update(o *minMax) {
	m.min = min(m.min, o.min)
	m.max = max(m.max, o.max)
}

func (m *minMax) InRange(lo, hi int64) bool {
	return m.min <= hi && lo <= m.max
}

type bounds struct {
	col    uint64
	minMax minMax
}

func updateBounds(m map[uint64]*minMax, o []byte) {
	other := magic.ReinterpretSlice[bounds](o)
	for i := range other {
		x, ok := m[other[i].col]
		if !ok {
			n := other[i].minMax
			m[other[i].col] = &n
			continue
		}
		x.update(&other[i].minMax)
	}
}

func buildBounds(m map[uint64]*minMax) []bounds {
	result := make([]bounds, 0, len(m))

	for k, v := range m {
		result = append(result, bounds{
			col: k, minMax: *v,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].col < result[j].col
	})
	return result
}
