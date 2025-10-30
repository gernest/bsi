// Package tsid defines api for working with timeseries samples. We assign unique
// identifiers for time series that are used throughout our RBF storage.
//
// To speed up ingestion , once processed TSID are serialized in a way that can be
// reused when we find the same series.
package tsid

import (
	"sync"
)

// B is reusable buffer of ID.
type B struct {
	B []ID
}

// Pool pools B instances to avoid excessive allocations.
type Pool struct {
	pool sync.Pool
}

// Get creates anew B instance or returns existing one from the pool.
func (p *Pool) Get() *B {
	v := p.pool.Get()
	if v != nil {
		return v.(*B)
	}
	return new(B)
}

// Put returns b to the pool, retaining b capacity.
func (p *Pool) Put(b *B) {
	b.B = b.B[:0]
	p.pool.Put(b)
}

// ID stores assigned identifiers for time series. The first Column is for the assigned
// uint64 for the whole series, the rest are (column, assigned_uint64_for_the_value) tuple
// for each series label where column=label name and  assigned_uint64=label value.
type ID []Column

// Column Identifies a prometheus label (name, value) tuple as uint64, to be used
// in RBF storage.
type Column struct {
	ID    uint64
	Value uint64
}

// ID helper method to return series ID.
func (id ID) ID() uint64 {
	return id[0].Value
}
