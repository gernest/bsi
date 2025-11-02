package tsid

import (
	"sync"
)

type B struct {
	B []ID
}

type Pool struct {
	pool sync.Pool
}

func (p *Pool) Get() *B {
	v := p.pool.Get()
	if v != nil {
		return v.(*B)
	}
	return new(B)
}

func (p *Pool) Put(b *B) {
	b.B = b.B[:0]
	p.pool.Put(b)
}

type ID []Column

type Column struct {
	ID    uint64
	Value uint64
}
