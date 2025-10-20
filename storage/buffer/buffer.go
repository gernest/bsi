package buffer

import "sync"

type Pool struct {
	pool sync.Pool
}

func (p *Pool) Get() *B {
	b := p.pool.Get()
	if b != nil {
		return b.(*B)
	}
	return &B{}
}

func (p *Pool) Put(b *B) {
	b.B = b.B[:0]
	p.pool.Put(b)
}

type B struct {
	B []byte
}
