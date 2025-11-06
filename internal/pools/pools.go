package pools

import "sync"

type PooledItem[T any] interface {
	Init() T
	Reset(T) T
}

type Pool[T any] struct {
	Init PooledItem[T]
	base sync.Pool
}

func (p *Pool[T]) Get() T {
	if v := p.base.Get(); v != nil {
		return v.(T)
	}
	return p.Init.Init()
}

func (p *Pool[T]) Put(v T) {
	p.base.Put(p.Init.Reset(v))
}
