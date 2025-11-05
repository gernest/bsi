package storage

import "sync"

type pooledItem[T any] interface {
	Init() T
	Reset(T) T
}

type pool[T any] struct {
	init pooledItem[T]
	base sync.Pool
}

func (p *pool[T]) Get() T {
	if v := p.base.Get(); v != nil {
		return v.(T)
	}
	return p.init.Init()
}

func (p *pool[T]) Put(v T) {
	p.base.Put(p.init.Reset(v))
}
