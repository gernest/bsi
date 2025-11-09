package work

import (
	"iter"
	"slices"
	"sync"
)

// Work manages a set of work items to be executed in parallel, at most once each.
// The items in the set must all be valid map keys.
type Work[T any] struct {
	todo []T // items yet to be run
}

// Init initializes w with todo work items
func (w *Work[T]) Init(todo iter.Seq[T]) {
	for i := range todo {
		w.todo = append(w.todo, i)
	}
}

func (w *Work[T]) Reset() *Work[T] {
	w.todo = w.todo[:0]
	return w
}

func (w *Work[T]) Do(n int, f func(item T)) {
	if n < 1 {
		panic("work.Do: n < 1")
	}

	var g sync.WaitGroup

	for work := range slices.Chunk(w.todo, n) {
		g.Add(1)
		go w.do(&g, work, f)
	}
	g.Wait()
}

func (w *Work[T]) do(g *sync.WaitGroup, all []T, f func(T)) {
	defer g.Done()

	for i := range all {
		f(all[i])
	}
}
