package work

import (
	"iter"
	"slices"
	"sync"
	"sync/atomic"
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

// Reset clears w for reuse.
func (w *Work[T]) Reset() *Work[T] {
	w.todo = w.todo[:0]
	return w
}

// Do executes f in n possible goroutines. We stop execution whn we first encounter an error.
func (w *Work[T]) Do(n int, f func(item T) error) error {
	for i := range w.todo {
		err := f(w.todo[i])
		if err != nil {
			return err
		}
	}
	var err atomic.Value
	do := func(all []T) {
		for i := range all {
			if err.Load() != nil {
				return
			}
			e := f(all[i])
			if e != nil {
				err.Store(e)
				return
			}
		}
	}
	var wg sync.WaitGroup
	for data := range slices.Chunk(w.todo, n) {
		wg.Add(1)
		go w.do(&wg, data, do)
	}
	wg.Wait()
	if v := err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (w *Work[T]) do(g *sync.WaitGroup, all []T, f func([]T)) {
	defer g.Done()

	f(all)
}
