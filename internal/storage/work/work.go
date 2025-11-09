package work

import (
	"errors"
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

// Do executes f in n possible goroutines. We stop execution whn we first encounter an error.
func (w *Work[T]) Do(n int, f func(item T) error) error {
	if n < 1 {
		panic("work.Do: n < 1")
	}
	var mu sync.RWMutex
	var errs []error
	do := func(a []T) {
		mu.RLock()
		sz := len(errs)
		mu.RUnlock()
		if sz > 0 {
			return
		}
		for i := range a {
			err := f(a[i])
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
		}
	}

	var g sync.WaitGroup

	for work := range slices.Chunk(w.todo, n) {
		g.Add(1)
		go w.do(&g, work, do)
	}
	g.Wait()
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (w *Work[T]) do(g *sync.WaitGroup, all []T, f func([]T)) {
	defer g.Done()

	f(all)
}
