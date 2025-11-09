package work

import (
	"iter"
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
	// we do sequential scan for now.
	// TODO: concurrent processing once promql tests pass.
	for i := range w.todo {
		err := f(w.todo[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Work[T]) do(g *sync.WaitGroup, all []T, f func([]T)) {
	defer g.Done()

	f(all)
}
