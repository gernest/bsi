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

	var (
		err    error
		cancel atomic.Bool
		once   sync.Once
	)

	do := func(all []T) {
		for i := range all {
			if cancel.Load() {
				return
			}
			e := f(all[i])
			once.Do(func() {
				err = e
				cancel.Store(true)
			})
		}
	}
	var wg sync.WaitGroup
	// each shard covers a limited number of records, we expect large queries to cover
	// shards exceeding available cores.
	//
	// Here we only spin n goroutines and distribute shards across these goroutines. Each
	// goroutine process chunks sequentially.
	// When a shard processing call returns an error the execution is stopped and no new tasks
	// are executed.
	// There is no way to stop already executing tasks when an error occur, so we will wait for existing tasks
	// ti complete. Only the first error is recorded and returned.
	// The caller must discard any state if returned err is not nil.
	for data := range slices.Chunk(w.todo, n) {
		wg.Add(1)
		go w.do(&wg, data, do)
	}
	wg.Wait()
	return err
}

func (w *Work[T]) do(g *sync.WaitGroup, all []T, f func([]T)) {
	defer g.Done()

	f(all)
}
