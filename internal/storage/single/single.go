// Package single implements suppression of duplicate open calls for shards databases.
package single

import (
	"io"
	"log/slog"
	"sync"
)

// Group ensures open is called only once for the same key. This ensures during
// high concurrent writes/reads for the same shard can be synchronized to use the
// same opened shard.
//
// We combine locking and reference counting to manage resource T. T is expected to be
// typically an instance of shard database (rbf+bolt) but it can be used with any resource
// that implement io.Closer interface.
type Group[K comparable, T io.Closer, O any] struct {
	m    map[K]*call[T]
	open func(K, O) (T, error)
	lo   *slog.Logger
	mu   sync.Mutex
}

type call[T io.Closer] struct {
	val T
	err error
	wg  sync.WaitGroup
	ref int32
}

// Close calls Close on all open resources. Returns the last observed non nil error.
func (g *Group[K, T, O]) Close() (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, v := range g.m {
		g.lo.Info("closing resource", "key", k)
		e := v.val.Close()
		if e != nil {
			g.lo.Error("failed closing resource", "key", k, "err", e)
			err = e
		}
	}
	return
}

// Init initializes group.
func (g *Group[K, T, O]) Init(open func(K, O) (T, error)) {
	g.lo = slog.Default().With("component", "singleflight")
	g.m = make(map[K]*call[T])
	g.open = open
}

// Do executes g.open only once when concurrent calls for key are made. We mainly use this
// to manage accessing shards database. We don't want to keep them in memory when we no longer
// need them, however there can only be one instance of a shard database per (signal, shard) at
// any point in time.
//
// The returned io.Closer must be be called even if error is not nil to ensure resources are properly released.
func (g *Group[K, T, O]) Do(key K, opts O) (T, io.Closer, error) {
	g.mu.Lock()
	c, ok := g.m[key]
	if ok {
		c.ref++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, g.release(key, c), c.err
	}

	c = new(call[T])
	c.wg.Add(1)
	g.m[key] = c
	g.lo.Info("opening resource", "key", key)
	c.val, c.err = g.open(key, opts)
	if c.err != nil {
		g.lo.Error("failed opening resource", "key", key, "err", c.err)
	}
	c.ref++
	c.wg.Done()
	g.mu.Unlock()
	return c.val, g.release(key, c), c.err
}

// Returns io.Closer that dereference u. If ref count is 0 we delete u from the group.
// The purpose of the group is to reduce stress during high concurrent access, it is
// not a cache.
//
// Bolt databases are pretty fast to open on demand, this is the same with rbf. Any optimizations
// and tuning should be dedicated there.
func (g *Group[K, T, O]) release(key K, u *call[T]) io.Closer {
	return release(func() error {
		g.mu.Lock()
		defer g.mu.Unlock()
		u.ref--
		if u.ref == 0 {
			g.lo.Info("releasing resource", "key", key)
			err := u.val.Close()
			delete(g.m, key)
			return err
		}
		return nil
	})
}

type release func() error

func (r release) Close() error {
	return r()
}
