package rows

import (
	"sync"
	"time"

	"github.com/gernest/u128/internal/rbf"
)

var rowsPool = &sync.Pool{New: func() any { return new(Rows) }}

// View is ISO 8601 year and week
type View = rbf.View

// Set stores rows categorized by views.
type Set map[View]*Rows

// Release returns rows to the pool.
func (r Set) Release() {
	for _, v := range r {
		v.Reset()
		rowsPool.Put(v)
	}
	clear(r)
}

func (r Set) GetUnixMilli(ts int64) *Rows {
	return r.Get(time.UnixMilli(ts).ISOWeek())
}

// Get creates new rows or returns existing one for the (year, week) tuple.
func (r Set) Get(year, week int) *Rows {
	v := View{Year: uint16(year), Week: uint8(week)}
	x, ok := r[v]
	if !ok {
		x = rowsPool.Get().(*Rows)
		r[v] = x
	}
	return x
}
