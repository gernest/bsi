package rows

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var rowsPool = &sync.Pool{New: func() any { return new(Rows) }}

// View is ISO 8601 year and week
type View struct {
	Year uint16
	Week uint16
}

func (v View) String() string {
	b := make([]byte, 6)
	if v.Year < 1000 {
		_ = fmt.Appendf(b[:0], "%04d", v.Year)
	} else if v.Year >= 10000 {
		_ = fmt.Appendf(b[:0], "%04d", v.Year%1000)
	} else {
		strconv.AppendInt(b[:0], int64(v.Week), 10)
	}
	b[4] = '0' + byte(v.Week/10)
	b[5] = '0' + byte(v.Week%10)
	return string(b)
}

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
	v := View{Year: uint16(year), Week: uint16(week)}
	x, ok := r[v]
	if !ok {
		x = rowsPool.Get().(*Rows)
		r[v] = x
	}
	return x
}
