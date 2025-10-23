package compress

import (
	"sync"

	"github.com/minio/minlz"
)

type Pool struct {
	read  sync.Pool
	write sync.Pool
}

func (p *Pool) GetReader() *minlz.Reader {
	v := p.read.Get()
	if v != nil {
		return v.(*minlz.Reader)
	}
	return minlz.NewReader(nil)
}

func (p *Pool) PutReader(r *minlz.Reader) {
	r.Reset(nil)
	p.read.Put(r)
}

func (p *Pool) GetWriter() *minlz.Writer {
	v := p.write.Get()
	if v != nil {
		return v.(*minlz.Writer)
	}
	return minlz.NewWriter(nil)
}

func (p *Pool) PutWriter(w *minlz.Writer) {
	w.Reset(nil)
	p.write.Put(w)
}
