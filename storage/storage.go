package storage

import (
	"bytes"
	"iter"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/storage/tsid"
)

type DB interface {
	AllocateID(size uint64) (hi uint64, err error)
	UpdateSequence(uint64) error
	GetTSID(out *tsid.ID, labels []byte) error
	Apply(data iter.Seq2[string, *roaring.Bitmap]) error
}

type Store struct {
	db DB
}

// Add adds rows to storage.
func (db *Store) Add(rows []Row) error {
	// 1. Allocate sequences covering all rows.
	hi, err := db.db.AllocateID(uint64(len(rows)))
	if err != nil {
		return err
	}
	// lo is the first id.
	lo := hi - uint64(len(rows))

	id := tsid.Get()
	defer id.Release()

	ma := NewMap()

	for i := range rows {
		if i != 0 && bytes.Equal(rows[i].Labels, rows[i-1].Labels) {
			// fast path: consecutive rows belongs to the same metrics group.
			// reuse the same tsid.
			ma.Index(id, lo+uint64(i), rows[i].Timestamp, rows[i].Value)
			continue
		}
		err = db.db.GetTSID(id, rows[i].Labels)
		if err != nil {
			return err
		}
		ma.Index(id, lo+uint64(i), rows[i].Timestamp, rows[i].Value)
	}
	return db.db.Apply(ma.Range())
}
