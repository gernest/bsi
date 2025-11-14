package row

import (
	"math"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

// Limit scopes number of shards to process.
type Limit struct {
	StartShard uint64
	EndShard   uint64
}

// Unlimited returns a Limit that will select all shards.
func Unlimited() Limit {
	return Limit{
		EndShard: math.MaxUint64,
	}
}

// Limit Returns the shard range that r covers.
func (r *Row) Limit() Limit {
	if len(r.Segments) == 0 {
		return Limit{}
	}
	return Limit{
		StartShard: r.Segments[0].Shard(),
		EndShard:   r.Segments[len(r.Segments)-1].Shard() + 1,
	}
}

func Range(tx *rbf.Tx, records *rbf.Records, limit Limit, col uint64, op bitmaps.OP, predicate, end int64) (*Row, error) {
	it := records.Iterator()
	result := roaring.NewBitmap()
	for it.Seek(rbf.Key{Column: col, Shard: limit.StartShard}); !it.Done(); {
		name, page, ok := it.Next()
		if !ok || name.Column != col || name.Shard >= limit.EndShard {
			break
		}
		err := bsi(tx, page, name.Shard, op, predicate, end, func(ra *roaring.Bitmap) {
			result = result.Union(ra)
		})
		if err != nil {
			return nil, err
		}
	}
	return NewRowFromBitmap(result), nil
}

func bsi(tx *rbf.Tx, root uint32, shard uint64, op bitmaps.OP, predicate, end int64, cb func(ra *roaring.Bitmap)) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	mx, err := cu.Max()
	if err != nil {
		return err
	}
	depth := mx / shardwidth.ShardWidth
	ra, err := bitmaps.Range(cu, op, shard, depth, predicate, end)
	if err != nil {
		return err
	}
	cb(ra)
	return nil
}

// Mutex search all shards for column for mutex encoded rows.
func Mutex(tx *rbf.Tx, records *rbf.Records, limit Limit, col uint64, rows *roaring.Bitmap) (*roaring.Bitmap, error) {
	it := records.Iterator()
	result := roaring.NewBitmap()
	for it.Seek(rbf.Key{Column: col, Shard: limit.StartShard}); !it.Done(); {
		name, page, ok := it.Next()
		if !ok || name.Column != col || name.Shard >= limit.EndShard {
			break
		}
		err := mutex(tx, page, name.Shard, rows, func(ra *roaring.Bitmap) {
			result = result.Union(ra)
		})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func mutex(tx *rbf.Tx, root uint32, shard uint64, all *roaring.Bitmap, cb func(ra *roaring.Bitmap)) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	for row := range all.RangeAll() {
		ra, err := bitmaps.Row(cu, shard, row)
		if err != nil {
			return err
		}
		cb(ra)
	}
	return nil
}
