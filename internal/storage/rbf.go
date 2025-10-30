package storage

import (
	"fmt"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/bsi"
	"github.com/gernest/u128/internal/storage/views"
)

func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, depth uint8, op bitmaps.OP, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return bitmaps.Range(cu, op, shard, uint64(depth), predicate, end)
}

func readBSI(tx *rbf.Tx, root uint32, shard uint64, depth uint8, filter *roaring.Bitmap, result *bsi.BSI) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return result.From(cu, shard, uint8(depth), filter)
}

// Perform intersection of all matchers with filter.
func applyBSIFilters(tx *rbf.Tx, records *rbf.Records, shard uint64, filter *roaring.Bitmap, matchers []views.Search) (*roaring.Bitmap, error) {
	for i := range matchers {
		rx, err := readBSIFilter(tx, records, shard, &matchers[i])
		if err != nil {
			return nil, err
		}
		filter = filter.Intersect(rx)
		if !filter.Any() {
			break
		}
	}
	return filter, nil
}

func readBSIFilter(tx *rbf.Tx, records *rbf.Records, shard uint64, match *views.Search) (*roaring.Bitmap, error) {
	root, ok := records.Get(rbf.Key{Column: match.Column, Shard: shard})
	if !ok {
		return nil, fmt.Errorf("missing root record for column %d", match.Column)
	}

	// multiple values in the same search are treated as union.
	all := make([]*roaring.Bitmap, len(match.Values))
	var err error
	for i := range match.Values {
		va := &match.Values[i]
		all[i], err = readBSIRange(tx, root, shard, va.Depth(), match.OP, va.Predicate, va.End)
		if err != nil {
			return nil, err
		}
	}
	return all[0].Union(all[1:]...), nil
}
