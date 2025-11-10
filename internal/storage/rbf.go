package storage

import (
	"fmt"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/raw"
	"github.com/gernest/roaring"
)

func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, depth uint8, op bitmaps.OP, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return bitmaps.Range(cu, op, shard, uint64(depth), predicate, end)
}

func readRaw(tx *rbf.Tx, root uint32, shard uint64, depth uint8, filter *roaring.Bitmap, result *raw.BSI) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return result.From(cu, shard, uint8(depth), filter)
}

func applyBSIFiltersAny(tx *rbf.Tx, records *rbf.Records, shard uint64, filter *roaring.Bitmap, matchers [][]match) (*roaring.Bitmap, error) {
	all := make([]*roaring.Bitmap, 0, len(matchers))
	for i := range matchers {
		m := matchers[i]
		rx, err := readBSIFilter(tx, records, shard, &m[0])
		if err != nil {
			return nil, err
		}
		if !rx.Any() {
			continue
		}
		rx, err = applyBSIFilters(tx, records, shard, rx, matchers[i][1:])
		if err != nil {
			return nil, err
		}
		if !rx.Any() {
			continue
		}
		all = append(all, rx)
	}
	if len(all) == 0 {
		return roaring.NewBitmap(), nil
	}
	if len(all) == 1 {
		return filter.Intersect(all[0]), nil
	}
	return filter.Intersect(all[0].Union(all[1:]...)), nil
}

func applyBSIFilters(tx *rbf.Tx, records *rbf.Records, shard uint64, filter *roaring.Bitmap, matchers []match) (*roaring.Bitmap, error) {
	for i := range matchers {
		m := &matchers[i]
		// handle special cases
		if len(m.rows) == 0 || (len(m.rows) == 1 && m.rows[0] == 0) {
			switch m.op {
			case bitmaps.EQ:
				// resets all filters: we will never find suitable columns again.
				return roaring.NewBitmap(), nil
			case bitmaps.NEQ:
				// same as matching existing bitmap
				continue
			default:
				panic(fmt.Sprintf("unexpected operation %s for col=%s ", m.op, m.column))
			}
		}
		rx, err := readBSIFilter(tx, records, shard, m)
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

func readBSIFilter(tx *rbf.Tx, records *rbf.Records, shard uint64, match *match) (*roaring.Bitmap, error) {
	root, ok := records.Get(match.Column())
	if !ok {
		return nil, fmt.Errorf("missing root record for column %s", match.column)
	}
	if len(match.rows) == 1 {
		return readBSIRange(tx, root, shard, match.depth, match.op, int64(match.rows[0]), 0)
	}

	// multiple values in the same search are treated as union.
	all := make([]*roaring.Bitmap, len(match.rows))
	var err error
	for i := range match.rows {
		all[i], err = readBSIRange(tx, root, shard, match.depth, match.op, int64(match.rows[i]), 0)
		if err != nil {
			return nil, err
		}
	}
	return all[0].Union(all[1:]...), nil
}
