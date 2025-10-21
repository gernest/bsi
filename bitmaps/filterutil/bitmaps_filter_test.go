package filterutil_test

import (
	"testing"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/bitmaps/filterutil"
	"github.com/stretchr/testify/require"
)

func TestBitmapFilter(t *testing.T) {
	ra := roaring.NewBitmap()
	bitmaps.Mutex(ra, shardwidth.ShardWidth+1, 2, 3)
	bitmaps.Mutex(ra, shardwidth.ShardWidth+2, 100)
	bitmaps.Mutex(ra, shardwidth.ShardWidth+3, 88)
	it, _ := ra.Containers.Iterator(0)
	got := map[uint64][]uint64{}
	f := filterutil.NewBitmapBitmapFilter(
		roaring.NewBitmap(
			shardwidth.ShardWidth+1,
			shardwidth.ShardWidth+3,
		),
		func(col, row uint64) error {
			got[col] = append(got[col], row)
			return nil
		},
	)
	err := roaring.ApplyFilterToIterator(f, it)
	require.NoError(t, err)
	want := map[uint64][]uint64{
		1: {2, 3},
		3: {88},
	}
	require.Equal(t, want, got)
}
