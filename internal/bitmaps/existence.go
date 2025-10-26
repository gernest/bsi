package bitmaps

import "github.com/gernest/roaring"

const rowSize = 1 << 4

// ExtractExistsFromMutex builds existence bitmap from  mutex encoded bitmap.
func ExtractExistsFromMutex(ra *roaring.Bitmap) *roaring.Bitmap {
	o := roaring.NewBitmap()

	it, _ := ra.Containers.Iterator(0)
	for it.Next() {
		k, synthC := it.Value()
		o.Containers.Update(k%rowSize, func(oldC *roaring.Container, _ bool) (newC *roaring.Container, write bool) {
			existN := oldC.N()
			if existN == roaring.MaxContainerVal+1 {
				return oldC, false
			}
			if existN == 0 {
				newerC := synthC.Clone()
				return newerC, true
			}
			newC = oldC.UnionInPlace(synthC)

			newC.Repair()
			if newC.N() != existN {
				return newC, true
			}
			return oldC, false
		})
	}
	return o

}
