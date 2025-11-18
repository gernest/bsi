package resets

import "github.com/prometheus/prometheus/model/histogram"

func ExpandFloatSpansAndBuckets(a, b []histogram.Span, aBuckets []float64, bBuckets []float64) (ok bool) {
	ai := newBucketIterator(a)
	bi := newBucketIterator(b)

	aIdx, aOK := ai.Next()
	bIdx, bOK := bi.Next()

	// Bucket count. Initialize the absolute count and index into the
	// positive/negative counts or deltas array. The bucket count is
	// used to detect counter reset as well as unused buckets in a.
	var (
		aCount    float64
		bCount    float64
		aCountIdx int
		bCountIdx int
	)
	if aOK {
		aCount = aBuckets[aCountIdx]
	}
	if bOK {
		bCount = bBuckets[bCountIdx]
	}

	advanceA := func() {
		aIdx, aOK = ai.Next()
		aCountIdx++
		if aOK {
			aCount = aBuckets[aCountIdx]
		}
	}

	advanceB := func() {

		bIdx, bOK = bi.Next()

		bCountIdx++
		if bOK {
			bCount = bBuckets[bCountIdx]
		}
	}

loop:
	for {
		switch {
		case aOK && bOK:
			switch {
			case aIdx == bIdx: // Both have an identical bucket index.
				// Bucket count. Check bucket for reset from a to b.
				if aCount > bCount {
					return false
				}
				advanceA()
				advanceB()

				continue
			case aIdx < bIdx: // b misses a bucket index that is in a.
				// This is ok if the count in a is 0, in which case we make a note to
				// fill in the bucket in b and advance a.
				if aCount == 0 {
					advanceA()
					continue
				}
				// Otherwise we are missing a bucket that was in use in a, which is a reset.
				return false
			case aIdx > bIdx: // a misses a value that is in b. Forward b and recompare.
				advanceB()
			}
		case aOK && !bOK: // b misses a value that is in a.
			// This is ok if the count in a is 0, in which case we make a note to
			// fill in the bucket in b and advance a.
			if aCount == 0 {
				advanceA()
				continue
			}
			// Otherwise we are missing a bucket that was in use in a, which is a reset.
			return false
		case !aOK && bOK: // a misses a value that is in b. Forward b and recompare.
			advanceB()
		default: // Both iterators ran out. We're done.
			break loop
		}
	}

	return true
}

type bucketIterator struct {
	spans  []histogram.Span
	span   int // Span position of last yielded bucket.
	bucket int // Bucket position within span of last yielded bucket.
	idx    int // Bucket index (globally across all spans) of last yielded bucket.
}

func newBucketIterator(spans []histogram.Span) *bucketIterator {
	b := bucketIterator{
		spans:  spans,
		span:   0,
		bucket: -1,
		idx:    -1,
	}
	if len(spans) > 0 {
		b.idx += int(spans[0].Offset)
	}
	return &b
}

func (b *bucketIterator) Next() (int, bool) {
	// We're already out of bounds.
	if b.span >= len(b.spans) {
		return 0, false
	}
	if b.bucket < int(b.spans[b.span].Length)-1 { // Try to move within same span.
		b.bucket++
		b.idx++
		return b.idx, true
	}

	for b.span < len(b.spans)-1 { // Try to move from one span to the next.
		b.span++
		b.idx += int(b.spans[b.span].Offset + 1)
		b.bucket = 0
		if b.spans[b.span].Length == 0 {
			b.idx--
			continue
		}
		return b.idx, true
	}

	// We're out of options.
	return 0, false
}
