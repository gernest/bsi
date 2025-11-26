package api

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
)

func TestPromql(t *testing.T) {
	parser.EnableExperimentalFunctions = true
	parser.EnableExtendedRangeSelectors = true
	parser.ExperimentalDurationExpr = true
	promqltest.RunBuiltinTestsWithStorage(t, newTestEngine(t), newStorage)
}

func newTestEngine(t *testing.T) *promql.Engine {
	return promqltest.NewTestEngine(t, false, 0, promqltest.DefaultMaxSamplesPerQuery)
}

func TestDeleteSimple(t *testing.T) {
	const numSamples int64 = 10

	cases := []struct {
		Intervals tombstones.Intervals
		remaint   []int64
	}{
		{
			Intervals: tombstones.Intervals{{Mint: 0, Maxt: 3}},
			remaint:   []int64{4, 5, 6, 7, 8, 9},
		},
		{
			Intervals: tombstones.Intervals{{Mint: 1, Maxt: 3}},
			remaint:   []int64{0, 4, 5, 6, 7, 8, 9},
		},
		{
			Intervals: tombstones.Intervals{{Mint: 1, Maxt: 3}, {Mint: 4, Maxt: 7}},
			remaint:   []int64{0, 8, 9},
		},
		{
			Intervals: tombstones.Intervals{{Mint: 1, Maxt: 3}, {Mint: 4, Maxt: 700}},
			remaint:   []int64{0},
		},
		// Unlike prometheus, we return an empty series set wen no samples are matched.
		// This case will always fail because labels translation is delayed until we have
		// observed valid samples.
		//
		// { // This case is to ensure that labels and symbols are deleted.
		// 	Intervals: tombstones.Intervals{{Mint: 0, Maxt: 9}},
		// 	remaint:   []int64{},
		// },
	}

	for _, c := range cases {

		t.Run("", func(t *testing.T) {
			db := openTestDB(t)
			defer func() {
				require.NoError(t, db.Close())
			}()

			ctx := context.Background()
			app := db.Appender(ctx)

			smpls := make([]float64, numSamples)
			for i := range numSamples {
				smpls[i] = rand.Float64()
				app.Append(0, labels.FromStrings("a", "b"), i, smpls[i])
			}

			require.NoError(t, app.Commit())

			// TODO(gouthamve): Reset the tombstones somehow.
			// Delete the ranges.
			for _, r := range c.Intervals {
				require.NoError(t, db.Delete(ctx, r.Mint, r.Maxt, labels.MustNewMatcher(labels.MatchEqual, "a", "b")))
			}

			// Compare the result.
			q, err := db.Querier(0, numSamples)
			require.NoError(t, err)

			res := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "a", "b"))

			expSamples := make([]chunks.Sample, 0, len(c.remaint))
			for _, ts := range c.remaint {
				expSamples = append(expSamples, sample{ts, smpls[ts], nil, nil})
			}

			expss := newMockSeriesSet([]storage.Series{
				storage.NewListSeries(labels.FromStrings("a", "b"), expSamples),
			})

			for {
				eok, rok := expss.Next(), res.Next()
				require.Equal(t, eok, rok)

				if !eok {
					require.Empty(t, res.Warnings())
					break
				}
				sexp := expss.At()
				sres := res.At()

				require.Equal(t, sexp.Labels(), sres.Labels())

				smplExp, errExp := storage.ExpandSamples(sexp.Iterator(nil), nil)
				smplRes, errRes := storage.ExpandSamples(sres.Iterator(nil), nil)

				require.Equal(t, errExp, errRes)
				require.Equal(t, smplExp, smplRes)
			}
		})
	}
}

// TODO(bwplotka): Replace those mocks with remote.concreteSeriesSet.
type mockSeriesSet struct {
	next   func() bool
	series func() storage.Series
	ws     func() annotations.Annotations
	err    func() error
}

func (m *mockSeriesSet) Next() bool                        { return m.next() }
func (m *mockSeriesSet) At() storage.Series                { return m.series() }
func (m *mockSeriesSet) Err() error                        { return m.err() }
func (m *mockSeriesSet) Warnings() annotations.Annotations { return m.ws() }

func newMockSeriesSet(list []storage.Series) *mockSeriesSet {
	i := -1
	return &mockSeriesSet{
		next: func() bool {
			i++
			return i < len(list)
		},
		series: func() storage.Series {
			return list[i]
		},
		err: func() error { return nil },
		ws:  func() annotations.Annotations { return nil },
	}
}

type sample struct {
	t  int64
	f  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func newSample(t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) chunks.Sample {
	return sample{t, v, h, fh}
}

func (s sample) T() int64                      { return s.t }
func (s sample) F() float64                    { return s.f }
func (s sample) H() *histogram.Histogram       { return s.h }
func (s sample) FH() *histogram.FloatHistogram { return s.fh }

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s sample) Copy() chunks.Sample {
	c := sample{t: s.t, f: s.f}
	if s.h != nil {
		c.h = s.h.Copy()
	}
	if s.fh != nil {
		c.fh = s.fh.Copy()
	}
	return c
}

func newStorage(t testutil.T) storage.Storage {
	return openTestDB(t.(testing.TB))
}

func openTestDB(t testing.TB) *API {
	tb := t.(testing.TB)
	tb.Helper()
	a := new(API)
	require.NoError(tb, a.Init(tb.TempDir(), nil, nil))
	return a
}
