package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/gernest/bsi/internal/rbf/cfg"
	db "github.com/gernest/bsi/internal/storage"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/annotations"
)

var appenderPool = sync.Pool{New: func() any { return new(appender) }}

// API implements prometheus storage api on top of our own timeseries database.
type API struct {
	db db.Store
}

var _ storage.Storage = (*API)(nil)

// Init setup databases.
func (a *API) Init(dataPath string, lo *slog.Logger, opts *cfg.Config) error {
	return a.db.Init(filepath.Join(dataPath, "bsi"), lo, opts)
}

// ChunkQuerier implements storage.ChunkQueryable.
func (a *API) ChunkQuerier(_, _ int64) (storage.ChunkQuerier, error) {
	return nil, errors.New("unsupported operation with BSI storage")
}

// StartTime implements storage.Storage.
func (a *API) StartTime() (int64, error) {
	return a.db.MinTs()
}

// Stats implements the api_v1.TSDBAdminStats interface.
func (a *API) Stats(statsByLabelName string, limit int) (result *tsdb.Stats, err error) {
	return a.db.Stats(statsByLabelName, limit)
}

// Snapshot implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (a *API) Snapshot(dir string, _ bool) error {
	return a.db.Snapshot(dir)
}

// Close implements storage.Storage .
func (a *API) Close() error {
	return a.db.Close()
}

// Delete implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (a *API) Delete(_ context.Context, mint int64, maxt int64, ms ...*labels.Matcher) error {
	return a.db.Delete(mint, maxt, ms...)
}

// Appender implements storage.Appendable
func (a *API) Appender(_ context.Context) storage.Appender {
	app := appenderPool.Get().(*appender)
	app.db = &a.db
	return app
}

type appender struct {
	db  *db.Store
	set db.Rows
}

var _ storage.Appender = (*appender)(nil)

func (w *appender) SetOptions(_ *storage.AppendOptions) {
}

func (w *appender) Rollback() error {
	w.release()
	return nil
}

func (w *appender) release() {
	w.db = nil
	w.set.Reset()
	appenderPool.Put(w)
}

func (w *appender) Commit() error {
	err := w.db.AddRows(&w.set)
	w.release()
	return err
}

func (w *appender) AppendCTZeroSample(_ storage.SeriesRef, _ labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return 0, nil
}

func (w *appender) UpdateMetadata(_ storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	l = l.WithoutEmpty()
	if l.IsEmpty() {
		return 0, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}
	if lbl, dup := l.HasDuplicateLabelNames(); dup {
		return 0, fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidSample)
	}
	t := time.Now()
	meta := prompb.MetricMetadata{
		Type:             prompb.FromMetadataType(m.Type),
		MetricFamilyName: l.Get(model.MetricNameLabel),
		Unit:             m.Unit,
		Help:             m.Help,
	}
	data, _ := meta.Marshal()
	w.set.AppendMetadata(l, t.UnixMilli(), data)
	return 0, nil
}

func (w *appender) AppendHistogram(_ storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if h != nil {
		if err := h.Validate(); err != nil {
			return 0, err
		}
	}

	if fh != nil {
		if err := fh.Validate(); err != nil {
			return 0, err
		}
	}
	l = l.WithoutEmpty()
	if l.IsEmpty() {
		return 0, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}
	if lbl, dup := l.HasDuplicateLabelNames(); dup {
		return 0, fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidSample)
	}

	var hs prompb.Histogram
	if h != nil {
		hs = prompb.FromIntHistogram(t, h)
		hs.CustomValues = h.CustomValues
	}
	if fh != nil {
		hs = prompb.FromFloatHistogram(t, fh)
		hs.CustomValues = fh.CustomValues
	}
	data, _ := hs.Marshal()
	w.set.AppendHistogram(l, t, data, fh != nil)
	return 0, nil
}

func (w *appender) AppendHistogramCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (w *appender) AppendExemplar(_ storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	l = l.WithoutEmpty()
	if l.IsEmpty() {
		return 0, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}
	if lbl, dup := l.HasDuplicateLabelNames(); dup {
		return 0, fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidSample)
	}
	exe := prompb.Exemplar{
		Labels:    prompb.FromLabels(e.Labels, nil),
		Value:     e.Value,
		Timestamp: e.Ts,
	}
	data, err := exe.Marshal()
	if err != nil {
		return 0, err
	}
	w.set.AppendExemplar(l, e.Ts, data)
	return 0, nil
}

func (w *appender) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	l = l.WithoutEmpty()
	if l.IsEmpty() {
		return 0, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}

	if lbl, dup := l.HasDuplicateLabelNames(); dup {
		return 0, fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidSample)
	}
	w.set.AppendFloat(l, t, v)
	return 0, nil
}

var _ storage.Queryable = (*API)(nil)

// Querier implements storage.Queryable.
func (a *API) Querier(mint, maxt int64) (storage.Querier, error) {
	return &querier{lo: mint, hi: maxt, db: &a.db}, nil
}

type querier struct {
	db *db.Store
	lo int64
	hi int64
}

var _ storage.Querier = (*querier)(nil)

func (q *querier) Close() error {
	return nil
}

func (q *querier) LabelValues(_ context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.db.LabelValues(q.lo, q.hi, makeLimit(hints), name, matchers...)
}

func (q *querier) LabelNames(_ context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.db.LabelNames(q.lo, q.hi, makeLimit(hints), matchers...)
}

func makeLimit(hints *storage.LabelHints) int {
	limit := math.MaxInt
	if hints != nil {
		limit = hints.Limit
	}
	return limit
}

func (q *querier) Select(ctx context.Context, sort bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if hints == nil {
		hints = &storage.SelectHints{}
	}
	hints.Start = max(q.lo, hints.Start)
	hints.End = max(q.hi, hints.End)
	return q.db.Select(ctx, sort, hints, matchers...)
}

type exemplarQuery struct {
	db *db.Store
}

var _ storage.ExemplarStorage = (*API)(nil)

// ExemplarQuerier implements storage.ExemplarStorage.
func (a *API) ExemplarQuerier(_ context.Context) (storage.ExemplarQuerier, error) {
	return &exemplarQuery{db: &a.db}, nil
}

// AppendExemplar implements storage.ExemplarStorage.
func (a *API) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	l = l.WithoutEmpty()
	if l.IsEmpty() {
		return 0, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}
	if lbl, dup := l.HasDuplicateLabelNames(); dup {
		return 0, fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidSample)
	}

	app := a.Appender(context.TODO())
	_, err := app.AppendExemplar(ref, l, e)
	if err != nil {
		app.Rollback()
		return 0, err
	}
	return 0, app.Commit()
}

func (s *exemplarQuery) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	return s.db.SelectExemplar(start, end, matchers...)
}
