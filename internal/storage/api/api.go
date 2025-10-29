package api

import (
	"context"
	"fmt"
	"time"

	db "github.com/gernest/u128/internal/storage"
	"github.com/gernest/u128/internal/storage/rows"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

var rowsPool rows.Pool

// API implements prometheus storage api on top of our own timeseries database.
type API struct {
	db *db.Store
}

var _ storage.Appendable = (*API)(nil)

// Appender implements storage.Appendable
func (a *API) Appender(_ context.Context) storage.Appender {
	return &appender{
		db:  a.db,
		set: rowsPool.Get(),
	}
}

type appender struct {
	set *rows.Rows
	db  *db.Store
}

var _ storage.Appender = (*appender)(nil)

func (w *appender) SetOptions(_ *storage.AppendOptions) {
}

func (w *appender) Rollback() error {
	if w.set != nil {
		rowsPool.Put(w.set)
		w.set = nil
	}
	return nil
}

func (w *appender) Commit() error {
	err := w.db.AddRows(w.set)
	rowsPool.Put(w.set)
	w.set = nil
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
	}
	if fh != nil {
		hs = prompb.FromFloatHistogram(t, fh)
	}
	data, _ := hs.Marshal()
	w.set.AppendHistogram(l, t, data)
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
