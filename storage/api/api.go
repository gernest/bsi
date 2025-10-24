package api

import (
	"context"
	"time"

	db "github.com/gernest/u128/storage"
	"github.com/gernest/u128/storage/rows"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

// API implements prometheus storage api on top of our own timeseries database.
type API struct {
	db *db.Store
}

var _ storage.Appendable = (*API)(nil)

// Appender implements storage.Appendable
func (a *API) Appender(_ context.Context) storage.Appender {
	return &appender{
		db:  a.db,
		set: make(rows.Set),
	}
}

type appender struct {
	set rows.Set
	db  *db.Store
}

var _ storage.Appender = (*appender)(nil)

func (w *appender) SetOptions(_ *storage.AppendOptions) {
}

func (w *appender) Rollback() error {
	w.set.Release()
	return nil
}

func (w *appender) Commit() error {
	defer w.set.Release()

	for k, v := range w.set {
		err := w.db.AddRows(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *appender) AppendCTZeroSample(_ storage.SeriesRef, _ labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return 0, nil
}

func (w *appender) UpdateMetadata(_ storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	t := time.Now()
	meta := prompb.MetricMetadata{
		Type:             prompb.FromMetadataType(m.Type),
		MetricFamilyName: l.Get(model.MetricNameLabel),
		Unit:             m.Unit,
		Help:             m.Help,
	}
	data, _ := meta.Marshal()
	w.set.Get(t.ISOWeek()).AppendMetadata(l, t.UnixMilli(), data)
	return 0, nil
}

func (w *appender) AppendHistogram(_ storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	var hs prompb.Histogram
	if h != nil {
		hs = prompb.FromIntHistogram(t, h)
	}
	if fh != nil {
		hs = prompb.FromFloatHistogram(t, fh)
	}
	data, _ := hs.Marshal()
	w.set.GetUnixMilli(t).AppendHistogram(l, t, data)
	return 0, nil
}

func (w *appender) AppendHistogramCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (w *appender) AppendExemplar(_ storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	exe := prompb.Exemplar{
		Labels:    prompb.FromLabels(e.Labels, nil),
		Value:     e.Value,
		Timestamp: e.Ts,
	}
	data, err := exe.Marshal()
	if err != nil {
		return 0, err
	}
	w.set.GetUnixMilli(e.Ts).AppendExemplar(l, e.Ts, data)
	return 0, nil
}

func (w *appender) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	w.set.GetUnixMilli(t).AppendFloat(l, t, v)
	return 0, nil
}
