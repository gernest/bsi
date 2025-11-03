package remote

import (
	"math"

	"github.com/gernest/bsi/internal/checksum"
	"github.com/gernest/bsi/internal/storage/buffer"
	"github.com/gernest/bsi/internal/storage/keys"
	"github.com/gernest/bsi/internal/storage/rows"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/prometheus/prometheus/prompb"
)

func Write(rows *rows.Rows, ids *tsid.B) (w *prompb.WriteRequest) {
	series := map[uint64]*prompb.TimeSeries{}
	w = &prompb.WriteRequest{}
	for i := range ids.B {
		id := ids.B[i][0].Value
		sx, ok := series[id]
		if !ok {
			w.Timeseries = append(w.Timeseries, prompb.TimeSeries{})
			sx = &w.Timeseries[len(w.Timeseries)-1]
			for name, value := range buffer.RangeLabels(rows.Labels[i]) {
				sx.Labels = append(sx.Labels, prompb.Label{
					Name:  string(name),
					Value: string(value),
				})
			}
			w.Metadata = append(w.Metadata, prompb.MetricMetadata{})
			w.Metadata[len(w.Metadata)-1].Unmarshal(rows.Metadata[i])
			series[id] = sx
		}
		switch rows.Kind[i] {
		case keys.Float:
			sx.Samples = append(sx.Samples, prompb.Sample{
				Value:     math.Float64frombits(rows.Value[i]),
				Timestamp: rows.Timestamp[i],
			})
		case keys.Exemplar:
			sx.Exemplars = append(sx.Exemplars, prompb.Exemplar{})
			sx.Exemplars[len(sx.Exemplars)-1].Unmarshal(rows.Exemplar[i])
		case keys.Histogram, keys.FloatHistogram:
			sx.Histograms = append(sx.Histograms, prompb.Histogram{})
			sx.Histograms[len(sx.Histograms)-1].Unmarshal(rows.Histogram[i])
		}
	}
	return
}

func WriteSum(rows *rows.Rows) (w *prompb.WriteRequest) {
	series := map[uint64]*prompb.TimeSeries{}
	w = &prompb.WriteRequest{}
	for i := range rows.Labels {
		id := checksum.Hash(rows.Labels[i])
		sx, ok := series[id]
		if !ok {
			w.Timeseries = append(w.Timeseries, prompb.TimeSeries{})
			sx = &w.Timeseries[len(w.Timeseries)-1]
			for name, value := range buffer.RangeLabels(rows.Labels[i]) {
				sx.Labels = append(sx.Labels, prompb.Label{
					Name:  string(name),
					Value: string(value),
				})
			}
			w.Metadata = append(w.Metadata, prompb.MetricMetadata{})
			w.Metadata[len(w.Metadata)-1].Unmarshal(rows.Metadata[i])
			series[id] = sx
		}
		switch rows.Kind[i] {
		case keys.Float:
			sx.Samples = append(sx.Samples, prompb.Sample{
				Value:     math.Float64frombits(rows.Value[i]),
				Timestamp: rows.Timestamp[i],
			})
		case keys.Exemplar:
			sx.Exemplars = append(sx.Exemplars, prompb.Exemplar{})
			sx.Exemplars[len(sx.Exemplars)-1].Unmarshal(rows.Exemplar[i])
		case keys.Histogram, keys.FloatHistogram:
			sx.Histograms = append(sx.Histograms, prompb.Histogram{})
			sx.Histograms[len(sx.Histograms)-1].Unmarshal(rows.Histogram[i])
		}
	}
	return
}
