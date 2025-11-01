package storage

import (
	"strings"

	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/scrape"
	"go.etcd.io/bbolt"
)

var _ scrape.MetricMetadataStore = (*Store)(nil)

// ListMetadata implements scrape.MetricMetadataStore.
func (db *Store) ListMetadata() (result []scrape.MetricMetadata) {
	db.txt.View(func(tx *bbolt.Tx) error {
		metaB := tx.Bucket(metaData)
		var m prompb.MetricMetadata

		return metaB.ForEach(func(_, v []byte) error {
			m.Unmarshal(v)
			result = append(result, scrape.MetricMetadata{
				MetricFamily: m.MetricFamilyName,
				Type:         model.MetricType(strings.ToLower(m.Type.String())),
				Help:         m.Help,
				Unit:         m.Unit,
			})
			return nil
		})
	})
	return
}

// GetMetadata implements scrape.MetricMetadataStore.
func (db *Store) GetMetadata(mfName string) (meta scrape.MetricMetadata, found bool) {
	db.txt.View(func(tx *bbolt.Tx) error {
		metaB := tx.Bucket(metaData)
		var m prompb.MetricMetadata
		if v := metaB.Get(magic.Slice(mfName)); v != nil {
			m.Unmarshal(v)
			meta = scrape.MetricMetadata{
				MetricFamily: m.MetricFamilyName,
				Type:         model.MetricType(strings.ToLower(m.Type.String())),
				Help:         m.Help,
				Unit:         m.Unit,
			}
			found = true
		}

		return nil
	})
	return
}

// SizeMetadata implements scrape.MetricMetadataStore.
func (db *Store) SizeMetadata() (n int) {
	db.txt.View(func(tx *bbolt.Tx) error {
		metaB := tx.Bucket(metaData)
		stats := metaB.Stats()
		n += stats.BranchAlloc
		n += stats.LeafAlloc
		return nil
	})
	return
}

// LengthMetadata implements scrape.MetricMetadataStore.
func (db *Store) LengthMetadata() (n int) {
	db.txt.View(func(tx *bbolt.Tx) error {
		metaB := tx.Bucket(metaData)
		stats := metaB.Stats()
		n += stats.KeyN
		return nil
	})
	return
}
