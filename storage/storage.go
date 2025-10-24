package storage

import (
	"fmt"
	"os"

	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/rows"
	"github.com/gernest/u128/storage/single"
	"github.com/gernest/u128/storage/tsid"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	dataPath string
	rbf      single.Group[string, *rbf.DB, struct{}]
	txt      single.Group[textKey, *txt, txtOptions]
}

// Init initializes store on dataPath.
func (db *Store) Init(dataPath string) error {
	err := os.MkdirAll(dataPath, 0755)
	if err != nil {
		return fmt.Errorf("setup data path %w", err)
	}
	db.rbf.Init(openRBF)
	db.txt.Init(openTxt)
	return nil
}

// AddRows index and store rows in the (year, week) view database.
func (db *Store) AddRows(view rows.View, rows *rows.Rows) error {
	hi, err := db.allocate(uint64(len(rows.Timestamp)))
	if err != nil {
		return fmt.Errorf("assigning ids to rows %w", err)
	}

	ids := tsidPool.Get()
	defer tsidPool.Put(ids)

	err = db.translateLabels(ids, view, rows.Labels)
	if err != nil {
		return fmt.Errorf("assigning tsid to rows %w", err)
	}

	seen := map[keys.Kind]bool{
		keys.Float: true,
	}

	for i := range rows.Kind {
		if seen[rows.Kind[i]] {
			continue
		}
		switch rows.Kind[i] {
		case keys.Histogram:
			err = db.translateHistograms(rows.Value, view, rows.Histogram)
			if err != nil {
				return fmt.Errorf("translating histograms %w", err)
			}
		case keys.Exemplar:
			err = db.translateExemplars(rows.Value, view, rows.Exemplar)
			if err != nil {
				return fmt.Errorf("translating exemplars %w", err)
			}
		case keys.Metadata:
			err = db.translateMetadata(rows.Value, view, rows.Metadata)
			if err != nil {
				return fmt.Errorf("translating metadata %w", err)
			}
		}
		seen[rows.Kind[i]] = true
	}

	ma := make(rbf.Map)

	start := hi - uint64(len(rows.Timestamp))

	for i, row := range rows.Range() {
		buildIndex(ma, view, &ids.B[i], start+uint64(i), row.Timestamp, row.Value, row.Kind)
	}

	return db.apply(ma)
}

func (db *Store) allocate(size uint64) (uint64, error) {
	da, done, err := db.txt.Do(textKey{
		column: keys.Root,
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return 0, err
	}
	defer done.Close()

	return da.AllocateID(size)
}

func (db *Store) translateLabels(b *tsid.B, view rows.View, labels [][]byte) error {
	da, done, err := db.txt.Do(textKey{
		column: keys.MetricsLabels,
		view:   view,
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.GetTSID(b, labels)
}

func (db *Store) translateHistograms(b []uint64, view rows.View, data [][]byte) error {
	da, done, err := db.txt.Do(textKey{
		column: keys.MetricsHistogram,
		view:   view,
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.TranslateHistogram(b, data)
}

func (db *Store) translateExemplars(b []uint64, view rows.View, data [][]byte) error {
	da, done, err := db.txt.Do(textKey{
		column: keys.MetricsExemplar,
		view:   view,
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.TranslateExemplar(b, data)
}

func (db *Store) translateMetadata(b []uint64, view rows.View, data [][]byte) error {
	da, done, err := db.txt.Do(textKey{
		column: keys.MetricsMetadata,
		view:   view,
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.TranslateMetadata(b, data)
}

func (db *Store) apply(ma rbf.Map) error {
	da, done, err := db.rbf.Do(db.dataPath, struct{}{})
	if err != nil {
		return fmt.Errorf("opening view database %w", err)
	}
	defer done.Close()
	tx, err := da.Begin(true)
	if err != nil {
		return fmt.Errorf("creating write transaction %w", err)
	}
	defer tx.Rollback()
	for k, v := range ma {
		v.Optimize()
		_, err = tx.AddRoaring(k, v)
		if err != nil {
			return fmt.Errorf("writing bitmap %w", err)
		}
	}
	return tx.Commit()
}
