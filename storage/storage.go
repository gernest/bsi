package storage

import (
	"fmt"
	"os"

	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/rows"
	"github.com/gernest/u128/storage/single"
	"github.com/gernest/u128/storage/tsid"
	"go.etcd.io/bbolt"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	dataPath string
	rbf      single.Group[string, *rbf.DB, struct{}]
	txt      single.Group[rbf.View, *bbolt.DB, txtOptions]
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

// AddRows index and store rows.
func (db *Store) AddRows(view rows.View, rows *rows.Rows) error {
	da, done, err := db.txt.Do(view, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()
	hi, err := allocateRecordsID(da, uint64(len(rows.Timestamp)))
	if err != nil {
		return fmt.Errorf("assigning ids to rows %w", err)
	}

	ids := tsidPool.Get()
	defer tsidPool.Put(ids)

	err = assignTSID(da, ids, rows.Labels)
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
			err = assignU64ToHistogams(da, rows.Value, rows.Histogram)
			if err != nil {
				return fmt.Errorf("translating histograms %w", err)
			}
		case keys.Exemplar:
			err = assignU64ToExemplars(da, rows.Value, rows.Exemplar)
			if err != nil {
				return fmt.Errorf("translating exemplars %w", err)
			}
		case keys.Metadata:
			err = assignU64ToMetadata(da, rows.Value, rows.Metadata)
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
