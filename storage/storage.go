package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/rows"
	"github.com/gernest/u128/storage/single"
	"github.com/gernest/u128/storage/tsid"
	"github.com/google/btree"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	dataPath string
	rbf      single.Group[rows.View, *rbfDB, viewOption]
	txt      single.Group[textKey, *txt, txtOptions]

	tree struct {
		mu    sync.RWMutex
		views *btree.BTreeG[rows.View]
	}
}

type viewOption struct {
	dataPath string
	write    bool
}

// Init initializes store on dataPath.
func (db *Store) Init(dataPath string) error {
	err := os.MkdirAll(dataPath, 0755)
	if err != nil {
		return fmt.Errorf("setup data path %w", err)
	}
	db.dataPath = dataPath
	db.tree.views = btree.NewG(8, func(a, b rows.View) bool {
		return a.Year < b.Year && a.Week < b.Week
	})

	// load all existing views in memory.
	views, err := os.ReadDir(dataPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	for i := range views {
		if !views[i].IsDir() {
			continue
		}
		view := views[i].Name()
		// first 4 bytes are year
		y, err := strconv.Atoi(view[:4])
		if err != nil {
			return fmt.Errorf("parsing year for view %s %w", view, err)
		}
		// last 2 bytes for week
		w, err := strconv.Atoi(view[4:])
		if err != nil {
			return fmt.Errorf("parsing week for view %s %w", view, err)
		}
		if y == 0 && w == 0 {
			// root view.
			continue
		}
		db.tree.views.ReplaceOrInsert(rows.View{
			Year: uint16(y),
			Week: uint8(w),
		})
	}

	db.rbf.Init(func(vk rows.View, vo viewOption) (*rbfDB, error) {
		base := filepath.Join(vo.dataPath, vk.String())
		if vo.write {
			_, err := os.Stat(base)
			if os.IsNotExist(err) {
				// we are seeing anew view, update the in memory view state
				db.tree.mu.Lock()
				db.tree.views.ReplaceOrInsert(vk)
				db.tree.mu.Unlock()
			}
		}
		err := os.MkdirAll(base, 0755)
		if err != nil {
			return nil, fmt.Errorf("creating view directory %w", err)
		}
		db := rbf.NewDB(base, nil)
		err = db.Open()
		if err != nil {
			return nil, fmt.Errorf("opening rbf database %w", err)
		}
		return &rbfDB{rbf: db}, nil
	})
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

	return db.apply(view, ma)
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

func (db *Store) apply(view rows.View, ma rbf.Map) error {
	da, done, err := db.rbf.Do(view, viewOption{dataPath: db.dataPath})
	if err != nil {
		return fmt.Errorf("opening view database %w", err)
	}
	defer done.Close()
	return da.Apply(ma)
}
