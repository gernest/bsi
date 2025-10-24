package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/single"
	"github.com/gernest/u128/storage/tsid"
	"github.com/google/btree"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	dataPath string
	db       single.Group[viewKey, *dbView, viewOption]
	txt      single.Group[textKey, *txt, txtOptions]

	tree struct {
		mu    sync.RWMutex
		views *btree.BTreeG[viewKey]
	}
}

type viewKey struct {
	year uint16
	week uint16
}
type Key struct {
	year uint16
	week uint16
}

type viewOption struct {
	dataPath string
	write    bool
}

func (v viewKey) String() string {
	return keys.View(int(v.week), int(v.week))
}

// Init initializes store on dataPath.
func (db *Store) Init(dataPath string) error {
	db.dataPath = dataPath
	db.tree.views = btree.NewG(8, func(a, b viewKey) bool {
		return a.year < b.year && a.week < b.week
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
		db.tree.views.ReplaceOrInsert(viewKey{
			year: uint16(y),
			week: uint16(w),
		})
	}

	db.db.Init(func(vk viewKey, vo viewOption) (*dbView, error) {
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
		return &dbView{rbf: db}, nil
	})
	db.txt.Init(openTxt)
	return nil
}

// AddRows index and store rows in the (year, week) view database.
func (db *Store) AddRows(year, week int, rows *Rows) error {

	hi, err := db.allocate(uint64(len(rows.Timestamp)))
	if err != nil {
		return fmt.Errorf("assigning ids to rows %w", err)
	}

	ids := tsidPool.Get()
	defer tsidPool.Put(ids)

	err = db.translateLabels(ids, year, week, rows.Labels)
	if err != nil {
		return fmt.Errorf("assigning tsid to rows %w", err)
	}

	for i := range rows.Histogram {
		if len(rows.Histogram[i]) != 0 {
			err = db.translateHistograms(rows.Value, year, week, rows.Histogram)
			if err != nil {
				return fmt.Errorf("translating histograms %w", err)
			}
			break
		}
	}

	ma := make(rbf.Map)

	start := hi - uint64(len(rows.Timestamp))
	for i := range rows.Timestamp {
		buildIndex(ma, &ids.B[i], start+uint64(i), rows.Timestamp[i], rows.Value[i], len(rows.Histogram) != 0)
	}

	return db.apply(year, week, ma)
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

func (db *Store) translateLabels(b *tsid.B, year, week int, labels [][]byte) error {
	da, done, err := db.txt.Do(textKey{
		column: keys.MetricsLabels,
		year:   uint16(year),
		week:   uint8(week),
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.GetTSID(b, labels)
}

func (db *Store) translateHistograms(b []uint64, year, week int, data [][]byte) error {
	da, done, err := db.txt.Do(textKey{
		column: keys.MetricsHistogram,
		year:   uint16(year),
		week:   uint8(week),
	}, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.TranslateHistogram(b, data)
}

func (db *Store) apply(year, week int, ma rbf.Map) error {
	da, done, err := db.db.Do(viewKey{year: uint16(year), week: uint16(week)}, viewOption{dataPath: db.dataPath})
	if err != nil {
		return fmt.Errorf("opening view database &w", err)
	}
	defer done.Close()
	return da.Apply(ma)
}
