package storage

import (
	"bytes"
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
	"go.etcd.io/bbolt"
)

// Store implements timeseries database.
type Store struct {
	dataPath string
	db       single.Group[viewKey, *dbView, viewOption]

	tree struct {
		mu    sync.RWMutex
		views *btree.BTreeG[viewKey]
	}
}

type viewKey struct {
	year uint16
	week uint8
}

type viewOption struct {
	dataPath string
	write    bool
}

func (v viewKey) String() string {
	b := bytesPool.Get()
	defer bytesPool.Put(b)
	return string(keys.View(b, int(v.year), int(v.week)))
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
		db.tree.views.ReplaceOrInsert(viewKey{
			year: uint16(y),
			week: uint8(w),
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
		textPath := filepath.Join(base, "txt")
		txt, err := bbolt.Open(textPath, 0600, nil)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("opening text database %w", err)
		}
		return &dbView{rbf: db, meta: txt}, nil
	})
	return nil
}

// Add adds rows to storage.
func (db *Store) Add(year int, week int, rows *Rows) error {
	da, done, err := db.db.Do(viewKey{year: uint16(year), week: uint8(week)}, viewOption{dataPath: db.dataPath, write: true})
	if err != nil {
		return fmt.Errorf("opening view database %w", err)
	}
	defer done.Close()

	// 1. Allocate sequences covering all rows.
	hi, err := da.AllocateID(uint64(len(rows.Timestamp)))
	if err != nil {
		return err
	}
	// lo is the first id.
	lo := hi - uint64(len(rows.Timestamp))

	id := tsid.Get()
	defer id.Release()

	vb := bytesPool.Get()

	defer bytesPool.Put(vb)

	ma := NewMap()

	for i := range rows.Timestamp {

		if i == 0 || !bytes.Equal(rows.Labels[i], rows.Labels[i-1]) {
			// The check above ensures we only compute tsid if we care processing
			// rows with different series.
			// Ingesting same series with many samples will avoid this expensive call
			// and just use already computed tsid.
			err = da.GetTSID(id, rows.Labels[i])
			if err != nil {
				return err
			}
		}

		ma.Index(id, lo+uint64(i),
			rows.Timestamp[i],
			rows.Value[i],
			len(rows.Histogram[i]) != 0)
	}
	return da.Apply(ma.Range())
}
