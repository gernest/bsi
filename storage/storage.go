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
		var createBuckets bool
		if vo.write {
			_, err := os.Stat(base)
			if os.IsNotExist(err) {
				// we are seeing anew view, update the in memory view state
				db.tree.mu.Lock()
				db.tree.views.ReplaceOrInsert(vk)
				db.tree.mu.Unlock()
				createBuckets = true
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
		if createBuckets {
			// opening write transaction is not cheap.  Create buckets only when we have to.
			err = txt.Update(func(tx *bbolt.Tx) error {
				_, err = tx.CreateBucket(metricsSum)
				if err != nil {
					return fmt.Errorf("creating sum bucket %w", err)
				}
				_, err = tx.CreateBucket(metricsData)
				if err != nil {
					return fmt.Errorf("creating data bucket %w", err)
				}
				_, err = tx.CreateBucket(histogramData)
				if err != nil {
					return fmt.Errorf("creating histogram bucket %w", err)
				}
				_, err = tx.CreateBucket(search)
				if err != nil {
					return fmt.Errorf("creating search bucket %w", err)
				}
				return nil
			})
			if err != nil {
				txt.Close()
				db.Close()
				return nil, err
			}
		}
		return &dbView{rbf: db, meta: txt}, nil
	})
	return nil
}
