package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"
)

// Snapshot takes snapshot of the whole database. We have a very similar design like LSM
// based systems, once full shards are immutable so we use hard links for faster snapshots.
//
// Partial shards are  manually copied.
func (db *Store) Snapshot(dir string) error {
	name := ulid.Make()
	base := filepath.Join(dir, name.String())
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return err
	}
	err = db.snapshot(base, name)
	if err != nil {
		// delete failed snapshots dir
		os.RemoveAll(base)
		return err
	}
	return nil

}

func (db *Store) snapshot(base string, name ulid.ULID) error {
	full := make([]shardYM, 0, 1<<10)
	partial := make([]shardYM, 0, 1<<10)
	err := db.txt.View(func(tx *bbolt.Tx) error {
		// copy local txt database state
		path := filepath.Join(base, filepath.Base(db.txt.Path()))
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("creating txt snapshot file %w", err)
		}
		_, err = tx.WriteTo(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("writing txt file %w", err)
		}

		// collect all shards for backup
		return walkPartitions(tx, yyyyMM{}, yyyyMM{year: math.MaxInt}, func(key yyyyMM, m meta) error {
			if m.full {
				full = append(full, shardYM{
					ym:    key,
					shard: m.shard,
				})
				return nil
			}
			partial = append(partial, shardYM{
				ym:    key,
				shard: m.shard,
			})
			return nil
		})
	})
	if err != nil {
		return err
	}
	if len(full) == 0 && len(partial) == 0 {
		return fmt.Errorf("no shards found for snapshot")
	}

	// copy all partial shards
	for i := range partial {
		err := db.copyShard(base, partial[i])
		if err != nil {
			return fmt.Errorf("copying  shard %s %w", partial[i], err)
		}
	}

	// we hard link the rest of the shards
	for i := range full {
		err := db.linkShard(base, full[i])
		if err != nil {
			return fmt.Errorf("linking shard %s %w", full[i], err)
		}
	}
	all := append(full, partial...)
	sort.Slice(all, func(i, j int) bool {
		return all[i].ym.Compare(&all[j].ym) < 0 &&
			all[i].shard < all[j].shard
	})
	// we do not rely on state of filesystem to track snapshots. We use txt database
	// to ensure only fully successful snapshot  operations show up.
	return db.txt.Update(func(tx *bbolt.Tx) error {
		snapB := tx.Bucket(snapshots)
		return snapB.Put(name[:], magic.ReinterpretSlice[byte](all))
	})
}

func (db *Store) copyShard(base string, manual shardYM) error {
	return db.partition(manual.ym, manual.shard, false, func(tx *rbf.Tx) error {
		path := partitionPath(base, manual)
		err := os.Mkdir(path, 0755)
		if err != nil {
			return err
		}
		data := filepath.Join(path, "data")
		f, err := os.Create(data)
		if err != nil {
			return fmt.Errorf("creating data file %w", err)
		}
		err = tx.WriteTo(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("writing data file %w", err)
		}
		return nil
	})
}

func (db *Store) linkShard(base string, ym shardYM) error {
	path := partitionPath(base, ym)
	err := os.Mkdir(path, 0755)
	if err != nil {
		return err
	}
	data := filepath.Join(path, "data")
	src := filepath.Join(partitionPath(db.dataPath, ym), "data")
	return os.Link(src, data)
}
