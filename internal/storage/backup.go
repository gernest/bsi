package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// Snapshot takes snapshot of the whole database. Snapshots occupies the same storage
// space as current databases. We simply copy pages over to the snapshot directory.
//
// Results in two files
// 1. data : which is in RBF format
// 2. txt : which is a bolt database containing symbol translations.
//
// Make sure snapshot dir is deleted after use to free up space.
func (db *Store) Snapshot(base string) error {
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return err
	}
	err = db.snapshot(base)
	if err != nil {
		// delete failed snapshots dir
		os.RemoveAll(base)
		return err
	}
	return nil
}

func (db *Store) snapshot(base string) error {
	err := db.txt.View(func(tx *bbolt.Tx) error {
		// copy local txt database state
		path := filepath.Join(base, "txt")
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("creating txt snapshot file %w", err)
		}
		_, err = tx.WriteTo(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("writing txt file %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	path := filepath.Join(base, "data")
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating txt snapshot file %w", err)
	}
	err = db.db.Backup(f)
	f.Close()
	if err != nil {
		return fmt.Errorf("writing rbf database %w", err)
	}
	return nil
}
