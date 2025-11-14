package storage

import (
	"fmt"
	"os"
	"path/filepath"

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
		return fmt.Errorf("writing rbf database $w", err)
	}
	return nil
}
