package storage

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

// Backup writes a backup file in the directory. We use UUIDv7 for the backup file
// which is in the zip format.
func (db *Store) Backup(dir string) error {
	path := filepath.Join(dir, uuid.Must(uuid.NewV7()).String())
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := zip.NewWriter(file)
	txt, err := w.Create("txt")
	if err != nil {
		return err
	}
	err = db.txt.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(txt)
		return err
	})
	if err != nil {
		return fmt.Errorf("writing txt database %w", err)
	}

	data, err := w.Create("data")
	if err != nil {
		return err
	}
	err = db.rbf.Backup(data)
	if err != nil {
		return fmt.Errorf("writing rbf database %w", err)
	}
	return w.Close()
}
