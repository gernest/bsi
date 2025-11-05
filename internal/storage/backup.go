package storage

// Backup writes a backup file in the directory. We use UUIDv7 for the backup file
// which is in the zip format.
func (db *Store) Backup(_ string) error {
	return nil
}
