package paths

import (
	"fmt"
	"path/filepath"

	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/magic"
	"github.com/oklog/ulid/v2"
)

var bytesPool buffer.Pool

// Rows returns path to serialized rows. We use ulid to make sure files are unique
// during processing.
func Rows(base string, year, week int, size int) string {
	b := bytesPool.Get()
	defer bytesPool.Put(b)
	view := string(keys.View(b, year, week))
	id := ulid.Make()
	b.B = fmt.Appendf(b.B[:0], "%v_%d.mz", id, size)
	return filepath.Join(
		base, view, magic.String(b.B),
	)
}
