package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/tsid"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var (
	sys         = []byte("sys")
	metrics     = []byte("metrics")
	metricsSum  = []byte("sum")
	metricsData = []byte("data")
	search      = []byte("index")
)

type db struct {
	rbf  *rbf.DB
	meta *bbolt.DB
}

var _ DB = (*db)(nil)

// AllocateID implements DB using bolt bucket sequence api.
func (db *db) AllocateID(size uint64) (hi uint64, err error) {
	err = db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sys)
		hi = b.Sequence() + size
		return b.SetSequence(hi)
	})
	return
}

// GetTSID implements DB.
//
// This uses a single bolt database as the underlying storage. We rely heavily on
// nested buckets to map key hierarchy.
// Here is a simple outlook for generating tsid for metrics labels {foo=xxx, bar=yyy, baz=zzz}
// we use a = b to denote key value paris.
//
//	                                                     ┌──────┐
//	                                                     │ sys  │
//	                                                     │      │
//	                                                     └──────┘
//	                                                        │
//	                                                        ▼
//	                                                    ┌───────┐
//	                                                    │ view  │
//	                                                    │       │
//	                                                    └───────┘
//	                                                        │
//	                                                        ▼
//	                                                  ┌──────────┐
//	                                                  │ metrics  │
//	                                                  │          │
//	                                                  └──────────┘
//	                                                     │  │  │
//	                     ┌───────────────────────────────┘  │  └───────────────────────────┐
//	                     │                                  │                              │
//	                     ▼                                  ▼                              ▼
//	              ┌────────────┐                        ┌───────┐                    ┌──────────┐
//	              │ checksums  │                        │ data  │                    │  index   │
//	              │            │                        │       │                    │          │
//	              └────────────┘                        └───────┘                    └──────────┘
//	                     │                                  │                           │  │  │
//	                     │                                  │                  ┌────────┘  │  └───────┐
//	                     │                                  │                  │           │          │
//	                     ▼                                  ▼                  ▼           ▼          ▼
//	┌──────────────────────────────────────────┌────────────────────────────┌──────┐   ┌──────┐    ┌──────┐
//	│u128 = {id =1 , view=[foo, bar, baz], rows│1 = {foo=xxx, bar=yyy baz=zz│}foo  │   │ bar  │    │ baz  │
//	│                                          │           │                │  │   │   │      │    │      │
//	└──────────────────────────────────────────└────────────────────────────└──────┘   └──────┘    └──────┘
//	                                                                           │           │          │
//	                                                                           ▼           ▼          ▼
//	                                                                      ┌────────┐  ┌────────┐  ┌────────┐
//	                                                                      │xxx = 1 │  │yyy = 2 │  │zzz = 3 │
//	                                                                      │        │  │        │  │        │
//	                                                                      └────────┘  └────────┘  └────────┘
//
// We scope things under view bucket to simplify application of retention policies.
// We can simply delete the view bucket to remove all old data collected in it.
func (db *db) GetTSID(out *tsid.ID, view, labels []byte) error {
	return db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sys)
		vb, err := b.CreateBucketIfNotExists(view)
		if err != nil {
			return err
		}
		metricsB, err := vb.CreateBucketIfNotExists(metrics)
		if err != nil {
			return err
		}

		metricsSumB, err := metricsB.CreateBucketIfNotExists(metricsSum)
		if err != nil {
			return err
		}
		// This is the heart and building block of everything. We rely on speed and
		// cryptographic property to ensure series are correctly enumerated in each
		// view.
		sum := checksum.Hash(labels)
		if got := metricsSumB.Get(sum[:]); got != nil {
			// fast path: we have already processed labels in this view. We don't need
			// to do any more work.
			out.Decode(got)
			return nil
		}

		out.Reset()
		out.ID, err = metricsB.NextSequence()

		searchIndexB, err := metricsB.CreateBucketIfNotExists(search)
		if err != nil {
			return err
		}

		// Building index
		for name, value := range buffer.RangeLabels(labels) {
			labelNameB, err := searchIndexB.CreateBucketIfNotExists(name)
			if err != nil {
				return fmt.Errorf("creating label bucket %w", err)
			}
			if got := labelNameB.Get(value); got != nil {
				// fast path: we already assigned unique id for label value
				out.Views = append(out.Views, name)
				out.Rows = append(out.Rows, binary.BigEndian.Uint64(got))
			} else {
				// slow path: assign unique id to value
				nxt, err := labelNameB.NextSequence()
				if err != nil {
					return fmt.Errorf("assigning sequence id %w", err)
				}
				err = labelNameB.Put(value, binary.BigEndian.AppendUint64(nil, nxt))
				if err != nil {
					return fmt.Errorf("storing sequence id %w", err)
				}
				out.Views = append(out.Views, name)
				out.Rows = append(out.Rows, nxt)
			}

		}

		// 1. encode tsid
		buf := bytesPool.Get()
		defer bytesPool.Put(buf)
		out.Encode(buf)

		// 2. store checksum => tsid in checksums bucket
		err = metricsSumB.Put(sum[:], bytes.Clone(buf.B))
		if err != nil {
			return fmt.Errorf("storing metrics checksum %w", err)
		}

		// 3. store labels_sequence_id => labels_data in data bucket
		dB, err := metricsB.CreateBucketIfNotExists(metricsData)
		if err != nil {
			return fmt.Errorf("creating metrics data bucket %w", err)
		}
		err = dB.Put(binary.BigEndian.AppendUint64(nil, out.ID), labels)
		if err != nil {
			return fmt.Errorf("storing metrics data %w", err)
		}
		return nil
	})
}

// Apply implements DB.
func (db *db) Apply(data iter.Seq2[string, *roaring.Bitmap]) error {
	tx, err := db.rbf.Begin(true)
	if err != nil {
		return fmt.Errorf("creating write transaction %w", err)
	}
	defer tx.Rollback()

	for k, v := range data {
		_, err := tx.AddRoaring(k, v)
		if err != nil {
			return fmt.Errorf("storing bitmap %v %w", k, err)
		}
	}
	return nil
}

// Search finds matching rows across multiple views and fields. Computing views is left
// to the caller, the only condition is they must be sorted in lexicographic order.
func (db *db) Search(startView, endView []byte, startTs, endTs int64, selectors []*labels.Matcher) error {

	// It is not ideal to keep long running read transaction to conserve memory.
	// We instead open short lived ones to find relevant bits.
	views := make([][]byte, 0, 4<<10)
	err := db.findMatchingViews(startView, endView, func(view []byte) error {
		views = append(views, bytes.Clone(view))
		return nil
	})
	if err != nil {
		return fmt.Errorf("searching for views %w", err)
	}
	if len(views) == 0 {
		return nil
	}

	// We use timestamp data field to find data that is matching timestamp conditions.
	// This resolves all shards and views we need to process if further filtering is needed.
	rsl := newRowsSelector(startTs, endTs)

	// process views concurrently. We generate ridiculous amount of views, since
	// (view, shard) are independent we can process chunks safely.
	var wg sync.WaitGroup

	// distribute work among available cpu.
	for chunks := range slices.Chunk(views, runtime.GOMAXPROCS(0)) {
		wg.Add(1)
		go rsl.Do(&wg, chunks)
	}
	wg.Done()

	if err := rsl.Error(); err != nil {
		return err
	}

	if !rsl.Any() {
		// fast path: nothing matches the time range
		return nil
	}

	// translation is view scoped:  we process views chunks concurrently.
	for start, end := range chunkRange(len(rsl.views), runtime.GOMAXPROCS(0)) {
		wg.Add(1)
		go rsl.Read(&wg, start, end, selectors)
	}
	return nil
}

// Search for all observed views within the range.
func (db *db) findMatchingViews(start, end []byte, cb func(view []byte) error) error {
	return db.meta.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sys)
		cu := b.Cursor()

		// Buckets have nil value but non nil keys. Views are stored in buckets under sys.
		// We iterate within the provided range.
		// The upper bound is exclusive,
		for k, _ := cu.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, _ = cu.Next() {
			err := cb(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type rowsSelector struct {
	err       atomic.Value
	db        *rbf.DB
	views     [][]byte
	data      [][]shardMatch
	predicate int64
	end       int64
	mu        sync.Mutex
}

func newRowsSelector(predicate, end int64) *rowsSelector {
	return &rowsSelector{
		predicate: predicate,
		end:       end,
	}
}

type shardMatch struct {
	ra    *roaring.Bitmap
	shard uint64
}

func (t *rowsSelector) Any() bool {
	return len(t.views) != 0
}

func (t *rowsSelector) Error() error {
	err := t.err.Load()
	if err != nil {
		return err.(error)
	}
	return nil
}

func (t *rowsSelector) Read(wg *sync.WaitGroup, start, end int, selectors []*labels.Matcher) {
	defer wg.Done()
}

func (t *rowsSelector) Do(wg *sync.WaitGroup, views [][]byte) {
	defer wg.Done()
	// If any process failed, we invalidate the whole pipeline.
	if err := t.err.Load(); err != nil {
		return
	}
	tx, err := t.db.Begin(false)
	if err != nil {
		t.err.Store(fmt.Errorf("opening read transaction %w", err))
		return
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		t.err.Store(fmt.Errorf("reading root records%w", err))
		return
	}
	it := records.Iterator()

	b := bytesPool.Get()
	defer bytesPool.Put(b)

	for _, view := range views {
		prefix := keys.KeyViewPrefix(b, keys.DataIndex, keys.MetricsTimestamp, view)
		prefixText := magic.String(prefix)
		it.Seek(prefixText)
		base := make([]shardMatch, 0, 16)
		for !it.Done() {
			name, pgno, ok := it.Next()
			if !ok {
				break
			}
			if !strings.HasPrefix(name, prefixText) {
				break
			}
			shard := keys.GetShard(name)
			ra, err := readBSIRange(tx, pgno, shard, t.predicate, t.end)
			if err != nil {
				t.err.Store(fmt.Errorf("range search on predicates s%w", err))
				return
			}
			if ra.Any() {
				// found a match: ra is memory mapped to the current transaction.
				/// we need to clone to avoid segfault.
				base = append(base, shardMatch{
					shard: shard,
					ra:    ra.Clone(),
				})
			}
		}
		if len(base) > 0 {
			t.mu.Lock()
			t.views = append(t.views, view)
			t.data = append(t.data, base)
			t.mu.Unlock()
		}
	}
}

// readBSIRange performs a range search  in predicate...end bounds with upper bound being exclusive.
func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return nil, fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth
	return bitmaps.Range(cu, bitmaps.BETWEEN, shard, depth, predicate, end)
}

// chunkRange pro of slices.Chunk modified to generate indices only.
func chunkRange(size, n int) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for i := 0; i < size; i += n {
			end := min(n, size-1)
			if !yield(i, i+end) {
				return
			}
		}
	}
}
