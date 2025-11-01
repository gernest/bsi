// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package rbf

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/gernest/bsi/internal/checksum"
	"github.com/gernest/bsi/internal/rbf/cfg"
	"github.com/gernest/roaring"
	"github.com/stretchr/testify/require"
)

func rbfName(index, field, view string, shard uint64) Key {
	k := fmt.Sprintf("%v:%v:%v;%v", index, field, view, shard)
	return Key{
		Column: checksum.Hash([]byte(k)),
	}
}

/*
// rbtree uses 15% memory and needs half the ingest time
// for our 10K view ingest.
//
// previous master with slice copying instead of rbtree:

=== RUN   TestIngest_lots_of_views
ingest_test.go:141 2020-11-13T03:14:09.778839Z m0.TotalAlloc = 728408
ingest_test.go:144 2020-11-13T03:14:37.492104Z m1.TotalAlloc = 41,816,617,216
--- PASS: TestIngest_lots_of_views (27.71s)

// lots_views with rbtree

=== RUN   TestIngest_lots_of_views
ingest_test.go:141 2020-11-13T03:11:01.540076Z m0.TotalAlloc = 726072
ingest_test.go:144 2020-11-13T03:11:15.003591Z m1.TotalAlloc = 35,510,273,184
--- PASS: TestIngest_lots_of_views (13.46s)
*/
func TestIngest_lots_of_views(t *testing.T) {

	// realistic
	//nCt := 10000

	// fast CI
	nCt := 10

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	//vv("m0.TotalAlloc = %v", m0.TotalAlloc)
	defer func() {
		runtime.ReadMemStats(&m1)
		//vv("m1.TotalAlloc = %v", m1.TotalAlloc)
	}()

	path := t.TempDir()
	defer os.Remove(path)

	cfg := cfg.NewDefaultConfig()
	db := NewDB(path, cfg)
	require.NoError(t, db.Open())

	// setup profiling
	if false {
		profile, err := os.Create("./rbf_ingest_put_ct.cpu")
		require.NoError(t, err)
		_ = pprof.StartCPUProfile(profile)
		defer func() {
			pprof.StopCPUProfile()
			profile.Close()
		}()
	}

	// put containers
	tx, err := db.Begin(true)
	require.NoError(t, err)

	index := "i"
	field := "f"
	var view string // set below in the loop.

	// put a raw-bitmap container to many views.
	bits := []uint16{}
	//for i := 0; i < 1<<16; i++ {
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			bits = append(bits, uint16(i))
		}
	}
	ct := roaring.NewContainerArray(bits)

	ckey := uint64(0)
	shard := ckey / ShardWidth

	for i := 0; i < nCt; i++ {
		view = fmt.Sprintf("view_%v", i)
		name := rbfName(index, field, view, shard)
		err = tx.PutContainer(name, ckey, ct)
		require.NoError(t, err)
		ct2, err := tx.Container(name, ckey)
		require.NoError(t, err)
		require.NoError(t, ct2.BitwiseCompare(ct))

		// write .dot of it...
		if false { //ckey == nCt-1 {
			c, err := tx.cursor(name)
			require.NoError(t, err)
			defer c.Close()
			c.Dump("one.bitmap.dot.dump")
		}
	}

	require.NoError(t, tx.Commit())

	sz, err := DiskUse(path, "")
	require.NoError(t, err)
	_ = sz
	//vv("sz in bytes= %v", sz)
	db.Close()
}

func DiskUse(root string, requiredSuffix string) (tot int, err error) {

	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if info.IsDir() {
			// skip the size of directories themselves, only summing files.
		} else {
			sz := info.Size()
			if requiredSuffix == "" || strings.HasSuffix(path, requiredSuffix) {
				tot += int(sz)
			}
		}
		return nil
	})
	return
}
