/*
 * Copyright 2023 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wal

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/gernest/bsi/internal/rbf/syswrap"
	"github.com/pkg/errors"
)

const (
	// maxNumEntries is maximum number of entries before rotating the file.
	maxNumEntries = 30000
	// logFileOffset is offset in the log file where data is stored.
	logFileOffset = 1 << 20 // 1MB
	// encOffset is offset in the log file where keyID (first 8 bytes)
	// and baseIV (remaining 8 bytes) are stored.
	encOffset = logFileOffset - 16 // 1MB - 16B
	// logFileSize is the initial size of the log file.
	logFileSize = 256 << 20 // 256MB
	// entrySize is the size in bytes of a single entry.
	entrySize = 32
	// logSuffix is the suffix for log files.
	logSuffix = ".wal"
)

var (
	emptyEntry = entry(make([]byte, entrySize))
)

type entry []byte

func (e entry) Term() uint64       { return binary.BigEndian.Uint64(e) }
func (e entry) Index() uint64      { return binary.BigEndian.Uint64(e[8:]) }
func (e entry) DataOffset() uint64 { return binary.BigEndian.Uint64(e[16:]) }
func (e entry) Type() uint64       { return binary.BigEndian.Uint64(e[24:]) }

func marshalEntry(b []byte, term, index, do, typ uint64) {
	AssertTrue(len(b) == entrySize)
	binary.BigEndian.PutUint64(b, term)
	binary.BigEndian.PutUint64(b[8:], index)
	binary.BigEndian.PutUint64(b[16:], do)
	binary.BigEndian.PutUint64(b[24:], typ)
}

type logFile struct {
	file *os.File
	data []byte
	id   int64
}

func logPath(dir string, id int64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", id, logSuffix))
}

func openFile(dir string, id int64) (*logFile, error) {
	path := logPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	data, err := syswrap.Mmap(int(f.Fd()), logFileSize, true)
	if err != nil {
		return nil, err
	}
	return &logFile{file: f, data: data, id: id}, nil
}

// getEntry gets the entry at the slot idx.
func (lf *logFile) getEntry(idx int) entry {
	if lf == nil {
		return emptyEntry
	}
	AssertTrue(idx < maxNumEntries)
	offset := idx * entrySize
	return entry(lf.data[offset : offset+entrySize])
}

// firstIndex returns the first index in the file.
func (lf *logFile) firstIndex() uint64 {
	return lf.getEntry(0).Index()
}

// firstEmptySlot returns the index of the first empty slot in the file.
func (lf *logFile) firstEmptySlot() int {
	return sort.Search(maxNumEntries, func(i int) bool {
		e := lf.getEntry(i)
		return e.Index() == 0
	})
}

// lastEntry returns the last valid entry in the file.
func (lf *logFile) lastEntry() entry {
	// This would return the first pos, where e.Index() == 0.
	pos := lf.firstEmptySlot()
	if pos > 0 {
		pos--
	}
	return lf.getEntry(pos)
}

// slotGe would return -1 if raftIndex < firstIndex in this file.
// Would return maxNumEntries if raftIndex > lastIndex in this file.
// If raftIndex is found, or the entryFile has empty slots, the offset would be between
// [0, maxNumEntries).
func (lf *logFile) slotGe(raftIndex uint64) int {
	fi := lf.firstIndex()
	// If first index is zero or the first index is less than raftIndex, this
	// raftindex should be in a previous file.
	if fi == 0 || raftIndex < fi {
		return -1
	}

	// Look at the entry at slot diff. If the log has entries for all indices between
	// fi and raftIndex without any gaps, the entry should be there. This is an
	// optimization to avoid having to perform the search below.
	if diff := int(raftIndex - fi); diff < maxNumEntries && diff >= 0 {
		e := lf.getEntry(diff)
		if e.Index() == raftIndex {
			return diff
		}
	}

	// Find the first entry which has in index >= to raftIndex.
	return sort.Search(maxNumEntries, func(i int) bool {
		e := lf.getEntry(i)
		if e.Index() == 0 {
			// We reached too far to the right and found an empty slot.
			return true
		}
		return e.Index() >= raftIndex
	})
}

func (lf *logFile) delete() error {
	err := syswrap.Munmap(lf.data)
	if err != nil {
		return err
	}
	err = lf.file.Close()
	if err != nil {
		return err
	}
	return os.Remove(lf.file.Name())
}

func AssertTrue(b bool) {

	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
