package wal

import (
	"errors"
	"iter"
	"log/slog"
	"sort"

	"github.com/prometheus/common/promslog"
)

var errNotFound = errors.New("Unable to find raft entry")

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// wal represents the entire entry log. It consists of one or more
// entryFile objects. This object is not lock protected but it's used by
// DiskStorage, which has a lock protecting the calls to this object.
type wal struct {
	// files is the list of all log files ordered in ascending order by the first
	// index in the file. current is the file currently being written to, and is
	// added to files only after it is full.
	files   []*logFile
	current *logFile
	// nextEntryIdx is the index of the next entry to write to. When this value exceeds
	// maxNumEntries the file will be rotated.
	nextEntryIdx int
	// dir is the directory to use to store files.
	dir string
	lo  *slog.Logger
}

func (l *wal) Init(dir string, lo *slog.Logger) error {
	if lo == nil {
		lo = promslog.NewNopLogger()

	}
	l.dir = dir
	l.lo = lo

	files, err := getLogFiles(dir)
	if err != nil {
		return err
	}
	out := files[:0]
	var nextFid int64
	for _, ef := range files {
		if nextFid < ef.id {
			nextFid = ef.id
		}
		if ef.firstIndex() == 0 {
			if err := ef.delete(); err != nil {
				return err
			}
		} else {
			out = append(out, ef)
		}
	}
	l.files = out
	if sz := len(l.files); sz > 0 {
		l.current = l.files[sz-1]
		l.nextEntryIdx = l.current.firstEmptySlot()

		l.files = l.files[:sz-1]
		return nil
	}

	// No files found. Create a new file.
	nextFid++
	ef, err := openFile(dir, nextFid)
	l.current = ef
	return err
}

func (l *wal) Record(data []byte) error {
	idx := l.LastIndex() + 1
	prev := l.nextEntryIdx - 1
	var offset int
	if prev >= 0 {
		// There was a previous entry. Retrieve the offset and the size data from that entry to
		// calculate the next offset.
		e := l.current.getEntry(prev)
		offset = int(e.DataOffset())
		offset += sliceSize(l.current.data, offset)
	} else {
		// At the start of the file so use entryFileOffset.
		offset = logFileOffset
	}
	if l.nextEntryIdx >= maxNumEntries || offset+4+len(data) > 1<<30 {
		if err := l.rotate(idx, offset); err != nil {
			return err
		}
		l.nextEntryIdx, offset = 0, logFileOffset
	}
	destBuf, next, err := l.current.allocate(len(data), offset)
	if err != nil {
		return err
	}
	assert(copy(destBuf, data) == len(data))
	buf := l.current.getEntry(l.nextEntryIdx)
	marshalEntry(buf, 0, idx, uint64(offset), 0)
	offset = next
	l.nextEntryIdx++
	return nil
}

func (l *wal) AllEntries(lo, hi, maxSize uint64) iter.Seq2[uint64, []byte] {
	return func(yield func(uint64, []byte) bool) {
		fileIdx, offset := l.slotGe(lo)
		var size uint64
		var count int

		if offset < 0 {
			// Start from the beginning of the entry file.
			offset = 0
		}

		currFile := l.getEntryFile(fileIdx)
		for {
			// If offset is greater than maxNumEntries, then we need to move to the next file.
			if offset >= maxNumEntries {
				if fileIdx == -1 {
					// We're already at the latest file. Return the entries we have.
					return
				}

				// Move to the next file.
				fileIdx++
				if fileIdx >= len(l.files) {
					// We're beyond the list of files in l.files. Move to the latest file.
					fileIdx = -1
				}
				currFile = l.getEntryFile(fileIdx)
				assert(currFile != nil)

				// Reset the offset to start reading the next file from the beginning.
				offset = 0
			}

			idx, data := currFile.GetEntry(offset)
			// fmt.Printf("Got raft entry: %v\n", re.Index)
			if idx >= hi {
				return
			}

			if idx == 0 {
				// This entry and all the following ones in this file are empty.
				// Setting the offset to maxNumEntries will trigger a move to the next
				// file in the next iteration.
				offset = maxNumEntries
				continue
			}

			size += uint64(len(data))
			if count > 0 && size > maxSize {
				break
			}
			if !yield(idx, data) {
				return
			}
			offset++
			count++
		}
	}
}

// firstIndex returns the first index available in the entry log.
func (l *wal) firstIndex() uint64 {
	if l == nil {
		return 0
	}
	var fi uint64
	if len(l.files) == 0 {
		fi = l.current.getEntry(0).Index()
	} else {
		fi = l.files[0].getEntry(0).Index()
	}

	// If fi is zero return one because RAFT expects the first index to always
	// be greater than zero.
	if fi == 0 {
		return 1
	}
	return fi
}

// LastIndex returns the last index in the log.
func (l *wal) LastIndex() uint64 {
	if l.nextEntryIdx-1 >= 0 {
		e := l.current.getEntry(l.nextEntryIdx - 1)
		return e.Index()
	}
	for i := len(l.files) - 1; i >= 0; i-- {
		ef := l.files[i]
		e := ef.lastEntry()
		if e.Index() > 0 {
			return e.Index()
		}
	}
	return 0
}

// getEntryFile returns right logFile corresponding to the fidx. A value of -1
// is meant to represent the current file, which is not yet stored in l.files.
func (l *wal) getEntryFile(fidx int) *logFile {
	if fidx == -1 {
		return l.current
	}
	if fidx >= len(l.files) {
		return nil
	}
	return l.files[fidx]
}

// slotGe returns the file index and the slot within that file containing the
// entry with an index greater than equals to the provided raftIndex. A
// value of -1 for the file index means that the entry is in the current file.
// A value of -1 for slot means that the raftIndex is lower than whatever is
// present in the WAL, thus it is not present in the WAL.
func (l *wal) slotGe(raftIndex uint64) (int, int) {
	// Look for the offset in the current file.
	if offset := l.current.slotGe(raftIndex); offset >= 0 {
		return -1, offset
	}

	// No previous files, therefore we can only go back to the start of the current file.
	if len(l.files) == 0 {
		return -1, -1
	}

	fileIdx := sort.Search(len(l.files), func(i int) bool {
		return l.files[i].firstIndex() >= raftIndex
	})
	// fileIdx points to the first log file, whose firstIndex is >= raftIndex.
	// If the firstIndex == raftIndex, then return.
	if fileIdx < len(l.files) && l.files[fileIdx].firstIndex() == raftIndex {
		return fileIdx, 0
	}
	// Otherwise, go back one file to the file which has firstIndex < raftIndex.
	if fileIdx > 0 {
		fileIdx--
	}

	offset := l.files[fileIdx].slotGe(raftIndex)
	return fileIdx, offset
}

// seekEntry returns the entry with the given raftIndex if it exists. If the
// raftIndex is lower than all the entries in the log, raft.ErrCompacted is
// returned. If the raftIndex is higher than all the entries in the log,
// raft.ErrUnavailable is returned. If no match is found, errNotFound is
// returned. Finally, if an entry matches the raftIndex, it is returned.
func (l *wal) seekEntry(raftIndex uint64) (entry, error) {
	if raftIndex == 0 {
		return emptyEntry, nil
	}

	fidx, off := l.slotGe(raftIndex)
	if off == -1 {
		// The entry is not in the log because it was already processed and compacted.
		return emptyEntry, ErrCompacted
	} else if off >= maxNumEntries {
		// The log has not advanced past the given raftIndex.
		return emptyEntry, ErrUnavailable
	}

	ef := l.getEntryFile(fidx)
	ent := ef.getEntry(off)
	if ent.Index() == 0 {
		// The log has not advanced past the given raftIndex.
		return emptyEntry, ErrUnavailable
	}
	if ent.Index() != raftIndex {
		return emptyEntry, errNotFound
	}
	return ent, nil
}

// Moves the current logFile into l.files and creates a new logFile.
func (l *wal) rotate(_ uint64, offset int) error {
	// Select the name for the new file based on the names of the existing files.
	nextFid := l.current.id
	assert(nextFid > 0)
	for _, ef := range l.files {
		if ef.id > nextFid {
			nextFid = ef.id
		}
	}
	nextFid++

	err := l.current.truncate(int64(offset))
	if err != nil {
		return err
	}

	ef, err := openFile(l.dir, nextFid)
	if err != nil {
		return err
	}

	// Move the existing current file to the end of the list of files and
	// update the current file to the file that was just created.
	l.files = append(l.files, l.current)
	l.current = ef
	return nil
}

// deleteBefore deletes all the files before the logFile containing the given raftIndex.
func (l *wal) deleteBefore(index uint64) {
	fIdx, off := l.slotGe(index)

	if off < 0 || fIdx >= len(l.files) {
		return
	}

	var before []*logFile
	if fIdx == -1 { // current file
		before = l.files
		l.files = l.files[:0]
	} else {
		before = l.files[:fIdx]
		l.files = l.files[fIdx:]
	}

	for _, ef := range before {
		if err := ef.delete(); err != nil {
			l.lo.Error("deleting file", "path", ef.file.Name(), "err", err)
		}
	}
}
