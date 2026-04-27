package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manages the log entries.
type RaftLog struct {
	storage         Storage
	committed       uint64
	applied         uint64
	stabled         uint64
	entries         []pb.Entry
	pendingSnapshot *pb.Snapshot
}

// newLog returns a new RaftLog using the provided storage.
func newLog(storage Storage) *RaftLog {
	first, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	ents, err := storage.Entries(first, last+1)
	if err != nil {
		panic(err)
	}
	rl := &RaftLog{
		storage: storage,
		applied: first - 1,
		stabled: last,
		entries: ents,
	}
	return rl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) stableTo(index uint64) {
	if index < l.stabled {
		return
	}
	l.stabled = index
}

func (l *RaftLog) appliedTo(index uint64) {
	if index < l.applied {
		return
	}
	if index > l.committed {
		panic("applied index is out of range")
	}
	l.applied = index
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
func (l *RaftLog) allEntries() []pb.Entry {
	return append([]pb.Entry{}, l.entries...)
}

func (l *RaftLog) firstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	first, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return first
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	first := l.firstIndex()
	if l.stabled < first-1 {
		return l.entries
	}
	return l.entries[l.stabled-first+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed > l.applied {
		first := l.firstIndex()
		return l.entries[l.applied+1-first : l.committed+1-first]
	}
	return nil
}

func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	last, _ := l.storage.LastIndex()
	return last
}

func (l *RaftLog) Term(i uint64) (uint64, error) {
	first := l.firstIndex()
	if i < first {
		return l.storage.Term(i)
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if len(l.entries) == 0 {
		return 0, ErrUnavailable
	}
	return l.entries[i-first].Term, nil
}
