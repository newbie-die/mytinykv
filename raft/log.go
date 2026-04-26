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
    firstIndex, _ := storage.FirstIndex()
    lastIndex, _ := storage.LastIndex()

    entries, _ := storage.Entries(firstIndex, lastIndex+1)

    return &RaftLog{
        storage:   storage,
        committed: firstIndex - 1,
        applied:   firstIndex - 1,
        stabled:   lastIndex,
        entries:   entries,
    }
}

func (l *RaftLog) allEntries() []pb.Entry {
    return l.entries
}

func (l *RaftLog) unstableEntries() []pb.Entry {
    if len(l.entries) == 0 {
        return nil
    }
    return l.entries[l.stabled-l.entries[0].Index+1:]
}

func (l *RaftLog) nextEnts() []pb.Entry {
    if l.applied >= l.committed {
        return nil
    }
    return l.entries[l.applied-l.entries[0].Index+1 : l.committed-l.entries[0].Index+1]
}

func (l *RaftLog) LastIndex() uint64 {
    if len(l.entries) > 0 {
        return l.entries[len(l.entries)-1].Index
    }
    i, _ := l.storage.LastIndex()
    return i
}

func (l *RaftLog) Term(i uint64) (uint64, error) {
    if len(l.entries) > 0 {
        first := l.entries[0].Index
        if i >= first && i <= l.LastIndex() {
            return l.entries[i-first].Term, nil
        }
    }
    return l.storage.Term(i)
}
// Append appends new entries to the log.
func (l *RaftLog) Append(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}