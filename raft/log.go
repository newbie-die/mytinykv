// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
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

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	last, _ := l.storage.LastIndex()
	return last
}

// Term return the term of the entry in the given index
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
