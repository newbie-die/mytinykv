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
	if storage == nil {
		panic("storage must not be nil")
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	log := &RaftLog{
		storage:   storage,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   make([]pb.Entry, 0),
	}

	if lastIndex >= firstIndex {
		ents, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
		log.entries = append(log.entries, ents...)
	}

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	if len(l.entries) == 0 {
		return
	}

	offset := l.entries[0].Index
	if firstIndex > offset {
		l.entries = l.entries[firstIndex-offset:]
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	ents := make([]pb.Entry, len(l.entries))
	copy(ents, l.entries)
	return ents
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	start := uint64(0)
	if l.stabled+1 > firstIndex {
		start = l.stabled + 1 - firstIndex
	}
	if start >= uint64(len(l.entries)) {
		return nil
	}

	ents := make([]pb.Entry, len(l.entries)-int(start))
	copy(ents, l.entries[start:])
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed <= l.applied {
		return nil
	}

	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	start := l.applied + 1 - firstIndex
	end := l.committed + 1 - firstIndex
	if start >= uint64(len(l.entries)) {
		return nil
	}
	if end > uint64(len(l.entries)) {
		end = uint64(len(l.entries))
	}

	ents = make([]pb.Entry, end-start)
	copy(ents, l.entries[start:end])
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		lastIndex, err := l.storage.LastIndex()
		if err != nil {
			panic(err)
		}
		return lastIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	if i < firstIndex {
		return l.storage.Term(i)
	}

	if len(l.entries) == 0 {
		return 0, ErrUnavailable
	}

	offset := i - firstIndex
	if offset >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}

	return l.entries[offset].Term, nil
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	if index == 0 && term == 0 {
		return true
	}
	entryTerm, err := l.Term(index)
	if err != nil {
		return false
	}
	return entryTerm == term
}

func (l *RaftLog) isUpToDate(index, term uint64) bool {
	lastIndex := l.LastIndex()
	lastTerm, err := l.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	if term != lastTerm {
		return term > lastTerm
	}
	return index >= lastIndex
}

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ent := range ents {
		if !l.matchTerm(ent.Index, ent.Term) {
			return ent.Index
		}
	}
	return 0
}

func (l *RaftLog) appendEntries(ents ...pb.Entry) {
	if len(ents) == 0 {
		return
	}

	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	after := ents[0].Index
	if after < firstIndex {
		ents = ents[firstIndex-after:]
	}

	if len(l.entries) > 0 {
		if after <= l.entries[len(l.entries)-1].Index {
			if after <= l.entries[0].Index {
				l.entries = l.entries[:0]
			} else {
				offset := after - l.entries[0].Index
				l.entries = l.entries[:offset]
			}
		}
	}

	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) maybeAppend(index, term, committed uint64, ents ...pb.Entry) (uint64, bool) {
	if !l.matchTerm(index, term) {
		return 0, false
	}

	conflictIndex := l.findConflict(ents)
	if conflictIndex != 0 {
		if conflictIndex <= l.committed {
			panic("entry conflict with committed entry")
		}
		offset := conflictIndex - l.entries[0].Index
		l.entries = l.entries[:offset]
		l.entries = append(l.entries, ents[conflictIndex-ents[0].Index:]...)
	} else {
		l.appendEntries(ents...)
	}

	lastNewIndex := index + uint64(len(ents))
	if committed > l.committed {
		l.committed = min(committed, lastNewIndex)
	}
	return lastNewIndex, true
}
