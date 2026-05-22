//log.go implements the log module of Raft, which manages the log entries and their states (committed, applied, etc.). It provides APIs for appending entries, retrieving entries, and managing the log state. The Raft struct in raft.go uses RaftLog to manage its log entries and states.

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
	// 从 storage 恢复初始状态
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// 读取 [firstIndex, lastIndex] 之间的所有已持久化 entries
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil && err != ErrUnavailable {
		panic(err)
	}
	if err == ErrUnavailable {
		entries = []pb.Entry{}
	}

	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	raftLog := &RaftLog{
		storage:   storage,
		entries:   entries,
		committed: hardState.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// 1. 检查 storage 是否为 nil
	if l.storage == nil {
		return
	}
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	// storage 的 firstIndex = truncatedIndex + 1
	// entries[0] 是哑 entry，其 Index 就是之前的 truncatedIndex
	// 只有当 storage 的 truncated 推进到超过我们内存第一条时才截断
	if len(l.entries) > 0 && firstIndex > l.entries[0].Index+1 {
		// 截掉已被 storage compact 的部分，保留从 firstIndex 开始的 entries
		// 但要保留一条哑 entry（index = firstIndex-1）
		newDummy := firstIndex - 1
		cutFrom := newDummy - l.entries[0].Index
		if cutFrom >= uint64(len(l.entries)) {
			// 全部被 compact，只保留一条哑 entry
			l.entries = []pb.Entry{{Index: newDummy}}
		} else {
			l.entries = l.entries[cutFrom:]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	// stabled 之后的 entries 都是 unstable
	firstInMem := l.entries[0].Index
	if l.stabled+1 > firstInMem+uint64(len(l.entries))-1 {
		return []pb.Entry{}
	}
	offset := l.stabled + 1 - firstInMem
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return nil
	}
	firstInMem := l.entries[0].Index
	// applied+1 到 committed 之间是待 apply 的 entries
	lo := l.applied + 1
	hi := l.committed + 1
	if lo >= hi {
		return nil
	}
	if lo < firstInMem {
		lo = firstInMem
	}
	return l.entries[lo-firstInMem : hi-firstInMem]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// 优先取内存中最后一条
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// 内存为空则从 storage 取
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 先在内存 entries 中查找
	if len(l.entries) > 0 {
		firstInMem := l.entries[0].Index
		lastInMem := l.entries[len(l.entries)-1].Index
		if i >= firstInMem && i <= lastInMem {
			return l.entries[i-firstInMem].Term, nil
		}
	}
	// 回退到 storage 查找（包含 dummy entry 和已 compact 的 entries）
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil // ← 修复 missing return
}
