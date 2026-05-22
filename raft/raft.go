//raft.go implements the core Raft algorithm. The API is defined in rawnode.go, and raft.go provides the implementation of the Raft struct and its methods.

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

import (
	"errors"
	//"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log" // ✅ 添加这一行
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower's progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
	// 随机化后的实际选举超时（每次 becomeFollower/becomeCandidate 时重新随机）
	randomizedElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftLog := newLog(c.Storage)

	// 从 storage 读取持久化的 HardState 和集群成员
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	// 初始化 Prs：优先使用 config 里的 peers，否则用 confState
	peers := c.peers
	if len(peers) == 0 {
		peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress, len(peers))
	for _, id := range peers {
		prs[id] = &Progress{}
	}

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// applied 由 config 注入（重启恢复场景）
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}

	// 启动时作为 Follower
	r.becomeFollower(r.Term, None)

	return r
}

// appendEntry appends entries to the leader's local log, assigns Term/Index,
// updates leader's own Progress, and returns the new entries.
func (r *Raft) appendEntry(es []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, e := range es {
		e.Term = r.Term
		e.Index = lastIndex + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	// update leader's own progress
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

// bcastAppend sends append RPCs to all peers except self.
func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	prevLogIndex := pr.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		// prevLogTerm 不可达，说明已被 compact，改发 Snapshot
		r.sendSnapshot(to)
		return false
	}

	var entries []*pb.Entry
	lastIndex := r.RaftLog.LastIndex()
	if pr.Next <= lastIndex {
		var ents []pb.Entry
		var fetchErr error
		if len(r.RaftLog.entries) > 0 {
			firstInMem := r.RaftLog.entries[0].Index
			if pr.Next >= firstInMem {
				ents = r.RaftLog.entries[pr.Next-firstInMem:]
			} else {
				ents, fetchErr = r.RaftLog.storage.Entries(pr.Next, lastIndex+1)
			}
		} else {
			ents, fetchErr = r.RaftLog.storage.Entries(pr.Next, lastIndex+1)
		}
		if fetchErr != nil {
			// 同样改发 Snapshot
			r.sendSnapshot(to)
			return false
		}
		for i := range ents {
			entries = append(entries, &ents[i])
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendSnapshot 向 to 发送 Snapshot 消息
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// Snapshot 还没生成好，本次放弃，下次 tick 再触发
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	// 更新 Next，避免重复发
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  commit,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				From:    r.id,
				To:      r.id,
				MsgType: pb.MessageType_MsgHup,
			})
		}

	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				From:    r.id,
				To:      r.id,
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	if term > r.Term {
		r.Term = term
		r.Vote = None
	}
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.State = StateCandidate
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// 初始化所有 peer 的 Progress
	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = lastIndex
			r.Prs[id].Next = lastIndex + 1
		} else {
			r.Prs[id].Next = lastIndex + 1
			r.Prs[id].Match = 0
		}
	}

	// 追加一条 noop entry，用于提交之前 term 的日志
	noop := &pb.Entry{Data: nil}
	r.appendEntry([]*pb.Entry{noop})

	// 单节点集群立即提交
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	// 广播 append 给所有 follower
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// ── 全局 term 检查 ──────────────────────────────────────────
	switch {
	case m.Term == 0:
		// 本地消息，不做 term 检查

	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)

	case m.Term < r.Term:
		// 过期消息忽略
		return nil
	}

	// ── 消息分发 ────────────────────────────────────────────────
	switch m.MsgType {

	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			return nil
		}
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return nil
		}
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastLogTerm,
			})
		}

	case pb.MessageType_MsgBeat:
		if r.State != StateLeader {
			return nil
		}
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendHeartbeat(id)
		}

	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return ErrProposalDropped
		}
		r.appendEntry(m.Entries)
		// 单节点立即提交
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
		}
		r.bcastAppend()

	case pb.MessageType_MsgRequestVote:
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)

		logUpToDate := (m.LogTerm > lastLogTerm) ||
			(m.LogTerm == lastLogTerm && m.Index >= lastIndex)
		canVote := (r.Vote == None) || (r.Vote == m.From)
		reject := !(canVote && logUpToDate)

		if !reject {
			r.Vote = m.From
			r.electionElapsed = 0
			r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  reject,
		})

	case pb.MessageType_MsgRequestVoteResponse:
		if r.State != StateCandidate {
			return nil
		}
		r.votes[m.From] = !m.Reject

		granted, rejected := 0, 0
		for _, v := range r.votes {
			if v {
				granted++
			} else {
				rejected++
			}
		}
		quorum := len(r.Prs)/2 + 1
		if granted >= quorum {
			r.becomeLeader()
		} else if rejected >= quorum {
			r.becomeFollower(r.Term, None)
		}

	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = 0
		r.handleAppendEntries(m)

	case pb.MessageType_MsgAppendResponse:
		if r.State != StateLeader {
			return nil
		}

		pr := r.Prs[m.From]

		if m.Reject {
			pr.Next = m.Index + 1
			r.sendAppend(m.From)
			return nil
		}

		// 更新 Match/Next
		if m.Index > pr.Match {
			pr.Match = m.Index
			pr.Next = m.Index + 1
		}

		// 尝试推进 commit
		r.maybeCommit()

	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = 0
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
		})

	case pb.MessageType_MsgHeartbeatResponse:
		// leader 收到心跳回复后，检查 follower 是否落后，若落后则发送 append
		if r.State != StateLeader {
			return nil
		}
		pr := r.Prs[m.From]
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	}
	return nil
}

// maybeCommit attempts to advance the commit index if a majority of
// peers have replicated log entries up to some index N with current term.
// Returns true if commit index was advanced.
func (r *Raft) maybeCommit() bool {
	match := make([]uint64, 0, len(r.Prs))
	for _, p := range r.Prs {
		match = append(match, p.Match)
	}
	sort.Sort(sort.Reverse(uint64Slice(match)))
	quorum := len(r.Prs)/2 + 1
	n := match[quorum-1]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err == nil && logTerm == r.Term {
			r.RaftLog.committed = n
			// broadcast updated commit index to all followers
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
			return true
		}
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 1. prevLog 检查
	lastIndex := r.RaftLog.LastIndex()
	if m.Index > lastIndex {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   lastIndex,
		})
		return
	}

	if m.Index > 0 {
		localTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || localTerm != m.LogTerm {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
				Index:   m.Index - 1,
			})
			return
		}
	}

	// 2. 追加/覆盖日志
	for i, ent := range m.Entries {
		if ent.Index <= lastIndex {
			localTerm, err := r.RaftLog.Term(ent.Index)
			if err == nil && localTerm == ent.Term {
				continue
			}
			// 冲突：截断并追加
			if len(r.RaftLog.entries) > 0 {
				firstInMem := r.RaftLog.entries[0].Index
				if ent.Index >= firstInMem {
					r.RaftLog.entries = r.RaftLog.entries[:ent.Index-firstInMem]
				} else {
					r.RaftLog.entries = r.RaftLog.entries[:0]
				}
			}
			if r.RaftLog.stabled >= ent.Index {
				r.RaftLog.stabled = ent.Index - 1
			}
			for _, e := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
			lastIndex = r.RaftLog.LastIndex()
			goto committed
		} else {
			// 超出本地日志，直接追加
			for _, e := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
			lastIndex = r.RaftLog.LastIndex()
			goto committed
		}
	}

committed:
	// 3. 更新 committed
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := m.Index
		if len(m.Entries) > 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, min(lastNewEntry, lastIndex))
	}

	// 4. 回复成功
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
    snap := m.Snapshot
    if snap == nil {
        log.Infof("[DEBUG] Snapshot is nil")
        return
    }
    snapMeta := snap.Metadata
    
    log.Infof("[DEBUG] Before check: snapMeta.Index=%d, committed=%d", 
        snapMeta.Index, r.RaftLog.committed)

    // 如果 snapshot 比自己已 committed 的还旧，忽略
    if snapMeta.Index <= r.RaftLog.committed {
        log.Infof("[DEBUG] REJECTED: snapshot is stale")
        r.msgs = append(r.msgs, pb.Message{
            MsgType: pb.MessageType_MsgAppendResponse,
            From:    r.id,
            To:      m.From,
            Term:    r.Term,
            Index:   r.RaftLog.committed,
        })
        return
    }

    log.Infof("[DEBUG] APPLYING snapshot")
    r.becomeFollower(m.Term, m.From)

    // 用一条哑 entry 作为占位
    r.RaftLog.entries = []pb.Entry{{
        Index: snapMeta.Index,
        Term:  snapMeta.Term,
    }}
    
    log.Infof("[DEBUG] After setting entries: len=%d, entries[0].Index=%d", 
        len(r.RaftLog.entries), r.RaftLog.entries[0].Index)

    r.RaftLog.committed  = snapMeta.Index
    r.RaftLog.applied    = snapMeta.Index
    r.RaftLog.stabled    = snapMeta.Index
    r.RaftLog.pendingSnapshot = snap

    // ✅ 添加防御性检查
    if snapMeta.ConfState == nil {
        log.Errorf("[DEBUG] ERROR: snapMeta.ConfState is nil!")
        return
    }
    
    log.Infof("[DEBUG] ConfState.Nodes = %v", snapMeta.ConfState.Nodes)

    // 重建 Prs（成员列表）
    r.Prs = make(map[uint64]*Progress)
    for _, id := range snapMeta.ConfState.Nodes {
        r.Prs[id] = &Progress{
            Match: 0,
            Next:  snapMeta.Index + 1,
        }
    }
    
    log.Infof("[DEBUG] After rebuilding Prs: len=%d, keys=%v", 
        len(r.Prs), nodes(r))

    // 回复 AppendResponse
    r.msgs = append(r.msgs, pb.Message{
        MsgType: pb.MessageType_MsgAppendResponse,
        From:    r.id,
        To:      m.From,
        Term:    r.Term,
        Index:   snapMeta.Index,
    })
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
