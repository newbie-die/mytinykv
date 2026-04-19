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
	"fmt"
	"math/rand"

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

// Progress represents a follower’s progress in the view of the leader. Leader maintains
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
	// randomized election timeout in ticks. It is reset on state changes and
	// when the follower receives valid append/heartbeat from a leader.
	randomizedElectionTimeout int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	rl := newLog(c.Storage)
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          rl,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	r.resetRandomizedElectionTimeout()
	if hardState.Commit > rl.committed {
		rl.committed = hardState.Commit
	}
	if c.Applied > rl.applied {
		rl.applied = c.Applied
	}
	if c.peers != nil {
		lastIndex := rl.LastIndex()
		for _, id := range c.peers {
			r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	if pr == nil {
		return false
	}
	lastIndex := r.RaftLog.LastIndex()
	if pr.Next > lastIndex && r.RaftLog.committed <= pr.Match {
		return false
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   pr.Next - 1,
		Commit:  r.RaftLog.committed,
	}
	// 👇 修改这里：去掉 if msg.Index > 0 的判断
    logTerm, err := r.RaftLog.Term(msg.Index)
    if err != nil {
        return false
    }
    msg.LogTerm = logTerm
	if pr.Next <= lastIndex {
		first := r.RaftLog.firstIndex()
		start := pr.Next - first
		end := lastIndex + 1 - first
		if start < uint64(len(r.RaftLog.entries)) && end <= uint64(len(r.RaftLog.entries)) && start < end {
			msg.Entries = append([]*pb.Entry{}, toEntries(r.RaftLog.entries[start:end])...)
		}
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func toEntries(ents []pb.Entry) []*pb.Entry {
	res := make([]*pb.Entry, len(ents))
	for i := range ents {
		res[i] = &ents[i]
	}
	return res
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) resetRandomizedElectionTimeout() {
	if r.electionTimeout <= 1 {
		r.randomizedElectionTimeout = r.electionTimeout
		return
	}
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout-1) + 1
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, To: r.id})
		}
	}
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) countVotes() (granted, rejected int) {
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}
	return
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if term > r.Term {
		r.Term = term
	}
	r.State = StateFollower
	r.Lead = lead
	r.Vote = None
	r.resetVotes()
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.State = StateCandidate
	r.Lead = None
	r.Vote = r.id
	r.resetVotes()
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	if r.Prs[r.id] != nil {
		last := r.RaftLog.LastIndex()
		r.Prs[r.id].Match = last
		r.Prs[r.id].Next = last + 1
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	panic("🌟 恭喜，新代码终于生效了！🌟")
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	// 1. 追加 No-op 空日志（这是 2AB 很多测试暗中期待的）
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})

	// 2. 初始化所有人的进度 (Next 和 Match)
	last := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = last
			r.Prs[id].Next = last + 1
		} else {
			r.Prs[id].Match = 0
			r.Prs[id].Next = last + 1
		}
	}

	// 3. 尝试提交并广播
	r.maybeCommit()
	r.bcastAppend()
}
func (r *Raft) isLogUpToDate(index, logTerm uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		return false
	}
	if logTerm != lastTerm {
		return logTerm > lastTerm
	}
	return index >= lastIndex
}

func (r *Raft) maybeCommit() bool {
	majority := r.quorum()
	for i := r.RaftLog.LastIndex(); i > r.RaftLog.committed; i-- {
		if term, err := r.RaftLog.Term(i); err == nil && term == r.Term {
			count := 0
			for _, pr := range r.Prs {
				if pr.Match >= i {
					count++
				}
			}
			if count >= majority {
				r.RaftLog.committed = i
				return true
			}
		}
	}
	return false
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.becomeCandidate()
			lastIndex := r.RaftLog.LastIndex()
			logTerm, _ := r.RaftLog.Term(lastIndex)
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, To: id, From: r.id, Term: r.Term, LogTerm: logTerm, Index: lastIndex})
			}
			granted, _ := r.countVotes()
			if granted >= r.quorum() {
				r.becomeLeader()
			}
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		}
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return ErrProposalDropped
		}
		if len(m.Entries) == 0 {
			return nil
		}
		
		// 🌟 恢复正确的 Leader 提案逻辑：直接赋予 Term 和 Index 并追加
		for _, entry := range m.Entries {
			entry.Term = r.Term
			entry.Index = r.RaftLog.LastIndex() + 1
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
		
		if r.Prs[r.id] != nil {
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		}
		r.maybeCommit()
		r.bcastAppend()
	case pb.MessageType_MsgRequestVote:
		fmt.Printf("[%d] recv RequestVote from %d term=%d logterm=%d index=%d state=%s vote=%d\n", r.id, m.From, m.Term, m.LogTerm, m.Index, r.State, r.Vote)
		resp := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term}
		if m.Term < r.Term {
			resp.Reject = true
			r.msgs = append(r.msgs, resp)
			return nil
		}
		if r.Vote == None || r.Vote == m.From {
			if r.isLogUpToDate(m.Index, m.LogTerm) {
				r.becomeFollower(m.Term, None)
				r.Vote = m.From
				resp.Reject = false
				r.msgs = append(r.msgs, resp)
				return nil
			}
		}
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
	case pb.MessageType_MsgRequestVoteResponse:
		fmt.Printf("[%d] recv RequestVoteResponse from %d term=%d reject=%v state=%s\n", r.id, m.From, m.Term, m.Reject, r.State)
		if r.State != StateCandidate || m.Term != r.Term {
			return nil
		}
		r.votes[m.From] = !m.Reject
		granted, _ := r.countVotes()
		fmt.Printf("[%d] votes after response: %v granted=%d\n", r.id, r.votes, granted)
		if granted >= r.quorum() {
			r.becomeLeader()
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		if r.State != StateLeader || m.Term < r.Term {
			return nil
		}
		pr := r.Prs[m.From]
		if pr == nil {
			return nil
		}

		if m.Reject {
			// 🚀 快速回退：不再傻傻地 pr.Next-- 慢慢退，而是直接退到 Follower 告诉我们的位置
			pr.Next = m.Index
			r.sendAppend(m.From)
			return nil
		}

		// 成功的情况
		if m.Index > pr.Match {
			pr.Match = m.Index
			pr.Next = m.Index + 1
			if r.maybeCommit() {
				r.bcastAppend()
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State != StateLeader {
			return nil
		}
		if m.Term < r.Term {
			return nil
		}
		pr := r.Prs[m.From]
		if pr == nil {
			return nil
		}
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	resp := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term}
	
	if m.Term < r.Term {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}

	// 只要 Term >= 当前 Term，就承认对方是 Leader
	r.becomeFollower(m.Term, m.From)

	// 校验日志一致性 (Log Matching)
	if m.Index > r.RaftLog.LastIndex() {
		resp.Reject = true
		resp.Index = r.RaftLog.LastIndex() + 1 // 告诉 Leader 我缺很多，请往前发
		r.msgs = append(r.msgs, resp)
		return
	}

	if m.Index >= r.RaftLog.firstIndex() {
		term, _ := r.RaftLog.Term(m.Index)
		if term != m.LogTerm {
			resp.Reject = true
			resp.Index = m.Index // 发生冲突，告诉 Leader 退回到这个位置重试
			r.msgs = append(r.msgs, resp)
			return
		}
	}

	// 开始追加和截断日志
	for _, entry := range m.Entries {
		if entry.Index < r.RaftLog.firstIndex() {
			continue // 已被快照清理，忽略
		}
		
		if entry.Index <= r.RaftLog.LastIndex() {
			term, _ := r.RaftLog.Term(entry.Index)
			if term != entry.Term {
				// 发生冲突：截断
				idx := entry.Index - r.RaftLog.firstIndex()
				r.RaftLog.entries = r.RaftLog.entries[:idx]
				
				// 关键保护：防止 stabled 越界 Panic
				if r.RaftLog.stabled >= entry.Index {
					r.RaftLog.stabled = entry.Index - 1
				}
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		} else {
			// 直接在尾部追加
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}

	// 更新 Commit Index
	if m.Commit > r.RaftLog.committed {
		lastNewIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewIndex = m.Entries[len(m.Entries)-1].Index
		}
		if m.Commit < lastNewIndex {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = lastNewIndex
		}
	}

	resp.Reject = false
	resp.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	resp := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term}
	if m.Term < r.Term {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}
	if m.Term > r.Term || r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	} else {
		r.Lead = m.From
		r.electionElapsed = 0
	}
	if m.Commit > r.RaftLog.committed {
		lastIndex := r.RaftLog.LastIndex()
		if m.Commit < lastIndex {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = lastIndex
		}
	}
	resp.Reject = false
	resp.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, resp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Snapshot handling not implemented in 2A/2B.
}

func (r *Raft) advance(rd Ready) {
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1].Index
		r.RaftLog.stableTo(last)
	}
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		r.RaftLog.appliedTo(last)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
