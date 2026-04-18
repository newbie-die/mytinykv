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
	electionTimeout     int
	electionTimeoutBase int
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

	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	peers := c.peers
	if len(peers) == 0 {
		peers = cs.Nodes
	}
	if len(peers) == 0 {
		peers = []uint64{c.ID}
	}

	raftLog := newLog(c.Storage)
	raftLog.committed = hs.Commit
	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}

	r := &Raft{
		id:                  c.ID,
		Term:                hs.Term,
		Vote:                hs.Vote,
		RaftLog:             raftLog,
		Prs:                 make(map[uint64]*Progress),
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		msgs:                make([]pb.Message, 0),
		Lead:                None,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeoutBase: c.ElectionTick,
		electionTimeout:     c.ElectionTick + rand.Intn(c.ElectionTick-1) + 1,
		heartbeatElapsed:    0,
		electionElapsed:     0,
	}

	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range peers {
		r.Prs[peer] = &Progress{Match: 0, Next: lastIndex + 1}
	}

	return r
}

func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   lastIndex,
		LogTerm: lastTerm,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	if pr == nil {
		return false
	}

	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		return false
	}

	entries := make([]*pb.Entry, 0)
	lastIndex := r.RaftLog.LastIndex()
	firstIndex, err := r.RaftLog.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	for idx := pr.Next; idx <= lastIndex; idx++ {
		src := r.RaftLog.entries[idx-firstIndex]
		entry := src
		entries = append(entries, &entry)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) bcastRequestVote() {
	for id := range r.Prs {
		if id != r.id {
			r.sendRequestVote(id)
		}
	}
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.bcastHeartbeat()
		}
		return
	}

	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.State = StateFollower
	r.Lead = lead
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase-1) + 1
	r.votes = make(map[uint64]bool)
	r.msgs = make([]pb.Message, 0)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase-1) + 1
	r.msgs = make([]pb.Message, 0)
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase-1) + 1
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.msgs = make([]pb.Message, 0)
	lastIndex := r.RaftLog.LastIndex()
	for id, pr := range r.Prs {
		pr.Match = 0
		pr.Next = lastIndex + 1
		if id == r.id {
			pr.Match = lastIndex
		}
	}

	noop := pb.Entry{Term: r.Term, Index: lastIndex + 1}
	r.RaftLog.entries = append(r.RaftLog.entries, noop)
	if len(r.Prs) == 1 {
		r.RaftLog.committed = noop.Index
		r.Prs[r.id].Match = noop.Index
		r.Prs[r.id].Next = noop.Index + 1
	}
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.becomeCandidate()
			if r.State == StateCandidate {
				r.bcastRequestVote()
			}
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bcastHeartbeat()
		}
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return ErrProposalDropped
		}
		r.handlePropose(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		if r.State == StateLeader {
			r.handleAppendResponse(m)
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader {
			r.handleHeartbeatResponse(m)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		// transfer not implemented in 2A
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	canVote := (r.Vote == None || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm)
	if canVote {
		r.Vote = m.From
		r.electionElapsed = 0
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term, Reject: !canVote})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Term < r.Term {
		return
	}

	r.votes[m.From] = !m.Reject

	granted := 0
	for _, v := range r.votes {
		if v {
			granted++
		}
	}
	if granted*2 > len(r.Prs) {
		r.becomeLeader()
	}
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: true, Index: r.RaftLog.LastIndex()})
		return
	}

	r.becomeFollower(m.Term, m.From)
	if !r.RaftLog.matchTerm(m.Index, m.LogTerm) {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: true, Index: r.RaftLog.LastIndex()})
		return
	}

	if len(m.Entries) > 0 {
		ents := make([]pb.Entry, 0, len(m.Entries))
		for _, e := range m.Entries {
			ents = append(ents, *e)
		}
		m.Index = m.Index // keep prev index
		_, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...)
		if !ok {
			return
		}
	}

	if m.Commit > r.RaftLog.committed {
		lastIndex := r.RaftLog.LastIndex()
		if m.Commit < lastIndex {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = lastIndex
		}
	}

	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: false, Index: r.RaftLog.LastIndex()})
}

func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
		return
	}

	r.becomeFollower(m.Term, m.From)
	if m.Commit > r.RaftLog.committed {
		lastIndex := r.RaftLog.LastIndex()
		if m.Commit < lastIndex {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = lastIndex
		}
	}

	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term, Reject: false})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}
	if m.Reject {
		if m.Index > 0 {
			pr.Next = m.Index + 1
		} else {
			pr.Next--
		}
		if pr.Next == 0 {
			pr.Next = 1
		}
		return
	}

	pr.Match = m.Index
	pr.Next = m.Index + 1

	matchCount := 0
	for _, p := range r.Prs {
		if p.Match >= pr.Match {
			matchCount++
		}
	}
	if matchCount*2 > len(r.Prs) {
		commit := pr.Match
		term, err := r.RaftLog.Term(commit)
		if err != nil {
			panic(err)
		}
		if term == r.Term && commit > r.RaftLog.committed {
			r.RaftLog.committed = commit
		}
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// no-op for 2A
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, entry := range m.Entries {
		ent := pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, EntryType: entry.EntryType, Data: entry.Data}
		r.RaftLog.entries = append(r.RaftLog.entries, ent)
	}

	if len(r.Prs) == 1 {
		lastIndex := r.RaftLog.LastIndex()
		r.RaftLog.committed = lastIndex
		r.Prs[r.id].Match = lastIndex
		r.Prs[r.id].Next = lastIndex + 1
	}

	r.bcastAppend()
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
