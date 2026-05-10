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

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
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
// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
    switch r.State {
    case StateFollower, StateCandidate:
        r.electionElapsed++
        if r.electionElapsed >= r.randomizedElectionTimeout {
            r.electionElapsed = 0
            // 触发选举：Step 内部会调用 becomeCandidate 并广播 MsgRequestVote
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
            // 触发心跳广播：Step 内部会向所有 follower 发送 MsgHeartbeat
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
        // term 升高才清空投票记录
        r.Term = term
        r.Vote = None
    }
    // 重置选举计时器并随机化超时
    r.electionElapsed = 0
    r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
    // 任期递增
    r.Term++
    r.State = StateCandidate
    r.Lead = None
    // 投票给自己
    r.Vote = r.id
    r.votes = make(map[uint64]bool)
    r.votes[r.id] = true
    // 重置选举计时器
    r.electionElapsed = 0
    r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
    r.State = StateLeader
    r.Lead = r.id
    r.heartbeatElapsed = 0

    // 初始化所有 follower 的 Progress
    lastIndex := r.RaftLog.LastIndex()
    for id := range r.Prs {
        r.Prs[id].Next = lastIndex + 1
        r.Prs[id].Match = 0
    }
    r.Prs[r.id].Match = lastIndex
    r.Prs[r.id].Next = lastIndex + 1

    // NOTE: Leader 当选后追加一条 noop entry（2AB 实现 append 后解除注释）
    // r.appendEntry(&pb.Entry{Data: nil})

    // 当选后立即广播心跳，让 followers 知晓新 leader
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
	case pb.MessageType_MsgAppend:
    // 2AA: 仅处理选举相关语义
    // candidate/leader/follower 收到合法 Append（term 已经在前面检查过 >= r.Term）
    // 都应承认发送方为 leader 并转为 follower
    r.becomeFollower(m.Term, m.From)
    r.electionElapsed = 0
    // 2AB 再实现 append entries 细节
    return nil
	
    case pb.MessageType_MsgHeartbeatResponse:
        // 2AB 实现日志同步后处理

    }
    return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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