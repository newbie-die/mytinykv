package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

const None uint64 = 0

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

var ErrProposalDropped = errors.New("raft proposal dropped")

type Config struct {
	ID            uint64
	peers         []uint64
	ElectionTick  int
	HeartbeatTick int
	Storage       Storage
	Applied       uint64
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

type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id               uint64
	Term             uint64
	Vote             uint64
	RaftLog          *RaftLog
	Prs              map[uint64]*Progress
	State            StateType
	votes            map[uint64]bool
	msgs             []pb.Message
	Lead             uint64
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election timeout in ticks. It is reset on state changes and
	// when the follower receives valid append/heartbeat from a leader.
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	electionElapsed  int
	leadTransferee   uint64
	PendingConfIndex uint64
}

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
	// if pr.Next > lastIndex && r.RaftLog.committed <= pr.Match {
	// 	return false
	// }

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
	log.Printf("[%d] becomeFollower: term=%d, lead=%d", r.id, term, lead)

	if term > r.Term {
		r.Term = term
		r.Vote = None // ✅ 只有 term 变大才清 vote
	}

	r.State = StateFollower
	r.Lead = lead

	r.resetVotes()
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

func (r *Raft) becomeCandidate() {
	log.Printf("[%d] becomeCandidate: term=%d", r.id, r.Term)
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
	log.Printf("[%d] becomeLeader: term=%d", r.id, r.Term)
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
	log.Printf("[%d] Step: handling message type %v, term=%d", r.id, m.MsgType, m.Term) // 关键日志

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			return nil
		}

		r.becomeCandidate()

		// 单节点直接成为 leader
		if r.quorum() == 1 {
			r.becomeLeader()
			return nil
		}

		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)

		// 给所有其他节点发送投票请求
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      id,
				From:    r.id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}

	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bcastHeartbeat()
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

		// If the request's term is less than our current term, we reject the vote.
		if m.Term < r.Term {
			resp.Reject = true
			r.msgs = append(r.msgs, resp)
			return nil
		}

		// If the node has not voted yet or it's voting for the same candidate, we can vote
		if (r.Vote == None || r.Vote == m.From) &&
			r.isLogUpToDate(m.Index, m.LogTerm) {

			// If the candidate's term is greater, become a follower with the new term.
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}

			// Grant vote to the candidate and set the vote field
			r.Vote = m.From
			r.electionElapsed = 0 // ⭐⭐关键修复
			resp.Reject = false
			r.msgs = append(r.msgs, resp)
			return nil
		}

		// If none of the conditions were satisfied, reject the vote request
		resp.Reject = true
		r.msgs = append(r.msgs, resp)

	case pb.MessageType_MsgRequestVoteResponse:
		log.Printf("[%d] recv RequestVoteResponse from %d term=%d reject=%v\n", r.id, m.From, m.Term, m.Reject)

		// 确保只有在候选人的 term 和消息的 term 一致时才继续处理
		if r.State != StateCandidate || m.Term != r.Term {
			return nil
		}

		r.votes[m.From] = !m.Reject
		granted, rejected := r.countVotes()
		log.Printf("[%d] votes count: granted=%d rejected=%d\n", r.id, granted, rejected) // 关键日志

		// 如果获得足够票数，转换为 Leader
		if granted >= r.quorum() {
			r.becomeLeader()
		} else if rejected >= r.quorum() {
			r.becomeFollower(r.Term, None)
		}

	case pb.MessageType_MsgAppend:
		log.Printf("[%d] recv MsgAppend from %d term=%d\n", r.id, m.From, m.Term)
		r.handleAppendEntries(m)

	case pb.MessageType_MsgAppendResponse:
		log.Printf("[%d] recv MsgAppendResponse from %d term=%d reject=%v\n", r.id, m.From, m.Term, m.Reject)
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
		log.Printf("[%d] recv MsgHeartbeat from %d term=%d\n", r.id, m.From, m.Term)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		log.Printf("[%d] recv MsgHeartbeatResponse from %d term=%d\n", r.id, m.From, m.Term)

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
	case pb.MessageType_MsgTransferLeader:
		// transfer not implemented in 2A
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	log.Printf("[%d] handleAppendEntries: term=%d, index=%d", r.id, m.Term, m.Index)
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
	log.Printf("[%d] handleHeartbeat: term=%d", r.id, m.Term)
	resp := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term}
	if m.Term < r.Term {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}
	log.Printf("[%d] handleHeartbeat: term=%d, index=%d", r.id, m.Term, m.Index)
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
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{}
	}
}

func (r *Raft) removeNode(id uint64) {
	delete(r.Prs, id)
}
