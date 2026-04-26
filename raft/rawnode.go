package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)
// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
// RawNode wraps the Raft implementation to provide an interface for the application.
type RawNode struct {
	Raft *Raft
}

// NewRawNode returns a new RawNode with the given configuration.
func NewRawNode(config *Config) (*RawNode, error) {
	raft := newRaft(config)
	return &RawNode{Raft: raft}, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent},
	})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
    return Ready{
        SoftState: &SoftState{
            Lead:      rn.Raft.Lead,
            RaftState: rn.Raft.State,
        },
        HardState: pb.HardState{
            Term:   rn.Raft.Term,
            Vote:   rn.Raft.Vote,
            Commit: rn.Raft.RaftLog.committed,
        },
        Entries:          rn.Raft.RaftLog.unstableEntries(),
        CommittedEntries: rn.Raft.RaftLog.nextEnts(),
        Messages:         rn.Raft.msgs,
    }
}
// // RawNode 中的 Advance 方法
// func (rn *RawNode) Advance(rd Ready) {
// 	rn.Raft.RaftLog.ApplyUpTo(rd.HardState.Commit)
// 	// Apply all committed entries to state machine
// 	// (这部分依赖于你的应用逻辑，可能需要修改)
// 	rn.Raft.msgs = nil // 清空消息
// }

// HasReady checks if there are any pending Ready state.
func (rn *RawNode) HasReady() bool {
    if len(rn.Raft.msgs) > 0 {
        return true
    }
    if len(rn.Raft.RaftLog.unstableEntries()) > 0 {
        return true
    }
    if len(rn.Raft.RaftLog.nextEnts()) > 0 {
        return true
    }
    return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the last Ready results.
func (rn *RawNode) Advance(rd Ready) {
    if len(rd.Entries) > 0 {
        last := rd.Entries[len(rd.Entries)-1].Index
        rn.Raft.RaftLog.stabled = last
    }

    if len(rd.CommittedEntries) > 0 {
        last := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
        rn.Raft.RaftLog.applied = last
    }

    rn.Raft.msgs = nil
}