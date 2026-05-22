//peer_storage.go implements the PeerStorage struct, which is responsible for managing the persistent state of a Raft peer, including the Raft log entries, Raft state, and apply state. It provides methods to retrieve log entries, terms, and snapshots, as well as to save the ready state and apply snapshots. The PeerStorage interacts with the underlying storage engines (Badger) to persist and retrieve data.

package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// generate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// 新增辅助函数，用于打印日志
func dbgPSRangeEntries(ents []eraftpb.Entry) (uint64, uint64, int) {
	if len(ents) == 0 {
		return 0, 0, 0
	}
	return ents[0].Index, ents[len(ents)-1].Index, len(ents)
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 || (len(oldEndKey) == 0 && len(newEndKey) != 0) {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	//------------------------打印日志------------------------------------------------
	// eFirst, eLast, eN := dbgPSRangeEntries(entries)
	// log.Infof("[DBG-PERSIST] region=%d Append begin entries=[%d,%d] n=%d oldLastIndex=%d oldLastTerm=%d",
	// 	ps.region.Id, eFirst, eLast, eN, ps.raftState.LastIndex, ps.raftState.LastTerm)
	//---------------------------------------------------------------------------------

	if len(entries) == 0 {
		//---------------------------------------打印日志---------------------------------
		// log.Infof("[DBG-PERSIST] region=%d Append skip empty entries", ps.region.Id)
		//---------------------------------------------------------------------------------
		return nil
	}

	lastEntry := entries[len(entries)-1]
	lastNewIndex := lastEntry.Index
	lastNewTerm := lastEntry.Term
	oldLastIndex := ps.raftState.LastIndex

	// Write new entries
	for _, entry := range entries {
		raftWB.SetMeta(meta.RaftLogKey(ps.region.Id, entry.Index), &entry)
		//----------------打印日志---------------------------------------------------------------
		// if entry.Index == eFirst || entry.Index == eLast {
		// 	log.Infof(
		// 		"[DBG-PERSIST] Append WRITE ENTRY region=%d index=%d term=%d",
		// 		ps.region.GetId(),
		// 		entry.Index,
		// 		entry.Term,
		// 	)
		// }
		//-----------------------------------------------------------------------------------
	}
	//----------------------打印日志----------------------------------------------------------
	// log.Infof("[DBG-PERSIST] region=%d Append wrote entries=[%d,%d] n=%d",
	// 	ps.region.Id, eFirst, eLast, eN)
	//----------------------------------------------------------------------------------------

	// Delete overwritten old log entries
	//--------------------打印日志--------------------------------------------------------------
	// if lastNewIndex+1 <= oldLastIndex {
	// 	log.Infof("[DBG-PERSIST] region=%d Append delete overwritten range=[%d,%d]",
	// 		ps.region.Id, lastNewIndex+1, oldLastIndex)
	// }
	//-----------------------------------------------------------------------------------------------
	for i := lastNewIndex + 1; i <= oldLastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(ps.region.Id, i))
	}

	// Update raft state
	ps.raftState.LastIndex = lastNewIndex
	ps.raftState.LastTerm = lastNewTerm
	//-----------------------打印日志---------------------------------------------------------
	log.Infof("[DBG-PERSIST] region=%d Append end newLastIndex=%d newLastTerm=%d",
		ps.region.Id, ps.raftState.LastIndex, ps.raftState.LastTerm)
	//------------------------------------------------------------------------------------------

	return nil
}

// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	raftWB := new(engine_util.WriteBatch)
	kvWB := new(engine_util.WriteBatch)

	var applySnapResult *ApplySnapResult

	// 1. 如果有 Snapshot，先 apply
	if !raft.IsEmptySnap(&ready.Snapshot) {
		result, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, err
		}
		applySnapResult = result
		// ApplySnapshot 内部已写 kvWB / raftWB，先落盘
		if err := kvWB.WriteToDB(ps.Engines.Kv); err != nil {
			return nil, err
		}
		if err := raftWB.WriteToDB(ps.Engines.Raft); err != nil {
			return nil, err
		}
		// 重置 WriteBatch
		kvWB = new(engine_util.WriteBatch)
		raftWB = new(engine_util.WriteBatch)
	}

	// 2. 持久化 entries
	if err := ps.Append(ready.Entries, raftWB); err != nil {
		return nil, err
	}

	// 3. 持久化 HardState
	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}
	raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)

	// 4. 落盘
	if err := raftWB.WriteToDB(ps.Engines.Raft); err != nil {
		return nil, err
	}

	return applySnapResult, nil
}

// Apply the peer with given snapshot
func (ps *PeerStorage) ApplySnapshot(
	snapshot *eraftpb.Snapshot,
	kvWB *engine_util.WriteBatch,
	raftWB *engine_util.WriteBatch,
) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)

	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// 先保存 prev region 用于返回
	prevRegion := ps.region

	// 清除旧 meta（只有 peer 已初始化时才有旧数据需要清）
	if ps.isInitialized() {
		if err := ps.clearMeta(kvWB, raftWB); err != nil {
			return nil, err
		}
		ps.clearExtraData(snapData.Region)
	}

	// 用 snapshot metadata 更新 raftState
	snapMeta := snapshot.Metadata
	ps.raftState.LastIndex = snapMeta.Index
	ps.raftState.LastTerm = snapMeta.Term

	// 用 snapshot metadata 更新 applyState
	ps.applyState.AppliedIndex = snapMeta.Index
	ps.applyState.TruncatedState = &rspb.RaftTruncatedState{
		Index: snapMeta.Index,
		Term:  snapMeta.Term,
	}

	// 更新 region
	ps.region = snapData.Region

	// 持久化 regionLocalState（标记为 Normal）
	regionState := &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: snapData.Region,
	}
	kvWB.SetMeta(meta.RegionStateKey(snapData.Region.Id), regionState)

	// 持久化 applyState 和 raftState
	kvWB.SetMeta(meta.ApplyStateKey(snapData.Region.Id), ps.applyState)
	raftWB.SetMeta(meta.RaftStateKey(snapData.Region.Id), ps.raftState)

	// 异步（这里同步等待）安装 snapshot 数据到 kvDB
	ch := make(chan bool, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Applying,
	}
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: snapData.Region.Id,
		Notifier: ch,
		SnapMeta: snapMeta,
		StartKey: snapData.Region.GetStartKey(),
		EndKey:   snapData.Region.GetEndKey(),
	}
	// 等待安装完成
	<-ch

	return &ApplySnapResult{
		PrevRegion: prevRegion,
		Region:     snapData.Region,
	}, nil
}

// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
// func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
// 	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
// 	// Your Code Here (2B/2C).
// 	return nil, nil
// }

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
