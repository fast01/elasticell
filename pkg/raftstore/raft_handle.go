// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"fmt"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/pkg/errors"
)

type tempRaftContext struct {
	raftState  mraft.RaftLocalState
	applyState mraft.RaftApplyState
	lastTerm   uint64
	snapCell   *metapb.Cell
}

type applySnapResult struct {
	prevCell metapb.Cell
	cell     metapb.Cell
}

// ====================== raft ready handle methods
func (ps *peerStorage) handleDoApplySnapshot(ctx *tempRaftContext, snap raftpb.Snapshot) error {
	log.Infof("raftstore[cell-%d]: begin to apply snapshot", ps.cell.ID)

	snapData := &mraft.RaftSnapshotData{}
	err := snapData.Unmarshal(snap.Data)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: decode snapshot data fail, errors:\n %+v",
			ps.cell.ID,
			err)
		return err
	}

	if snapData.Cell.ID != ps.cell.ID {
		return fmt.Errorf("raftstore[cell-%d]: cell not match, snapCell=<%d> currCell=<%d>",
			ps.cell.ID,
			snapData.Cell.ID,
			ps.cell.ID)
	}

	if ps.isInitialized() {
		err := ps.clearMeta()
		if err != nil {
			log.Errorf("raftstore[cell-%d]: clear meta failure, errors:\n %+v",
				ps.cell.ID,
				err)
			return err
		}
	}

	err = ps.updatePeerState(ps.cell, mraft.Applying)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: write peer state failure, errors:\n %+v",
			ps.cell.ID,
			err)
		return err
	}

	ctx.raftState.LastIndex = snap.Metadata.Index
	ctx.lastTerm = snap.Metadata.Term

	// The snapshot only contains log which index > applied index, so
	// here the truncate state's (index, term) is in snapshot metadata.
	ctx.applyState.AppliedIndex = snap.Metadata.Index
	ctx.applyState.TruncatedState.Index = snap.Metadata.Index
	ctx.applyState.TruncatedState.Term = snap.Metadata.Term
	c := ps.cell
	ctx.snapCell = &c
	log.Infof("raftstore[cell-%d]: apply snapshot ok, state=<%s>",
		ps.cell.ID,
		ctx.applyState.String())

	return nil
}

// handleDoAppendEntries the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *peerStorage) handleDoAppendEntries(ctx *tempRaftContext, entries []raftpb.Entry) error {
	c := len(entries)

	log.Debugf("raftstore[cell-%d]: append entries, count=<%d>",
		ps.cell.ID,
		c)

	if c == 0 {
		return nil
	}

	prevLastIndex := ctx.raftState.LastIndex
	lastIndex := entries[c-1].Index
	lastTerm := entries[c-1].Term

	for _, e := range entries {
		d, _ := e.Marshal()
		err := ps.store.engine.Set(getRaftLogKey(ps.cell.ID, e.Index), d)
		if err != nil {
			log.Errorf("raftstore[cell-%d]: append entry failure, entry=<%s> errors:\n %+v",
				ps.cell.ID,
				e.String(),
				err)
			return err
		}
	}

	// Delete any previously appended log entries which never committed.
	for index := lastIndex + 1; index < prevLastIndex+1; index++ {
		err := ps.store.engine.Delete(getRaftLogKey(ps.cell.ID, index))
		if err != nil {
			log.Errorf("raftstore[cell-%d]: delete any previously appended log entries failure, index=<%d> errors:\n %+v",
				ps.cell.ID,
				index,
				err)
			return err
		}
	}

	ctx.raftState.LastIndex = lastIndex
	ctx.lastTerm = lastTerm

	return nil
}

func (ps *peerStorage) handleDoSaveRaftState(ctx *tempRaftContext) error {
	data, _ := ctx.raftState.Marshal()
	err := ps.store.engine.Set(getRaftStateKey(ps.cell.ID), data)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: save temp raft state failure, errors:\n %+v",
			ps.cell.ID,
			err)
	}

	return err
}

func (ps *peerStorage) handleDoSaveApplyState(ctx *tempRaftContext) error {
	data, _ := ctx.applyState.Marshal()
	err := ps.store.engine.Set(getApplyStateKey(ps.cell.ID), data)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: save temp apply state failure, errors:\n %+v",
			ps.cell.ID,
			err)
	}

	return err
}

func (ps *peerStorage) handleDoPostReady(ctx *tempRaftContext) *applySnapResult {
	ps.raftState = ctx.raftState
	ps.applyState = ctx.applyState
	ps.lastTerm = ctx.lastTerm

	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.snapCell == nil {
		return nil
	}

	// cleanup data before task
	if ps.isInitialized() {
		// TODO: why??
		err := ps.clearExtraData(ps.cell)
		if err != nil {
			// No need panic here, when applying snapshot, the deletion will be tried
			// again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
			// [b, c) will be kept in rocksdb until a covered snapshot is applied or
			// store is restarted.
			log.Errorf("raftstore[cell-%d]: cleanup data fail, may leave some dirty data, errors:\n %+v",
				ps.cell.ID,
				err)
			return nil
		}
	}

	ps.startApplyingSnapJob()

	prevCell := ps.cell
	ps.cell = *ctx.snapCell

	return &applySnapResult{
		prevCell: prevCell,
		cell:     ps.cell,
	}
}

// ======================raft storage interface method

func (ps *peerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hardState := ps.raftState.HardState
	confState := raftpb.ConfState{}

	if hardState.Commit == 0 &&
		hardState.Term == 0 &&
		hardState.Vote == 0 {
		if ps.isInitialized() {
			log.Fatalf("raftstore[cell-%d]: cell is initialized but local state has empty hard state, hardState=<%v>",
				ps.cell.ID,
				hardState)
		}

		return hardState, confState, nil
	}

	for _, p := range ps.cell.Peers {
		confState.Nodes = append(confState.Nodes, p.ID)
	}

	return hardState, confState, nil
}

func (ps *peerStorage) Entries(low, high, maxSize uint64) ([]raftpb.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}

	var ents []raftpb.Entry
	if low == high {
		return ents, nil
	}

	var totalSize uint64
	nextIndex := low
	exceededMaxSize := false

	startKey := getRaftLogKey(ps.cell.ID, low)

	if low+1 == high {
		// If election happens in inactive cells, they will just try
		// to fetch one empty log.
		v, err := ps.store.engine.Get(startKey)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		if nil == v {
			return nil, raft.ErrUnavailable
		}

		e, err := ps.unmarshal(v, low)
		if err != nil {
			return nil, err
		}

		ents = append(ents, *e)
		return ents, nil
	}

	endKey := getRaftLogKey(ps.cell.ID, high)
	err = ps.store.engine.Scan(startKey, endKey, func(data []byte) (bool, error) {
		e, err := ps.unmarshal(data, nextIndex)
		if err != nil {
			return false, err
		}

		nextIndex++
		totalSize += uint64(len(data))

		exceededMaxSize = totalSize > maxSize
		if !exceededMaxSize || len(ents) == 0 {
			ents = append(ents, *e)
		}

		return !exceededMaxSize, nil
	})

	if err != nil {
		return nil, err
	}

	// If we get the correct number of entries the total size exceeds max_size, returns.
	if len(ents) == int(high-low) || exceededMaxSize {
		return ents, nil
	}

	return nil, raft.ErrUnavailable
}

func (ps *peerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.getTruncatedIndex() {
		return ps.getTruncatedTerm(), nil
	}

	err := ps.checkRange(idx, idx+1)
	if err != nil {
		return 0, err
	}

	lastIdx, err := ps.LastIndex()
	if err != nil {
		return 0, err
	}

	if ps.getTruncatedTerm() == ps.lastTerm || idx == lastIdx {
		return ps.lastTerm, nil
	}

	key := getRaftLogKey(ps.cell.ID, idx)
	v, err := ps.store.engine.Get(key)
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, raft.ErrUnavailable
	}

	e, err := ps.unmarshal(v, idx)
	if err != nil {
		return 0, err
	}

	return e.Term, nil
}

func (ps *peerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *peerStorage) FirstIndex() (uint64, error) {
	return ps.applyState.TruncatedState.Index + 1, nil
}

func (ps *peerStorage) Snapshot() (raftpb.Snapshot, error) {
	if ps.isGeneratingSnap() {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	if ps.isGenSnapJobComplete() {
		result := ps.applySnapJob.GetResult()
		// snapshot failure, we will continue try do snapshot
		if nil == result {
			log.Warnf("raftstore[cell-%d]: snapshot generating failed, triedCnt=<%d>",
				ps.cell.ID,
				ps.snapTriedCnt)
			ps.snapTriedCnt++
		} else {
			snap := result.(*raftpb.Snapshot)
			ps.snapTriedCnt = 0
			if ps.validateSnap(snap) {
				ps.resetGenSnapJob()
				return *snap, nil
			}
		}
	}

	if ps.snapTriedCnt >= maxSnapTryCnt {
		cnt := ps.snapTriedCnt
		ps.resetGenSnapJob()
		return raftpb.Snapshot{}, fmt.Errorf("raftstore[cell-%d]: failed to get snapshot after %d times",
			ps.cell.ID,
			cnt)
	}

	log.Infof("raftstore[cell-%d]: start snapshot", ps.cell.ID)
	ps.snapTriedCnt++

	job, err := ps.store.addJob(ps.doGenerateSnapshotJob)
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: add generate job failed, errors:\n %+v",
			ps.cell.ID,
			err)
	}
	ps.genSnapJob = job
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}
