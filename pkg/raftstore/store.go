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
	"bytes"
	"fmt"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
	"github.com/deepfabric/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

const (
	applyWorker  = "apply-worker-%d"
	snapWorker   = "snap-worerk"
	pdWorker     = "pd-worker"
	splitWorker  = "split-worker"
	raftGCWorker = "raft-gc-worker"

	size8    = 8
	size16   = 16
	size32   = 32
	size64   = 64
	size128  = 128
	size1024 = 1024
	size2048 = 2048
)

var (
	globalCfg *Cfg
)

// Store is the store for raft
type Store struct {
	id        uint64
	clusterID uint64
	startAt   uint32
	meta      metapb.Store

	snapshotManager SnapshotManager

	pdClient *pd.Client

	replicatesMap *cellPeersMap // cellid -> peer replicate
	keyRanges     *util.CellTree
	peerCache     *peerCacheMap
	delegates     *applyDelegateMap
	pendingCells  []metapb.Cell

	trans    *transport
	engine   storage.Driver
	runner   *util.Runner
	messages []*queue.Queue

	redisReadHandles  map[raftcmdpb.CMDType]func(*raftcmdpb.Request) *raftcmdpb.Response
	redisWriteHandles map[raftcmdpb.CMDType]func(*execContext, *raftcmdpb.Request) *raftcmdpb.Response

	sendingSnapCount   uint32
	reveivingSnapCount uint32
}

// NewStore returns store
func NewStore(clusterID uint64, pdClient *pd.Client, meta metapb.Store, engine storage.Driver, cfg *Cfg) *Store {
	globalCfg = cfg

	s := new(Store)
	s.clusterID = clusterID
	s.id = meta.ID
	s.meta = meta
	s.startAt = uint32(time.Now().Unix())
	s.engine = engine
	s.pdClient = pdClient
	s.snapshotManager = newDefaultSnapshotManager(cfg, engine.GetDataEngine())

	s.trans = newTransport(s, pdClient, s.notify)

	s.messages = make([]*queue.Queue, globalCfg.RaftMessageWorkerCount)
	var index uint64
	for ; index < globalCfg.RaftMessageWorkerCount; index++ {
		s.messages[index] = &queue.Queue{}
	}

	s.keyRanges = util.NewCellTree()
	s.replicatesMap = newCellPeersMap()
	s.peerCache = newPeerCacheMap()
	s.delegates = newApplyDelegateMap()

	s.runner = util.NewRunner()
	for i := 0; i < int(globalCfg.ApplyWorkerCount); i++ {
		s.runner.AddNamedWorker(fmt.Sprintf(applyWorker, i))
	}
	s.runner.AddNamedWorker(snapWorker)
	s.runner.AddNamedWorker(pdWorker)
	s.runner.AddNamedWorker(splitWorker)
	s.runner.AddNamedWorker(raftGCWorker)

	s.redisReadHandles = make(map[raftcmdpb.CMDType]func(*raftcmdpb.Request) *raftcmdpb.Response)
	s.redisWriteHandles = make(map[raftcmdpb.CMDType]func(*execContext, *raftcmdpb.Request) *raftcmdpb.Response)

	s.initRedisHandle()
	s.init()

	return s
}

func (s *Store) init() {
	totalCount := 0
	tomebstoneCount := 0
	applyingCount := 0

	wb := s.engine.NewWriteBatch()

	err := s.getMetaEngine().Scan(cellMetaMinKey, cellMetaMaxKey, func(key, value []byte) (bool, error) {
		cellID, suffix, err := decodeCellMetaKey(key)
		if err != nil {
			return false, err
		}

		if suffix != cellStateSuffix {
			return true, nil
		}

		totalCount++

		localState := new(mraft.CellLocalState)
		util.MustUnmarshal(localState, value)

		for _, p := range localState.Cell.Peers {
			s.peerCache.put(p.ID, *p)
		}

		if localState.State == mraft.Tombstone {
			s.clearMeta(cellID, wb)
			tomebstoneCount++
			log.Infof("bootstrap: cell is tombstone in store, cellID=<%d>",
				cellID)
			return true, nil
		}

		pr, err := createPeerReplicate(s, &localState.Cell)
		if err != nil {
			return false, err
		}

		if localState.State == mraft.Applying {
			applyingCount++
			log.Infof("bootstrap: cell is applying in store, cellID=<%d>", cellID)
			pr.startApplyingSnapJob()
		}

		pr.startRegistrationJob()

		s.keyRanges.Update(localState.Cell)
		s.replicatesMap.put(cellID, pr)

		return true, nil
	})

	if err != nil {
		log.Fatalf("bootstrap: init store failed, errors:\n %+v", err)
	}

	err = s.engine.Write(wb)
	if err != nil {
		log.Fatalf("bootstrap: init store failed, errors:\n %+v", err)
	}

	log.Infof("bootstrap: starts with %d cells, including %d tombstones and %d applying cells",
		totalCount,
		tomebstoneCount,
		applyingCount)

	s.cleanup()
}

// Start returns the error when start store
func (s *Store) Start() {
	log.Infof("bootstrap: begin to start store %d", s.id)

	go s.startTransfer()
	<-s.trans.server.Started()
	log.Infof("bootstrap: transfer started")

	s.startHandleNotifyMsg()
	log.Infof("bootstrap: ready to handle raft message")

	s.startStoreHeartbeatTask()
	log.Infof("bootstrap: ready to handle store heartbeat")

	s.startCellHeartbeatTask()
	log.Infof("bootstrap: ready to handle cell heartbeat")

	s.startGCTask()
	log.Infof("bootstrap: ready to handle gc task")

	s.startCellSplitCheckTask()
	log.Infof("bootstrap: ready to handle split check task")

	s.startCellReportTask()
	log.Infof("bootstrap: ready to handle report cell task")
}

func (s *Store) startTransfer() {
	err := s.trans.start()
	if err != nil {
		log.Fatalf("bootstrap: start transfer failed, errors:\n %+v", err)
	}
}

func (s *Store) startHandleNotifyMsg() {
	index := 0
	for _, messageQ := range s.messages {
		q := messageQ
		id := index
		index++

		s.runner.RunCancelableTask(func(ctx context.Context) {
			for {
				ns, err := q.Get(globalCfg.RaftMessageProcessBatchLimit)
				if err != nil {
					log.Infof("stop: store raft message handler<%d> stopped", id)
					return
				}

				for _, n := range ns {
					if msg, ok := n.(*mraft.RaftMessage); ok {
						s.onRaftMessage(msg)
					} else if msg, ok := n.(*splitCheckResult); ok {
						s.onSplitCheckResult(msg)
					} else if msg, ok := n.(*mraft.SnapshotData); ok {
						err := s.snapshotManager.ReceiveSnapData(msg)
						if err != nil {
							log.Errorf("raftstore-snap[cell-%d]: received snap data failed, errors:\n%+v",
								msg.Key.CellID,
								err)
						}
					} else if msg, ok := n.(*mraft.SnapKey); ok {
						err := s.snapshotManager.CleanSnap(msg)
						if err != nil {
							log.Errorf("raftstore-snap[cell-%d]: start received snap data failed, errors:\n%+v",
								msg.CellID,
								err)
						}
					} else if msg, ok := n.(*mraft.SnapshotDataEnd); ok {
						err := s.snapshotManager.ReceiveSnapDataComplete(msg)
						if err != nil {
							log.Errorf("raftstore-snap[cell-%d]: received snap data complete failed, errors:\n%+v",
								msg.Key.CellID,
								err)
						}
					}
				}

				queueGauge.WithLabelValues(labelQueueNotify).Set(float64(q.Len()))
			}
		})
	}
}

func (s *Store) startStoreHeartbeatTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(globalCfg.getStoreHeartbeatDuration())
		defer ticker.Stop()

		var err error
		var job *util.Job

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: store heartbeat job stopped")
				return
			case <-ticker.C:
				if job != nil && job.IsNotComplete() {
					// cancel last if not complete
					job.Cancel()
				}
				job, err = s.addPDJob(s.handleStoreHeartbeat)
				if err != nil {
					log.Errorf("heartbeat-store[%d]: add job failed, errors:\n %+v",
						s.GetID(),
						err)
					job = nil
				}
			}
		}
	})
}

func (s *Store) startCellHeartbeatTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(globalCfg.getCellHeartbeatDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: cell heartbeat job stopped")
				return
			case <-ticker.C:
				s.handleCellHeartbeat()
			}
		}
	})
}

func (s *Store) startGCTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(globalCfg.getRaftGCLogDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: raft log gc job stopped")
				return
			case <-ticker.C:
				s.handleRaftGCLog()
			}
		}
	})
}

func (s *Store) startCellReportTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(globalCfg.getReportCellDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: cell report job stopped")
				return
			case <-ticker.C:
				s.handleCellReport()
			}
		}
	})
}

func (s *Store) startCellSplitCheckTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(globalCfg.getSplitCellCheckDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: split check job stopped")
				return
			case <-ticker.C:
				s.handleCellSplitCheck()
			}
		}
	})
}

// Stop returns the error when stop store
func (s *Store) Stop() error {
	for _, q := range s.messages {
		q.Dispose()
	}

	s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		pr.stopEventLoop()
		return true, nil
	})

	s.trans.stop()
	err := s.runner.Stop()
	return err
}

// GetID returns store id
func (s *Store) GetID() uint64 {
	return s.id
}

// GetMeta returns store meta
func (s *Store) GetMeta() metapb.Store {
	return s.meta
}

func (s *Store) getTargetCell(key []byte) (*PeerReplicate, error) {
	cell := s.keyRanges.Search(key)
	if cell.ID == pd.ZeroID {
		return nil, errStoreNotMatch
	}

	pr := s.replicatesMap.get(cell.ID)
	if pr == nil {
		return nil, errStoreNotMatch
	}

	return pr, nil
}

// OnProxyReq process proxy req
func (s *Store) OnProxyReq(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) error {
	key := req.Cmd[1]
	pr, err := s.getTargetCell(key)
	if err != nil {
		if err == errStoreNotMatch {
			s.respStoreNotMatch(err, req, cb)
			return nil
		}

		return err
	}

	pr.onReq(req, cb)
	return nil
}

// OnRedisCommand process redis command
func (s *Store) OnRedisCommand(cmdType raftcmdpb.CMDType, cmd redis.Command, cb func(*raftcmdpb.RaftCMDResponse)) ([]byte, error) {
	if log.DebugEnabled() {
		log.Debugf("raftstore[store-%d]: received a redis command, cmd=<%s>", s.id, cmd.ToString())
	}

	key := cmd.Args()[0]
	pr, err := s.getTargetCell(key)
	if err != nil {
		return nil, err
	}

	uuid := uuid.NewV4().Bytes()
	req := &raftcmdpb.Request{
		UUID: uuid,
		Type: cmdType,
		Cmd:  cmd,
	}

	pr.onReq(req, cb)
	return uuid, nil
}

func (s *Store) notify(n interface{}) {
	var id uint64

	if msg, ok := n.(*mraft.RaftMessage); ok {
		id = msg.CellID
	} else if msg, ok := n.(*splitCheckResult); ok {
		id = msg.cellID
	} else if msg, ok := n.(*mraft.SnapshotData); ok {
		id = msg.Key.CellID
	} else if msg, ok := n.(*mraft.SnapKey); ok {
		id = msg.CellID
	} else if msg, ok := n.(*mraft.SnapshotDataEnd); ok {
		id = msg.Key.CellID
	}

	if id == 0 {
		return
	}

	index := (globalCfg.RaftMessageWorkerCount - 1) & id
	q := s.messages[index]
	q.Put(n)
	queueGauge.WithLabelValues(labelQueueNotify).Set(float64(q.Len()))
}

func (s *Store) onRaftMessage(msg *mraft.RaftMessage) {
	if !s.isRaftMsgValid(msg) {
		return
	}

	if msg.IsTombstone {
		// we receive a message tells us to remove ourself.
		s.handleGCPeerMsg(msg)
		return
	}

	yes, err := s.isMsgStale(msg)
	if err != nil || yes {
		return
	}

	if !s.tryToCreatePeerReplicate(msg.CellID, msg) {
		log.Warnf("raftstore[store-%d]: try to create peer failed. cell=<%d>",
			s.id,
			msg.CellID)
		return
	}

	ok, err := s.checkSnapshot(msg)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	s.addPeerToCache(msg.FromPeer)

	pr := s.getPeerReplicate(msg.CellID)
	pr.step(msg.Message)
}

func (s *Store) onSplitCheckResult(result *splitCheckResult) {
	if len(result.splitKey) == 0 {
		log.Errorf("raftstore-split[cell-%d]: split key must not be empty", result.cellID)
		return
	}

	p := s.replicatesMap.get(result.cellID)
	if p == nil || !p.isLeader() {
		log.Errorf("raftstore-split[cell-%d]: cell not exist or not leader, skip", result.cellID)
		return
	}

	cell := p.getCell()

	if cell.Epoch.CellVer != result.epoch.CellVer {
		log.Infof("raftstore-split[cell-%d]: epoch changed, need re-check later, current=<%+v> split=<%+v>",
			result.cellID,
			cell.Epoch,
			result.epoch)
		return
	}

	err := p.startAskSplitJob(cell, p.peer, result.splitKey)
	if err != nil {
		log.Errorf("raftstore-split[cell-%d]: add ask split job failed, errors:\n %+v",
			result.cellID,
			err)
	}
}

func (s *Store) handleGCPeerMsg(msg *mraft.RaftMessage) {
	cellID := msg.CellID
	needRemove := false
	asyncRemoved := true

	pr := s.replicatesMap.get(cellID)
	if pr != nil {
		fromEpoch := msg.CellEpoch

		if isEpochStale(pr.getCell().Epoch, fromEpoch) {
			log.Infof("raftstore[cell-%d]: receives gc message, remove. msg=<%+v>",
				cellID,
				msg)
			needRemove = true
			asyncRemoved = pr.ps.isInitialized()
		}
	}

	if needRemove {
		s.destroyPeer(cellID, msg.ToPeer, asyncRemoved)
	}
}

func (s *Store) handleStaleMsg(msg *mraft.RaftMessage, currEpoch metapb.CellEpoch, needGC bool) {
	cellID := msg.CellID
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer

	if !needGC {
		log.Infof("raftstore[cell-%d]: raft msg is stale, ignore it, msg=<%+v> current=<%+v>",
			cellID,
			msg,
			currEpoch)
		return
	}

	log.Infof("raftstore[cell-%d]: raft msg is stale, tell to gc, msg=<%+v> current=<%+v>",
		cellID,
		msg,
		currEpoch)

	gc := new(mraft.RaftMessage)
	gc.CellID = cellID
	gc.ToPeer = fromPeer
	gc.FromPeer = toPeer
	gc.CellEpoch = currEpoch
	gc.IsTombstone = true

	s.trans.send(gc)
}

// If target peer doesn't exist, create it.
//
// return false to indicate that target peer is in invalid state or
// doesn't exist and can't be created.
func (s *Store) tryToCreatePeerReplicate(cellID uint64, msg *mraft.RaftMessage) bool {
	var (
		hasPeer     = false
		asyncRemove = false
		stalePeer   metapb.Peer
	)

	target := msg.ToPeer

	if p := s.getPeerReplicate(cellID); p != nil {
		hasPeer = true

		// we may encounter a message with larger peer id, which means
		// current peer is stale, then we should remove current peer
		if p.peer.ID < target.ID {
			// cancel snapshotting op
			if p.ps.isApplyingSnapshot() && !p.ps.cancelApplyingSnapJob() {
				log.Infof("raftstore[cell-%d]: stale peer is applying snapshot, will destroy next time, peer=<%d>",
					cellID,
					p.peer.ID)

				return false
			}

			stalePeer = p.peer
			asyncRemove = p.getStore().isInitialized()
		} else if p.peer.ID > target.ID {
			log.Infof("raftstore[cell-%d]: may be from peer is stale, targetID=<%d> currentID=<%d>",
				cellID,
				target.ID,
				p.peer.ID)
			return false
		}
	}

	// If we found stale peer, we will destory it
	if stalePeer.ID > 0 {
		s.destroyPeer(cellID, stalePeer, asyncRemove)
		if asyncRemove {
			return false
		}

		hasPeer = false
	}

	if hasPeer {
		return true
	}

	// arrive here means target peer not found, we will try to create it
	message := msg.Message
	if message.Type != raftpb.MsgVote &&
		(message.Type != raftpb.MsgHeartbeat || message.Commit != invalidIndex) {
		log.Infof("raftstore[cell-%d]: target peer doesn't exist, peer=<%+v> message=<%s>",
			cellID,
			target,
			message.Type)
		return false
	}

	// check range overlapped
	item := s.keyRanges.Search(msg.Start)
	if item.ID > 0 {
		if bytes.Compare(encStartKey(&item), getDataEndKey(msg.End)) < 0 {
			var state string
			p := s.getPeerReplicate(item.ID)
			if p != nil {
				state = fmt.Sprintf("local=<%s> apply=<%s>", p.ps.raftState.String(),
					p.ps.applyState.String())
			}

			log.Infof("raftstore[cell-%d]: msg is overlapped with cell, cell=<%s> msg=<%s> state=<%s>",
				cellID,
				item.String(),
				msg.String(),
				state)
			return false
		}
	}

	// now we can create a replicate
	peerReplicate, err := doReplicate(s, msg, target.ID)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: replicate peer failure, errors:\n %+v",
			cellID,
			err)
		return false
	}

	peerReplicate.ps.cell.Peers = append(peerReplicate.ps.cell.Peers, &msg.ToPeer)
	peerReplicate.ps.cell.Peers = append(peerReplicate.ps.cell.Peers, &msg.FromPeer)
	s.keyRanges.Update(peerReplicate.ps.cell)

	// following snapshot may overlap, should insert into keyRanges after
	// snapshot is applied.
	s.replicatesMap.put(cellID, peerReplicate)
	s.addPeerToCache(msg.FromPeer)
	s.addPeerToCache(msg.ToPeer)
	return true
}

func (s *Store) destroyPeer(cellID uint64, target metapb.Peer, async bool) {
	if !async {
		log.Infof("raftstore[cell-%d]: destroying stale peer, peer=<%v>",
			cellID,
			target)

		pr := s.replicatesMap.delete(cellID)
		if pr == nil {
			log.Fatalf("raftstore[cell-%d]: destroy cell not exist", cellID)
		}

		if pr.ps.isApplyingSnap() {
			log.Fatalf("raftstore[cell-%d]: destroy cell is apply for snapshot", cellID)
		}

		err := pr.destroy()
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: destroy cell failed, errors:\n %+v",
				cellID,
				err)
		}

		if pr.ps.isInitialized() && !s.keyRanges.Remove(pr.getCell()) {
			log.Fatalf("raftstore[cell-%d]: remove key range  failed",
				cellID)
		}
	} else {
		log.Infof("raftstore[cell-%d]: asking destroying stale peer, peer=<%v>",
			cellID,
			target)

		s.startDestroyJob(cellID, target)
	}
}

func (s *Store) cleanup() {
	// clean up all possible garbage data
	lastStartKey := getDataKey([]byte(""))

	s.keyRanges.Ascend(func(cell *metapb.Cell) bool {
		start := encStartKey(cell)
		err := s.getDataEngine().RangeDelete(lastStartKey, start)
		if err != nil {
			log.Fatalf("bootstrap: cleanup possible garbage data failed, start=<%v> end=<%v> errors:\n %+v",
				lastStartKey,
				start,
				err)
		}

		lastStartKey = encEndKey(cell)
		return true
	})

	err := s.getDataEngine().RangeDelete(lastStartKey, dataMaxKey)
	if err != nil {
		log.Fatalf("bootstrap: cleanup possible garbage data failed, start=<%v> end=<%v> errors:\n %+v",
			lastStartKey,
			dataMaxKey,
			err)
	}

	log.Infof("bootstrap: cleanup possible garbage data complete")
}

func (s *Store) clearMeta(cellID uint64, wb storage.WriteBatch) error {
	metaCount := 0
	raftCount := 0

	// meta must in the range [cellID, cellID + 1)
	metaStart := getCellMetaPrefix(cellID)
	metaEnd := getCellMetaPrefix(cellID + 1)

	err := s.getMetaEngine().Scan(metaStart, metaEnd, func(key, value []byte) (bool, error) {
		err := wb.Delete(key)
		if err != nil {
			return false, errors.Wrapf(err, "")
		}

		metaCount++
		return true, nil
	})

	if err != nil {
		return errors.Wrapf(err, "")
	}

	raftStart := getCellRaftPrefix(cellID)
	raftEnd := getCellRaftPrefix(cellID + 1)

	err = s.getMetaEngine().Scan(raftStart, raftEnd, func(key, value []byte) (bool, error) {
		err := wb.Delete(key)
		if err != nil {
			return false, errors.Wrapf(err, "")
		}

		raftCount++
		return true, nil
	})

	if err != nil {
		return errors.Wrapf(err, "")
	}

	log.Infof("raftstore[cell-%d]: clear peer meta keys and raft keys, meta key count=<%d>, raft key count=<%d>",
		cellID,
		metaCount,
		raftCount)

	return nil
}

func (s *Store) getPeerReplicate(cellID uint64) *PeerReplicate {
	return s.replicatesMap.get(cellID)
}

func (s *Store) addPeerToCache(peer metapb.Peer) {
	s.peerCache.put(peer.ID, peer)
}

func (s *Store) addPDJob(task func() error) (*util.Job, error) {
	return s.addNamedJob("", pdWorker, task)
}

func (s *Store) addRaftLogGCJob(task func() error) (*util.Job, error) {
	return s.addNamedJob("", raftGCWorker, task)
}

func (s *Store) addSnapJob(task func() error, cb func(*util.Job)) (*util.Job, error) {
	return s.addNamedJobWithCB("", snapWorker, task, cb)
}

func (s *Store) addApplyJob(cellID uint64, desc string, task func() error, cb func(*util.Job)) (*util.Job, error) {
	index := (globalCfg.ApplyWorkerCount - 1) & cellID
	return s.addNamedJobWithCB(desc, fmt.Sprintf(applyWorker, index), task, cb)
}

func (s *Store) addSplitJob(task func() error) (*util.Job, error) {
	return s.addNamedJob("", splitWorker, task)
}

func (s *Store) addNamedJob(desc, worker string, task func() error) (*util.Job, error) {
	return s.runner.RunJobWithNamedWorker(desc, worker, task)
}

func (s *Store) addNamedJobWithCB(desc, worker string, task func() error, cb func(*util.Job)) (*util.Job, error) {
	return s.runner.RunJobWithNamedWorkerWithCB(desc, worker, task, cb)
}

func (s *Store) handleStoreHeartbeat() error {
	stats, err := util.DiskStats(globalCfg.StoreDataPath)
	if err != nil {
		log.Errorf("heartbeat-store[%d]: handle store heartbeat failed, errors:\n %+v",
			s.GetID(),
			err)
		return err
	}

	applySnapCount, err := s.getApplySnapshotCount()
	if err != nil {
		log.Errorf("heartbeat-store[%d]: handle store heartbeat failed, errors:\n %+v",
			s.GetID(),
			err)
		return err
	}

	req := new(pdpb.StoreHeartbeatReq)
	req.Header.ClusterID = s.clusterID
	req.Stats = &pdpb.StoreStats{
		StoreID:            s.GetID(),
		StartTime:          s.startAt,
		Capacity:           stats.Total,
		Available:          stats.Free,
		UsedSize:           stats.Used,
		CellCount:          s.replicatesMap.size(),
		SendingSnapCount:   s.sendingSnapCount,
		ReceivingSnapCount: s.reveivingSnapCount,
		ApplyingSnapCount:  applySnapCount,
		IsBusy:             false,
		BytesWritten:       0,
		LogLevel:           int32(log.GetLogLevel()),
	}

	storeStorageGaugeVec.WithLabelValues(labelStoreStorageCapacity).Set(float64(stats.Total))
	storeStorageGaugeVec.WithLabelValues(labelStoreStorageAvailable).Set(float64(stats.Free))

	snapshortActionGaugeVec.WithLabelValues(labelSnapshotActionSent).Set(float64(s.sendingSnapCount))
	snapshortActionGaugeVec.WithLabelValues(labelSnapshotActionReceived).Set(float64(s.reveivingSnapCount))
	snapshortActionGaugeVec.WithLabelValues(labelSnapshotActionApplying).Set(float64(applySnapCount))

	rsp, err := s.pdClient.StoreHeartbeat(context.TODO(), req)
	if err != nil {
		log.Errorf("heartbeat-store[%d]: handle store heartbeat failed, errors:\n %+v",
			s.GetID(),
			err)
	}

	if rsp.SetLogLevel != nil {
		log.Infof("heartbeat-store[%d]: log level changed to: %d",
			s.GetID(),
			rsp.SetLogLevel.NewLevel)
		log.SetLevel(log.Level(rsp.SetLogLevel.NewLevel))
	}

	return err
}

func (s *Store) getApplySnapshotCount() (uint32, error) {
	var cnt uint32
	err := s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if pr.ps.isApplyingSnap() {
			cnt++
		}
		return true, nil
	})

	return cnt, err
}

func (s *Store) handleCellHeartbeat() {
	for _, p := range s.replicatesMap.values() {
		p.checkPeers()
	}

	leaders := 0
	for _, p := range s.replicatesMap.values() {
		if p.isLeader() {
			p.handleHeartbeat()
			leaders++
		}
	}

	storeCellCountGaugeVec.WithLabelValues(labelCommandAdminPerAll).Set(float64(s.replicatesMap.size()))
	storeCellCountGaugeVec.WithLabelValues(labelStoreCellLeader).Set(float64(leaders))
}

func (s *Store) handleCellSplitCheck() {
	if s.runner.IsNamedWorkerBusy(splitWorker) {
		return
	}

	s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if !pr.isLeader() {
			return true, nil
		}

		if pr.sizeDiffHint < globalCfg.CellCheckSizeDiff {
			return true, nil
		}

		log.Debugf("raftstore-split[cell-%d]: cell need to check whether should split, diff=<%d> max=<%d>",
			pr.cellID,
			pr.sizeDiffHint,
			globalCfg.CellCheckSizeDiff)

		err := pr.startSplitCheckJob()
		if err != nil {
			log.Errorf("raftstore-split[cell-%d]: add split check job failed, errors:\n %+v",
				pr.cellID,
				err)
			return false, err
		}

		pr.sizeDiffHint = 0
		return true, nil
	})
}

func (s *Store) handleCellReport() {
	s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if !pr.isLeader() {
			pr.writtenBytes = 0
			pr.writtenKeys = 0
			return true, nil
		}

		storeWrittenBytesHistogram.Observe(float64(pr.writtenBytes))
		storeWrittenKeysHistogram.Observe(float64(pr.writtenKeys))

		pr.writtenBytes = 0
		pr.writtenKeys = 0
		return true, nil
	})
}

func (s *Store) handleRaftGCLog() {
	var gcLogCount uint64

	s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if !pr.isLeader() {
			return true, nil
		}

		// Leader will replicate the compact log command to followers,
		// If we use current replicated_index (like 10) as the compact index,
		// when we replicate this log, the newest replicated_index will be 11,
		// but we only compact the log to 10, not 11, at that time,
		// the first index is 10, and replicated_index is 11, with an extra log,
		// and we will do compact again with compact index 11, in cycles...
		// So we introduce a threshold, if replicated index - first index > threshold,
		// we will try to compact log.
		// raft log entries[..............................................]
		//                  ^                                       ^
		//                  |-----------------threshold------------ |
		//              first_index                         replicated_index

		var replicatedIdx uint64
		for _, p := range pr.rn.Status().Progress {
			if replicatedIdx == 0 {
				replicatedIdx = p.Match
			}

			if p.Match < replicatedIdx {
				replicatedIdx = p.Match
			}
		}

		// When an election happened or a new peer is added, replicated_idx can be 0.
		if replicatedIdx > 0 {
			lastIdx := pr.rn.LastIndex()
			if lastIdx < replicatedIdx {
				log.Fatalf("raft-log-gc: expect last index >= replicated index, last=<%d> replicated=<%d>",
					lastIdx,
					replicatedIdx)
			}

			raftLogLagHistogram.Observe(float64(lastIdx - replicatedIdx))
		}

		var compactIdx uint64
		appliedIdx := pr.ps.getAppliedIndex()
		firstIdx, _ := pr.ps.FirstIndex()

		if appliedIdx > firstIdx &&
			appliedIdx-firstIdx >= globalCfg.RaftLogGCCountLimit {
			compactIdx = appliedIdx
		} else if pr.sizeDiffHint >= globalCfg.RaftLogGCSizeLimit {
			compactIdx = appliedIdx
		} else if replicatedIdx < firstIdx ||
			replicatedIdx-firstIdx <= globalCfg.RaftLogGCThreshold {
			return true, nil
		} else {
			compactIdx = replicatedIdx
		}

		// Have no idea why subtract 1 here, but original code did this by magic.
		if compactIdx == 0 {
			log.Fatal("raft-log-gc: unexpect compactIdx")
		}

		compactIdx--
		if compactIdx < firstIdx {
			// In case compactIdx == firstIdx before subtraction.
			return true, nil
		}

		gcLogCount += compactIdx - firstIdx

		term, _ := pr.rn.Term(compactIdx)

		pr.onAdminRequest(newCompactLogRequest(compactIdx, term))

		return true, nil
	})

	if gcLogCount > 0 {
		raftLogCompactCounter.Add(float64(gcLogCount))
	}
}

func (s *Store) getMetaEngine() storage.Engine {
	return s.engine.GetEngine()
}

func (s *Store) getDataEngine() storage.DataEngine {
	return s.engine.GetDataEngine()
}

func (s *Store) getKVEngine() storage.KVEngine {
	return s.engine.GetKVEngine()
}

func (s *Store) getHashEngine() storage.HashEngine {
	return s.engine.GetHashEngine()
}

func (s *Store) getListEngine() storage.ListEngine {
	return s.engine.GetListEngine()
}

func (s *Store) getSetEngine() storage.SetEngine {
	return s.engine.GetSetEngine()
}

func (s *Store) getZSetEngine() storage.ZSetEngine {
	return s.engine.GetZSetEngine()
}
