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

package pdapi

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/pkg/errors"
)

// SetLogLevel set log level for components
type SetLogLevel struct {
	Targets []uint64 `json:"targets"`
	Level   int32    `json:"level"`
}

// StoreStatus store status
type StoreStatus struct {
	Stats           *pdpb.StoreStats `json:"stats"`
	LeaderCount     uint32           `json:"leaderCount"`
	LastHeartbeatTS int64            `json:"lastHeartbeatTS"`
}

// StoreInfo store info
type StoreInfo struct {
	Meta   metapb.Store `json:"meta"`
	Status *StoreStatus `json:"status"`
}

// CellInfo The cell info
type CellInfo struct {
	Meta         metapb.Cell      `json:"meta"`
	LeaderPeer   *metapb.Peer     `json:"leader"`
	DownPeers    []pdpb.PeerStats `json:"downPeers"`
	PendingPeers []metapb.Peer    `json:"pendingPeers"`
}

// System The system info of the elasticell cluster
type System struct {
	MaxReplicas uint32 `json:"maxReplicas"`

	StoreCount          int `json:"storeCount"`
	OfflineStoreCount   int `json:"offlineStoreCount"`
	TombStoneStoreCount int `json:"tombStoneStoreCount"`

	CellCount                int `json:"cellCount"`
	ReplicasNotFullCellCount int `json:"replicasNotFullCellCount"`

	StorageCapacity  uint64 `json:"storageCapacity"`
	StorageAvailable uint64 `json:"storageAvailable"`

	OperatorCount int `json:"operatorCount"`
}

// Service service interface
type Service interface {
	Name() string
	IsLeader() bool
	GetLeader() (*pdpb.Leader, error)

	GetSystem() (*System, error)

	ListStore() ([]*StoreInfo, error)
	GetStore(id uint64) (*StoreInfo, error)
	DeleteStore(id uint64, force bool) error
	SetStoreLogLevel(set *SetLogLevel) error

	ListCellInStore(storeID uint64) ([]*CellInfo, error)
	ListCell() ([]*CellInfo, error)
	GetCell(id uint64) (*CellInfo, error)
	TransferLeader(transfer *TransferLeader) error
	GetOperator(id uint64) (interface{}, error)

	GetOperators() ([]interface{}, error)
}

// TransferLeader transfer leader to spec peer
type TransferLeader struct {
	CellID   uint64 `json:"cellId"`
	ToPeerID uint64 `json:"toPeerId"`
}

func readTransferLeader(r io.ReadCloser) (*TransferLeader, error) {
	value := &TransferLeader{}
	return value, readJSON(r, value)
}

func readSetLogLevel(r io.ReadCloser) (*SetLogLevel, error) {
	value := &SetLogLevel{}
	return value, readJSON(r, value)
}

func readJSON(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
