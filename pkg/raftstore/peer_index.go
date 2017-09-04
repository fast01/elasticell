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
	"path/filepath"

	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/indexer"
	"golang.org/x/net/context"
)

func (pr *PeerReplicate) allocateDocID() (docID uint64, err error) {
	if pr.nextDocID&0xffff == 0 {
		var rsp *pdpb.AllocIDRsp
		rsp, err = pr.store.pdClient.AllocID(context.TODO(), new(pdpb.AllocIDReq))
		if err != nil {
			return
		}
		pr.nextDocID = rsp.GetID() << 16
	}
	docID = pr.nextDocID
	pr.nextDocID++
	return
}

func (pr *PeerReplicate) loadIndices() (err error) {
	indexerConf := &indexer.Conf{
		T0mCap:   pr.store.cfg.Index.T0mCap,
		LeafCap:  pr.store.cfg.Index.LeafCap,
		IntraCap: pr.store.cfg.Index.IntraCap,
	}
	indicesDir := filepath.Join(pr.store.cfg.Index.IndexDataPath, fmt.Sprintf("%d", pr.cellID))
	pr.indexer, err = indexer.NewIndexer(indicesDir, indexerConf, false)
	return
}
