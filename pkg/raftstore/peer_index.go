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
	"os"
	"path/filepath"
	"reflect"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/indexer"
	"github.com/deepfabric/indexer/cql"
	"golang.org/x/net/context"
)

func (pr *PeerReplicate) loadIndices() (err error) {
	indexerConf := &indexer.Conf{
		T0mCap:   pr.store.cfg.Index.T0mCap,
		LeafCap:  pr.store.cfg.Index.LeafCap,
		IntraCap: pr.store.cfg.Index.IntraCap,
	}
	indicesDir := filepath.Join(pr.store.cfg.Index.IndexDataPath, fmt.Sprintf("%d", pr.cellID))
	pr.indexer, err = indexer.NewIndexer(indicesDir, indexerConf, false)
	if err != nil {
		return
	}
	indicesFp := filepath.Join(indicesDir, "indices.json")
	err = util.FileUnmarshal(indicesFp, &pr.indices)
	if os.IsNotExist(err) {
		err = pr.persistIndices(pr.indices)
	}
	return
}

func (pr *PeerReplicate) persistIndices(indices map[string]*pdpb.IndexDef) (err error) {
	indicesFp := filepath.Join(pr.store.cfg.Index.IndexDataPath, fmt.Sprintf("%d", pr.cellID), "indices.json")
	if err = util.FileMarshal(indicesFp, indices); err != nil {
		log.Errorf("raftstore[cell-%d]: failed to persist indices definion\n%v",
			pr.cellID, err)
	}
	return
}

func (pr *PeerReplicate) readyToServeIndex(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(pr.indicesC)
			log.Infof("raftstore[cell-%d]: handle serve query stopped",
				pr.cellID)
			return
		case indicesNew := <-pr.indicesC:
			delta := diffIndices(pr.indices, indicesNew)
			for _, idxDef := range delta.toDelete {
				//deletion shall be idempotent
				if err := pr.indexer.DestroyIndex(idxDef.GetName()); err != nil {
					log.Errorf("raftstore[cell-%d]: failed to delete index %s\n%v",
						pr.cellID, idxDef.GetName(), err)
				} else {
					log.Infof("raftstore[cell-%d]: deleted index %v", pr.cellID, idxDef)
				}
			}
			for _, idxDef := range delta.toCreate {
				//creation shall be idempotent
				docProt := convertToDocProt(idxDef)
				if err := pr.indexer.CreateIndex(docProt); err != nil {
					log.Errorf("raftstore[cell-%d]: failed to create index %s\n%v",
						pr.cellID, idxDef.GetName(), err)
				} else {
					log.Infof("raftstore[cell-%d]: created index %v", pr.cellID, idxDef)
				}
			}
			if len(delta.toDelete) != 0 || len(delta.toCreate) != 0 {
				if err := pr.persistIndices(indicesNew); err != nil {
					return
				}
				pr.indices = indicesNew
			}
		}
	}
}

func convertToDocProt(idxDef *pdpb.IndexDef) (docProt *cql.DocumentWithIdx) {
	docProt = &cql.DocumentWithIdx{
		Document: cql.Document{
			DocID:     0,
			UintProps: make([]cql.UintProp, 0),
			StrProps:  make([]cql.StrProp, 0),
		},
		Index: idxDef.GetName(),
	}
	for _, f := range idxDef.Fields {
		switch f.GetType() {
		case pdpb.Uint8:
			docProt.Document.UintProps = append(docProt.Document.UintProps, cql.UintProp{Name: f.GetName(), ValLen: 1})
		case pdpb.Uint16:
			docProt.Document.UintProps = append(docProt.Document.UintProps, cql.UintProp{Name: f.GetName(), ValLen: 2})
		case pdpb.Uint32:
			docProt.Document.UintProps = append(docProt.Document.UintProps, cql.UintProp{Name: f.GetName(), ValLen: 4})
		case pdpb.Uint64:
			docProt.Document.UintProps = append(docProt.Document.UintProps, cql.UintProp{Name: f.GetName(), ValLen: 8})
		case pdpb.Text:
			docProt.Document.StrProps = append(docProt.Document.StrProps, cql.StrProp{Name: f.GetName()})
		default:
			log.Errorf("invalid filed type %v of idxDef %v", f.GetType().String(), idxDef)
			continue
		}
	}
	return
}

type IndicesDiff struct {
	toDelete []*pdpb.IndexDef
	toCreate []*pdpb.IndexDef
}

//detect difference of indices and indicesNew
func diffIndices(indices, indicesNew map[string]*pdpb.IndexDef) (delta *IndicesDiff) {
	delta = &IndicesDiff{
		toDelete: make([]*pdpb.IndexDef, 0),
		toCreate: make([]*pdpb.IndexDef, 0),
	}
	var ok bool
	var name string
	var idxDefCur, idxDefNew *pdpb.IndexDef
	for name, idxDefCur = range indices {
		if idxDefNew, ok = indicesNew[name]; !ok {
			delta.toDelete = append(delta.toDelete, idxDefCur)
		} else if !reflect.DeepEqual(idxDefCur, idxDefNew) {
			delta.toDelete = append(delta.toDelete, idxDefCur)
			delta.toCreate = append(delta.toCreate, idxDefNew)
		}
	}
	for _, idxDefNew = range indicesNew {
		if _, ok := indices[idxDefNew.GetName()]; !ok {
			delta.toCreate = append(delta.toCreate, idxDefNew)
		}
	}
	return
}
