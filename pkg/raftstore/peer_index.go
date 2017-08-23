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
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/indexer/cql"
	"golang.org/x/net/context"
)

func (pr *PeerReplicate) readyToServeIndex(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(pr.createIndexC)
			close(pr.deleteIndexC)
			log.Infof("raftstore[cell-%d]: handle serve query stopped",
				pr.cellID)
			return
		case toCreate := <-pr.createIndexC:
			for _, idxDef := range toCreate {
				//creation shall be idempotent
				docProt := pr.indexer.GetDocProt(idxDef.GetName())
				if docProt != nil {
					isSame := isSameScheme(idxDef, docProt)
					if isSame {
						log.Infof("raftstore[cell-%d]: skipped creating index %s", pr.cellID, idxDef.GetName())
						continue
					} else if err := pr.indexer.DestroyIndex(idxDef.GetName()); err != nil {
						log.Errorf("raftstore[cell-%d]: failed to destroy conflicting index %s\n%v",
							pr.cellID, idxDef.GetName(), err)
						continue
					} else {
						log.Infof("raftstore[cell-%d]: destroyed conflicting index %s", pr.cellID, idxDef.GetName())
					}
				}
				docProt = convertToDocProt(idxDef)
				if err := pr.indexer.CreateIndex(docProt); err != nil {
					log.Errorf("raftstore[cell-%d]: failed to create index %s\n%v",
						pr.cellID, idxDef.GetName(), err)
				} else {
					log.Infof("raftstore[cell-%d]: created index %v", pr.cellID, idxDef)
				}
			}
		case toDelete := <-pr.deleteIndexC:
			for _, idxDef := range toDelete {
				//deletion shall be idempotent
				if err := pr.indexer.DestroyIndex(idxDef.GetName()); err != nil {
					log.Errorf("raftstore[cell-%d]: failed to destroy index %s\n%v",
						pr.cellID, idxDef.GetName(), err)
				} else {
					log.Infof("raftstore[cell-%d]: deleted index %v", pr.cellID, idxDef)
				}
			}
		}
	}
}

func isSameScheme(idxDef *pdpb.IndexDef, docProt *cql.DocumentWithIdx) bool {
	types1 := make(map[string]pdpb.FieldType)
	types2 := make(map[string]pdpb.FieldType)
	for _, f := range idxDef.Fields {
		types1[f.GetName()] = f.GetType()
	}
	for _, f := range docProt.UintProps {
		var t pdpb.FieldType
		switch f.ValLen {
		case 1:
			t = pdpb.Uint8
		case 2:
			t = pdpb.Uint16
		case 4:
			t = pdpb.Uint32
		case 8:
			t = pdpb.Uint64
		default:
			log.Errorf("invalid ValLen %d of docProt %v", f.ValLen, docProt)
			return true
		}
		types2[f.Name] = t
	}
	for _, f := range docProt.StrProps {
		types2[f.Name] = pdpb.Text
	}
	if len(types1) != len(types2) {
		return false
	}
	for k, v1 := range types1 {
		if v2, ok := types2[k]; !ok || v1 != v2 {
			return false
		}
	}

	return true
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
