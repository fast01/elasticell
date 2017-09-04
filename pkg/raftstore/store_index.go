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
	"path/filepath"
	"reflect"
	"regexp"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/indexer/cql"
	"golang.org/x/net/context"
)

func (s *Store) loadIndices() (err error) {
	indicesFp := filepath.Join(s.cfg.Index.IndexDataPath, "indices.json")
	if err = util.FileUnmarshal(indicesFp, s.indices); err != nil {
		log.Errorf("index-store[%d]: failed to persist indices definion\n%v", s.GetID(), err)
	}
	return
}

func (s *Store) persistIndices() (err error) {
	indicesFp := filepath.Join(s.cfg.Index.IndexDataPath, "indices.json")
	if err = util.FileMarshal(indicesFp, s.indices); err != nil {
		log.Errorf("index-store[%d]: failed to persist indices definion\n%v", s.GetID(), err)
	}
	return
}

func (s *Store) handleIndicesChange(rspIndices []*pdpb.IndexDef) (err error) {
	indicesNew := make(map[string]*pdpb.IndexDef)
	for _, idxDefNew := range rspIndices {
		indicesNew[idxDefNew.GetName()] = idxDefNew
	}
	s.rwlock.RLock()
	if reflect.DeepEqual(s.indices, indicesNew) {
		s.rwlock.RUnlock()
		return
	}
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	reExpsNew := make(map[string]*regexp.Regexp)
	for _, idxDefNew := range rspIndices {
		reExpsNew[idxDefNew.GetName()] = regexp.MustCompile(idxDefNew.GetKeyPattern())
	}
	s.reExps = reExpsNew
	delta := diffIndices(s.indices, indicesNew)
	for _, idxDef := range delta.toDelete {
		//deletion shall be idempotent
		err = s.replicatesMap.foreach(func(pr *PeerReplicate) (cont bool, err error) {
			if err = pr.indexer.DestroyIndex(idxDef.GetName()); err != nil {
				cont = false
			}
			return
		})

		if err != nil {
			log.Errorf("index-store[%d]: failed to delete index\n%v", s.GetID(), err)
			return
		}
		log.Infof("index-store[%d]: deleted index %v", s.GetID(), idxDef)
	}
	for _, idxDef := range delta.toCreate {
		//creation shall be idempotent
		docProt := convertToDocProt(idxDef)
		err = s.replicatesMap.foreach(func(pr *PeerReplicate) (cont bool, err error) {
			if err = pr.indexer.CreateIndex(docProt); err != nil {
				cont = false
			}
			return
		})

		if err != nil {
			log.Errorf("index-store[%d]: failed to create index\n%v", s.GetID(), err)
			return
		}
		log.Infof("index-store[%d]: created index %v", s.GetID(), idxDef)
	}
	if err = s.persistIndices(); err != nil {
		return
	}
	s.indices = indicesNew
	return
}

func (s *Store) readyToServeIndex(ctx context.Context) {
	listEng := s.engine.GetListEngine()
	idxReqQueueKey := getIdxReqQueueKey()
	tickChan := time.Tick(500 * time.Microsecond)
	idxReq := &pdpb.IndexRequest{}
	var idxKeyReq *pdpb.IndexKeyRequest
	var idxSplitReq *pdpb.IndexSplitRequest
	var idxReqB []byte
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Infof("index-store[%d]: handle serve query stopped", s.GetID())
			return
		case <-tickChan:
			for {
				idxReqB, err = listEng.LIndex(idxReqQueueKey, 0)
				if err != nil {
					//queue is empty
					break
				}
				if err = idxReq.Unmarshal(idxReqB); err != nil {
					log.Errorf("index-store[%d]: failed to decode IndexRequest\n%v", s.GetID(), err)
					continue
				}
				if idxKeyReq = idxReq.GetIdxKey(); idxKeyReq != nil {
					if pr := s.getPeerReplicate(idxKeyReq.CellID); pr == nil {
						//TODO(yzc): add metric?
					} else {
						if idxKeyReq.IsDel {
							for _, key := range idxKeyReq.GetUserKeys() {
								if err = s.deleteIndexedKey(idxKeyReq.GetIdxName(), key); err != nil {
									log.Errorf("index-store[%d]: failed to delete indexed key %v from index %s\n%v",
										s.GetID(), key, idxKeyReq.GetIdxName(), err)
									continue
								}
							}
						} else {
							for _, key := range idxKeyReq.GetUserKeys() {
								if err = s.deleteIndexedKey(idxKeyReq.GetIdxName(), key); err != nil {
									log.Errorf("index-store[%d]: failed to delete indexed key %v from index %s\n%v",
										s.GetID(), key, idxKeyReq.GetIdxName(), err)
									continue
								}
								doc := cnemoGetAll(key) //TODO(yzc): new cnemo API
								if err = pr.indexer.Insert(doc); err != nil {
									log.Errorf("index-store[%d]: failed to add key %s to index %s\n%v", s.GetID(), key, idxKeyReq.GetIdxName(), err)
								}
							}
						}
					}
				}
				if idxSplitReq = idxReq.GetIdxSplit(); idxSplitReq != nil {

				}
				if _, err = listEng.LPop(idxReqQueueKey, 0); err != nil {
					log.Errorf("index-store[%d]: failed to lpop idxReqUeueu\n%v", s.GetID(), err)
					break
				}
			}
		}
	}
}

func (s *Store) deleteIndexedKey(idxNameIn, key string) (err error) {
	idxName, docID := cnemoGetMetaVal(key) //TODO(yzc): new cnemo API
	if idxName != idxNameIn {
		//TODO(yzc): add metric?
	}
	if idxName == "" || docID == 0 {
		return
	}
	if _, err = pr.indexer.Del(idxName, docID); err != nil {
		log.Errorf("index-store[%d]: failed to decode IndexRequest\n%v", s.GetID(), err)
	}
	if err = ctx.wb.Delete(getDocIDKey(docID)); err != nil {
		log.Errorf("raftstore-apply[cell-%d]: failed to delete docID %v from storage\n%v",
			d.cell.ID, docID, err)
		return
	}
	return
}

func (s *Store) splitIndices(cellID int64) (err error) {
	cell := newPR.getCell()
	pr.store.engine.GetDataEngine().Scan(cell.GetStart(), cell.GetEnd(), func(key, metaVal []byte) {
		idxName, docID := metaVal //parse metaVal
		if idxName != "" {
			docID, existing_field_value_slice, found := cnemoHgetallExt(key) //TODO(yzc): new cnemo API
			if _, err = pr.indexer.Del(idxName, docID); err != nil {
				return
			}
			//TODO(yzc): idxName could be non-exist at newPR?
			if err = newPR.indexer.Insert(idxName, docID, existing_field_value_slice); err != nil {
				return
			}
		}

	})
	return
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
