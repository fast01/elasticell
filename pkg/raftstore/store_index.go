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
	"strconv"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/indexer/cql"
	"golang.org/x/net/context"
)

func (s *Store) loadIndices() (err error) {
	indicesFp := filepath.Join(globalCfg.Index.IndexDataPath, "indices.json")
	if err = util.FileUnmarshal(indicesFp, s.indices); err != nil {
		log.Errorf("index-store[%d]: failed to persist indices definion\n%v", s.GetID(), err)
	}
	return
}

func (s *Store) persistIndices() (err error) {
	indicesFp := filepath.Join(globalCfg.Index.IndexDataPath, "indices.json")
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
	wb := s.engine.NewWriteBatch()
	var idxKeyReq *pdpb.IndexKeyRequest
	var idxSplitReq *pdpb.IndexSplitRequest
	var idxReqB []byte
	var pr *PeerReplicate
	var numProcessed int
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Infof("index-store[%d]: handle serve query stopped", s.GetID())
			return
		case <-tickChan:
			for {
				//TODO(yzc): add support for force indexing a cell?
				idxReqB, err = listEng.LPop(idxReqQueueKey)
				if err != nil {
					log.Errorf("index-store[%d]: failed to LPop idxReqQueueKey\n%+v", s.GetID(), err)
					continue
				} else if idxReqB == nil {
					// queue is empty
					break
				}
				if err = idxReq.Unmarshal(idxReqB); err != nil {
					log.Errorf("index-store[%d]: failed to decode IndexRequest\n%+v", s.GetID(), err)
					continue
				}
				if idxKeyReq = idxReq.GetIdxKey(); idxKeyReq != nil {
					if pr = s.getPeerReplicate(idxKeyReq.CellID); pr == nil {
						//TODO(yzc): add metric?
						continue
					}
					for _, key := range idxKeyReq.GetUserKeys() {
						if err = s.deleteIndexedKey(pr, idxKeyReq.GetIdxName(), key, wb); err != nil {
							log.Errorf("index-store[%d]: failed to delete indexed key %v from index %s\n%+v",
								s.GetID(), key, idxKeyReq.GetIdxName(), err)
							continue
						}
						if !idxKeyReq.IsDel {
							if err = s.addIndexedKey(pr, idxKeyReq.GetIdxName(), 0, key, wb); err != nil {
								log.Errorf("index-store[%d]: failed to add key %s to index %s\n%+v", s.GetID(), key, idxKeyReq.GetIdxName(), err)
							}
						}
					}
					numProcessed++
				}
				if idxSplitReq = idxReq.GetIdxSplit(); idxSplitReq != nil {
					if err = s.splitIndices(idxSplitReq, wb); err != nil {
						log.Errorf("index-store[%d]: failed to handle split %v\n%+v", s.GetID(), idxSplitReq, err)
					}
					numProcessed++
				}
			}
			if numProcessed != 0 {
				s.engine.Write(wb)
				wb = s.engine.NewWriteBatch()
				numProcessed = 0
			}
		}
	}
}

func (s *Store) deleteIndexedKey(pr *PeerReplicate, idxNameIn string, key []byte, wb storage.WriteBatch) (err error) {
	var metaValB []byte
	dataKey := getDataKey(key)
	metaValB, err = s.engine.GetDataEngine().GetMetaVal(dataKey)
	if err != nil {
		return
	}
	if metaValB == nil {
		//document is inserted before index creation
		//TODO(yzc): add metric?
		return
	}
	metaVal := &pdpb.KeyMetaVal{}
	if err = metaVal.Unmarshal(metaValB); err != nil {
		return
	}
	idxName, docID := metaVal.GetIdxName(), metaVal.GetDocID()
	if idxName != idxNameIn {
		//TODO(yzc): understand how this happen. Could be inserting data during changing an index' keyPattern?
		//TODO(yzc): add metric?
	}
	if _, err = pr.indexer.Del(idxName, docID); err != nil {
		return
	}
	if err = wb.Delete(getDocIDKey(docID)); err != nil {
		return
	}
	return
}

func (s *Store) addIndexedKey(pr *PeerReplicate, idxNameIn string, docID uint64, key []byte, wb storage.WriteBatch) (err error) {
	var metaVal *pdpb.KeyMetaVal
	var metaValB []byte
	var pairs []*raftcmdpb.FVPair
	var idxDef *pdpb.IndexDef
	var doc *cql.DocumentWithIdx
	var ok bool

	if idxDef, ok = s.indices[idxNameIn]; !ok {
		//TODO(yzc): add metric?
		return
	}

	dataKey := getDataKey(key)
	hashEng := s.engine.GetHashEngine()
	if pairs, err = hashEng.HGetAll(dataKey); err != nil {
		return
	}

	if docID == 0 {
		//TODO(yzc): allocate docID
		if err = wb.Set(getDocIDKey(docID), key); err != nil {
			return
		}
		metaVal = &pdpb.KeyMetaVal{
			IdxName: idxNameIn,
			DocID:   docID,
		}
		if metaValB, err = metaVal.Marshal(); err != nil {
			return
		}
		if err = s.engine.GetDataEngine().SetMetaVal(dataKey, metaValB); err != nil {
			return
		}
	}

	if doc, err = convertToDocument(idxDef, docID, pairs); err != nil {
		return
	}
	if err = pr.indexer.Insert(doc); err != nil {
		return
	}
	return
}

func (s *Store) splitIndices(idxSplitReq *pdpb.IndexSplitRequest, wb storage.WriteBatch) (err error) {
	var pr, newPR *PeerReplicate
	if pr = s.getPeerReplicate(idxSplitReq.LeftCellID); pr == nil {
		//TODO(yzc): add metric?
		return
	}
	if newPR = s.getPeerReplicate(idxSplitReq.RightCellID); pr == nil {
		//TODO(yzc): add metric?
		return
	}
	s.engine.GetDataEngine().Scan(idxSplitReq.RightStart, idxSplitReq.RightEnd, func(dataKey, metaValB []byte) (err error) {
		key := getOriginKey(dataKey)
		metaVal := &pdpb.KeyMetaVal{}
		if err = metaVal.Unmarshal(metaValB); err != nil {
			return
		}
		idxName, docID := metaVal.GetIdxName(), metaVal.GetDocID()
		if idxName != "" {
			if _, err = pr.indexer.Del(idxName, docID); err != nil {
				return
			}
			if err = s.addIndexedKey(newPR, idxName, docID, key, wb); err != nil {
				return
			}
		}
		return
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

func convertToDocument(idxDef *pdpb.IndexDef, docID uint64, pairs []*raftcmdpb.FVPair) (doc *cql.DocumentWithIdx, err error) {
	doc = &cql.DocumentWithIdx{
		Document: cql.Document{
			DocID:     docID,
			UintProps: make([]cql.UintProp, 0),
			StrProps:  make([]cql.StrProp, 0),
		},
		Index: idxDef.GetName(),
	}
	for _, pair := range pairs {
		field := string(pair.GetField())
		valS := string(pair.GetValue())
		var val uint64
		for _, f := range idxDef.Fields {
			if f.GetName() != field {
				continue
			}
			switch f.GetType() {
			case pdpb.Uint8:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 1})
			case pdpb.Uint16:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 2})
			case pdpb.Uint32:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 4})
			case pdpb.Uint64:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 8})
			case pdpb.Text:
				doc.Document.StrProps = append(doc.Document.StrProps, cql.StrProp{Name: f.GetName(), Val: valS})
				log.Errorf("invalid filed type %v of idxDef %v", f.GetType().String(), idxDef)
				continue
			}
		}
	}
	return
}

//IndicesDiff is indices definion difference
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
