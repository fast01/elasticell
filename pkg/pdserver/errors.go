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

package pdserver

import (
	"errors"
)

var (
	errMaybeNotLeader = errors.New("may be not leader")
	errTxnFailed      = errors.New("failed to commit transaction")

	errSchedulerExisted  = errors.New("scheduler is existed")
	errSchedulerNotFound = errors.New("scheduler is not found")
)

var (
	errEmbedEctdClusterIDNotMatch = errors.New("embed etcd cluster id not match")
	errRPCReq                     = errors.New("invalid rpc req")
	errStaleCell                  = errors.New("stale cell epoch")
	errNotBootstrapped            = errors.New("cluster not bootstrapped")
	errTombstoneStore             = errors.New("store is tombstone")
	errStoreNotFound              = errors.New("store is not found")
)
