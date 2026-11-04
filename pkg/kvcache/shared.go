/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvcache

import (
	"strings"
	"sync"
)

// SharedScope is the set of endpoints that can load a shared tier's blocks.
type SharedScope string

const (
	// SharedScopeNone marks a tier private to the endpoint that holds it.
	SharedScopeNone SharedScope = ""
	// SharedScopeNode marks a tier every endpoint on the same node can load,
	// such as a pool on node-local shared memory.
	SharedScopeNode SharedScope = "node"
	// SharedScopePool marks a tier every endpoint of the pool can load, such
	// as a pool on a cluster-wide shared filesystem.
	SharedScopePool SharedScope = "pool"
)

// sharedHolderPrefix starts every shared holder identity. Endpoint
// identities are host:port pairs and never start with it.
const sharedHolderPrefix = "shared:"

// poolSharedHolder holds blocks of every pool-scoped tier.
const poolSharedHolder = sharedHolderPrefix + "pool"

// IsSharedHolder reports whether an index identity is a shared holder
// rather than an endpoint.
func IsSharedHolder(identity string) bool {
	return strings.HasPrefix(identity, sharedHolderPrefix)
}

func nodeSharedHolder(node string) string {
	return sharedHolderPrefix + "node/" + node
}

// SharedHolders maps endpoints to the index identities that hold blocks of
// shared tiers on their behalf: one per node for node-scoped tiers and one
// for the whole pool. A KV-event publisher stores a shared block under its
// holder, and the prefix matcher credits that holder's blocks to every
// candidate endpoint in its scope. Holder entries are never cleared with an
// endpoint; they leave the index through BlockRemoved events or index
// capacity eviction.
//
// SharedHolders is safe for concurrent use. The zero value is not usable;
// use NewSharedHolders.
type SharedHolders struct {
	tiers map[string]SharedScope

	mu     sync.RWMutex
	nodeOf map[string]string // endpoint identity -> node
}

// NewSharedHolders builds the holder mapping for the shared backends.
func NewSharedHolders(backends []*KVCacheBackendConfig) (*SharedHolders, error) {
	tiers, err := sharedTiersFromBackends(backends)
	if err != nil {
		return nil, err
	}
	return &SharedHolders{tiers: tiers, nodeOf: make(map[string]string)}, nil
}

// Enabled reports whether any tier is shared.
func (s *SharedHolders) Enabled() bool {
	return s != nil && len(s.tiers) > 0
}

// SetNode records the node hosting an endpoint. An empty node forgets it.
func (s *SharedHolders) SetNode(endpoint, node string) {
	if !s.Enabled() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if node == "" {
		delete(s.nodeOf, endpoint)
		return
	}
	s.nodeOf[endpoint] = node
}

// Holder returns the identity that holds a block the endpoint published in
// the given lowercased device tier. A private tier returns the endpoint
// itself, as does a node-scoped tier on an endpoint whose node is unknown.
func (s *SharedHolders) Holder(endpoint, deviceTier string) string {
	if !s.Enabled() {
		return endpoint
	}
	switch s.tiers[deviceTier] {
	case SharedScopePool:
		return poolSharedHolder
	case SharedScopeNode:
		s.mu.RLock()
		node, ok := s.nodeOf[endpoint]
		s.mu.RUnlock()
		if ok {
			return nodeSharedHolder(node)
		}
	}
	return endpoint
}

// HoldersFor returns the endpoints each shared holder serves among the
// candidate endpoints, or nil when no tier is shared.
func (s *SharedHolders) HoldersFor(endpoints []string) map[string][]string {
	if !s.Enabled() {
		return nil
	}
	var hasPool, hasNode bool
	for _, scope := range s.tiers {
		hasPool = hasPool || scope == SharedScopePool
		hasNode = hasNode || scope == SharedScopeNode
	}
	holders := make(map[string][]string)
	if hasPool {
		holders[poolSharedHolder] = endpoints
	}
	if hasNode {
		s.mu.RLock()
		for _, endpoint := range endpoints {
			if node, ok := s.nodeOf[endpoint]; ok {
				holder := nodeSharedHolder(node)
				holders[holder] = append(holders[holder], endpoint)
			}
		}
		s.mu.RUnlock()
	}
	return holders
}
