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

package kvcache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func sharedBackends(scope kvcache.SharedScope) []*kvcache.KVCacheBackendConfig {
	return []*kvcache.KVCacheBackendConfig{
		{Name: kvcache.GPUTier, Weight: 1.0},
		{Name: "cpu", Weight: 0.8},
		{Name: "storage", Weight: 0.5, Shared: scope},
	}
}

func TestNewSharedHoldersRejectsUnknownScope(t *testing.T) {
	_, err := kvcache.NewSharedHolders(sharedBackends("rack"))
	require.ErrorContains(t, err, `unknown shared scope "rack"`)
}

func TestSharedHoldersHolder(t *testing.T) {
	node, err := kvcache.NewSharedHolders(sharedBackends(kvcache.SharedScopeNode))
	require.NoError(t, err)
	node.SetNode("10.0.0.1:8000", "node-1")
	node.SetNode("10.0.0.2:8000", "node-1")

	sameNode := node.Holder("10.0.0.1:8000", "storage")
	assert.True(t, kvcache.IsSharedHolder(sameNode))
	assert.Equal(t, sameNode, node.Holder("10.0.0.2:8000", "storage"), "pods of one node share a holder")
	assert.Equal(t, "10.0.0.1:8000", node.Holder("10.0.0.1:8000", "cpu"), "private tiers stay with the pod")
	assert.Equal(t, "10.0.0.9:8000", node.Holder("10.0.0.9:8000", "storage"), "a pod of unknown node keeps its blocks")

	node.SetNode("10.0.0.1:8000", "")
	assert.Equal(t, "10.0.0.1:8000", node.Holder("10.0.0.1:8000", "storage"))

	pool, err := kvcache.NewSharedHolders(sharedBackends(kvcache.SharedScopePool))
	require.NoError(t, err)
	assert.True(t, kvcache.IsSharedHolder(pool.Holder("10.0.0.9:8000", "storage")), "pool scope needs no node")

	var unshared *kvcache.SharedHolders
	assert.Equal(t, "10.0.0.1:8000", unshared.Holder("10.0.0.1:8000", "storage"))
}

// TestMatchBlockKeysSharedTier covers the failover and scale-out cases: a
// block a node-scoped holder holds counts for every candidate on that node,
// including one that never published it, and for no candidate elsewhere.
func TestMatchBlockKeysSharedTier(t *testing.T) {
	ctx := context.Background()
	podA, podB, podC := "10.0.0.1:8000", "10.0.0.2:8000", "10.0.1.1:8000"
	keys := []kvblock.BlockHash{10, 20, 30}

	backends := sharedBackends(kvcache.SharedScopeNode)
	holders, err := kvcache.NewSharedHolders(backends)
	require.NoError(t, err)
	holders.SetNode(podA, "node-1")
	holders.SetNode(podB, "node-1")
	holders.SetNode(podC, "node-2")
	holder := holders.Holder(podA, "storage")

	walked, idx := newMatcher(t, backends)
	materialized := newMaterializedMatcher(idx, backends)
	// podA published all three blocks to the shared tier, then went away;
	// podB holds the first block on GPU.
	require.NoError(t, idx.Add(ctx, nil, keys, []kvblock.PodEntry{{PodIdentifier: holder, DeviceTier: "storage"}}))
	require.NoError(t, idx.Add(ctx, nil, keys[:1], []kvblock.PodEntry{{PodIdentifier: podB, DeviceTier: kvcache.GPUTier}}))

	want := map[string]kvcache.PodMatch{
		podB: {
			WeightedScore: 1.0 + 0.5 + 0.5, MatchedBlocks: 3, ConfirmedBlocks: 3,
			BlocksByTier: map[string]int{kvcache.GPUTier: 1, "storage": 3},
		},
	}
	for name, indexer := range map[string]*kvcache.Indexer{"walked": walked, "materialized": materialized} {
		t.Run(name, func(t *testing.T) {
			indexer.SetSharedHolders(holders)
			got, err := indexer.MatchBlockKeys(ctx, keys, sets.New(podB, podC))
			require.NoError(t, err)
			assertPodMatches(t, want, got)
		})
	}
}
