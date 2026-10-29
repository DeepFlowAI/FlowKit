package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// ensure interface compliance at compile time
var _ Store = (*LocalStore)(nil)
var _ Store = (*RedisStore)(nil)

func TestLocalStore_IndexAndGet(t *testing.T) {
	s := NewLocalStore()
	ctx := context.Background()

	require.NoError(t, s.IndexCall(ctx, "call-1", "node-a"))

	node, err := s.GetNode(ctx, "call-1")
	require.NoError(t, err)
	require.Equal(t, "node-a", node)
}

func TestLocalStore_GetNode_NotFound(t *testing.T) {
	s := NewLocalStore()
	_, err := s.GetNode(context.Background(), "missing")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestLocalStore_Remove(t *testing.T) {
	s := NewLocalStore()
	ctx := context.Background()
	require.NoError(t, s.IndexCall(ctx, "c1", "node-a"))

	require.NoError(t, s.RemoveCall(ctx, "c1"))
	_, err := s.GetNode(ctx, "c1")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestLocalStore_Remove_NotFound(t *testing.T) {
	s := NewLocalStore()
	err := s.RemoveCall(context.Background(), "ghost")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestLocalStore_ListNodeCalls(t *testing.T) {
	s := NewLocalStore()
	ctx := context.Background()
	require.NoError(t, s.IndexCall(ctx, "c1", "node-a"))
	require.NoError(t, s.IndexCall(ctx, "c2", "node-a"))
	require.NoError(t, s.IndexCall(ctx, "c3", "node-b"))

	listA, err := s.ListNodeCalls(ctx, "node-a")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"c1", "c2"}, listA)

	listB, err := s.ListNodeCalls(ctx, "node-b")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"c3"}, listB)

	listUnknown, err := s.ListNodeCalls(ctx, "node-z")
	require.NoError(t, err)
	require.Empty(t, listUnknown)
}

func TestLocalStore_Reindex_UpdatesNodeListing(t *testing.T) {
	s := NewLocalStore()
	ctx := context.Background()
	require.NoError(t, s.IndexCall(ctx, "c1", "node-a"))
	require.NoError(t, s.IndexCall(ctx, "c1", "node-b")) // move

	node, err := s.GetNode(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, "node-b", node)

	listA, _ := s.ListNodeCalls(ctx, "node-a")
	require.Empty(t, listA, "old node listing must be cleared")

	listB, _ := s.ListNodeCalls(ctx, "node-b")
	require.ElementsMatch(t, []string{"c1"}, listB)
}

func TestLocalStore_EmptyArgs_RejectedAsInvalid(t *testing.T) {
	s := NewLocalStore()
	ctx := context.Background()
	require.ErrorIs(t, s.IndexCall(ctx, "", "node-a"), ErrInvalidArgument)
	require.ErrorIs(t, s.IndexCall(ctx, "c1", ""), ErrInvalidArgument)
}

func TestLocalStore_Concurrent(t *testing.T) {
	s := NewLocalStore()
	ctx := context.Background()

	const writers = 10
	const writes = 100

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			node := fmt.Sprintf("node-%d", w%3)
			for i := 0; i < writes; i++ {
				id := fmt.Sprintf("call-%d-%d", w, i)
				if err := s.IndexCall(ctx, id, node); err != nil {
					t.Errorf("index: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// All calls should be retrievable
	for w := 0; w < writers; w++ {
		for i := 0; i < writes; i++ {
			id := fmt.Sprintf("call-%d-%d", w, i)
			node, err := s.GetNode(ctx, id)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("node-%d", w%3), node)
		}
	}
}

func TestLocalStore_Close_NoOp(t *testing.T) {
	require.NoError(t, NewLocalStore().Close())
}

func TestRedisStore_StubReturnsError(t *testing.T) {
	_, err := NewRedisStore()
	require.Error(t, err)

	rs := &RedisStore{}
	ctx := context.Background()
	require.Error(t, rs.IndexCall(ctx, "c1", "n1"))
	_, err = rs.GetNode(ctx, "c1")
	require.Error(t, err)
	require.Error(t, rs.RemoveCall(ctx, "c1"))
	_, err = rs.ListNodeCalls(ctx, "n1")
	require.Error(t, err)
	require.NoError(t, rs.Close())
}
