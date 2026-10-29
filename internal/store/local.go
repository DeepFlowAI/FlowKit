package store

import (
	"context"
	"sync"
)

// LocalStore is an in-memory Store. Suitable for single-node deployments and
// tests. All data is lost on process restart.
type LocalStore struct {
	mu sync.RWMutex
	// callToNode lets GetNode(callID) run in O(1).
	callToNode map[string]string
	// nodeToCalls lets ListNodeCalls(node) run in O(calls on that node).
	nodeToCalls map[string]map[string]struct{}
}

// NewLocalStore returns an empty LocalStore.
func NewLocalStore() *LocalStore {
	return &LocalStore{
		callToNode:  make(map[string]string),
		nodeToCalls: make(map[string]map[string]struct{}),
	}
}

// IndexCall implements Store.
func (s *LocalStore) IndexCall(_ context.Context, callID, nodeAddr string) error {
	if callID == "" || nodeAddr == "" {
		return ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// If callID was already on another node, remove from the old index first.
	if prevNode, ok := s.callToNode[callID]; ok && prevNode != nodeAddr {
		if set, ok := s.nodeToCalls[prevNode]; ok {
			delete(set, callID)
			if len(set) == 0 {
				delete(s.nodeToCalls, prevNode)
			}
		}
	}

	s.callToNode[callID] = nodeAddr
	set, ok := s.nodeToCalls[nodeAddr]
	if !ok {
		set = make(map[string]struct{})
		s.nodeToCalls[nodeAddr] = set
	}
	set[callID] = struct{}{}
	return nil
}

// GetNode implements Store.
func (s *LocalStore) GetNode(_ context.Context, callID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.callToNode[callID]
	if !ok {
		return "", ErrNotFound
	}
	return node, nil
}

// RemoveCall implements Store.
func (s *LocalStore) RemoveCall(_ context.Context, callID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.callToNode[callID]
	if !ok {
		return ErrNotFound
	}
	delete(s.callToNode, callID)
	if set, ok := s.nodeToCalls[node]; ok {
		delete(set, callID)
		if len(set) == 0 {
			delete(s.nodeToCalls, node)
		}
	}
	return nil
}

// ListNodeCalls implements Store.
func (s *LocalStore) ListNodeCalls(_ context.Context, nodeAddr string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set, ok := s.nodeToCalls[nodeAddr]
	if !ok {
		return []string{}, nil
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out, nil
}

// Close is a no-op for LocalStore.
func (s *LocalStore) Close() error { return nil }
