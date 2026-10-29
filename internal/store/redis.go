package store

import (
	"context"
	"errors"
)

// RedisStore will become the multi-node implementation in M4. The real
// constructor accepts a config.RedisConfig and returns a connected client;
// for now we keep a stub so callers can refer to the type and the package
// dependency graph is established.
type RedisStore struct{}

// NewRedisStore is a stub. It returns an error until M4 wires up the real
// implementation.
func NewRedisStore() (*RedisStore, error) {
	return nil, errors.New("store: redis backend not implemented (M4)")
}

// IndexCall is a stub.
func (*RedisStore) IndexCall(_ context.Context, _, _ string) error {
	return errors.New("store: redis backend not implemented (M4)")
}

// GetNode is a stub.
func (*RedisStore) GetNode(_ context.Context, _ string) (string, error) {
	return "", errors.New("store: redis backend not implemented (M4)")
}

// RemoveCall is a stub.
func (*RedisStore) RemoveCall(_ context.Context, _ string) error {
	return errors.New("store: redis backend not implemented (M4)")
}

// ListNodeCalls is a stub.
func (*RedisStore) ListNodeCalls(_ context.Context, _ string) ([]string, error) {
	return nil, errors.New("store: redis backend not implemented (M4)")
}

// Close is a stub.
func (*RedisStore) Close() error { return nil }
