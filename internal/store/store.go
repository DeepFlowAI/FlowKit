package store

import (
	"context"
	"errors"
)

// ErrNotFound is returned when a lookup misses.
var ErrNotFound = errors.New("store: not found")

// ErrInvalidArgument is returned for empty / malformed arguments.
var ErrInvalidArgument = errors.New("store: invalid argument")

// Store indexes which Flowkit node owns each call. LocalStore is suitable for
// single-node deployments; RedisStore (M4) makes the index shared across a
// fleet so a Python SDK can find the right node from any entry point.
type Store interface {
	// IndexCall records that callID lives on nodeAddr. Overwrites any prior
	// entry for that callID.
	IndexCall(ctx context.Context, callID, nodeAddr string) error

	// GetNode returns the nodeAddr that owns callID, or ErrNotFound.
	GetNode(ctx context.Context, callID string) (string, error)

	// RemoveCall removes the entry. Returns ErrNotFound if it was missing.
	RemoveCall(ctx context.Context, callID string) error

	// ListNodeCalls returns the call ids currently indexed against nodeAddr.
	// An unknown node returns an empty slice and a nil error.
	ListNodeCalls(ctx context.Context, nodeAddr string) ([]string, error)

	// Close releases any underlying resources (network connections, etc.).
	Close() error
}
