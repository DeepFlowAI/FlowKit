package sip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterOriginateHeaders_AllowsXHeader(t *testing.T) {
	out, err := FilterOriginateHeaders(map[string]string{"X-Custom": "1"})
	require.NoError(t, err)
	require.Equal(t, "1", out["X-Custom"])
}

func TestFilterOriginateHeaders_RejectsFrom(t *testing.T) {
	_, err := FilterOriginateHeaders(map[string]string{"From": "evil"})
	require.Error(t, err)
}
