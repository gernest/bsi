package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClamp(t *testing.T) {
	m := []match{
		{column: "2"},
		{column: "7"},
		{column: "exists", exists: true},
		{column: "4"},
		{column: "8"},
		{column: "exists", exists: true},
	}
	o := clamp(m)
	got := []string{}
	for i := range o {
		got = append(got, o[i].column)
	}
	want := []string{"exists", "2", "7", "4", "8"}
	require.Equal(t, want, got)
}
