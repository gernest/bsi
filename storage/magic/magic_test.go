package magic

import (
	"crypto/rand"
	"testing"

	"github.com/gernest/u128/checksum"
	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	o := make([]checksum.U128, 4)
	for i := range o {
		rand.Read(o[i][:])
	}
	sl := ReinterpretSlice[byte](o)
	x := ReinterpretSlice[checksum.U128](sl)
	require.Equal(t, o, x)
}
