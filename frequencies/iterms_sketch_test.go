package frequencies

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type StringHasher struct {
}

func (h StringHasher) Hash(item string) uint64 {
	return uint64(len(item))
}

func TestEmpty(t *testing.T) {
	h := StringHasher{}
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, h)
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 0)
	assert.Equal(t, sketch.GetStreamLength(), int64(0))
	lb, err := sketch.GetLowerBound("a")
	assert.NoError(t, err)
	assert.Equal(t, lb, int64(0))
	ub, err := sketch.GetUpperBound("a")
	assert.NoError(t, err)
	assert.Equal(t, ub, int64(0))
}
