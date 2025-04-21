package count

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CountMinSketch(t *testing.T) {
	seed := int64(1234567)
	t.Run("CM init - throws", func(t *testing.T) {
		cms, err := NewCountMinSketch(5, 1, seed)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "using fewer than 3 buckets incurs relative error greater than 1.0")
		assert.Nil(t, cms)

		cms, err = NewCountMinSketch(4, 268435456, seed)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "these parameters generate a sketch that exceeds 2^30 elements")
		assert.Nil(t, cms)
	})

	t.Run("CM Init", func(t *testing.T) {
		numHashes := 3
		numBuckets := 5
		cms, err := NewCountMinSketch(int8(numHashes), int32(numBuckets), seed)
		assert.NoError(t, err)

		assert.Equal(t, numHashes, cms.getNumHashes())
		assert.Equal(t, numBuckets, cms.getNumBuckets())
		assert.Equal(t, seed, cms.getSeed())

	})

	t.Run("CM one update: uint64_t", func(t *testing.T) {
		numHashes := int8(3)
		numBuckets := int32(5)
		//insertedWeights := int64(0)
		cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)
		x := "x"

		estimate := cms.GetEstimateString(x)
		assert.Equal(t, int64(0), estimate)

		weight := int64(9)
		err = cms.UpdateString(x, weight)
		assert.NoError(t, err)

		estimate = cms.GetEstimateString(x)
		fmt.Println(estimate)
	})
}
