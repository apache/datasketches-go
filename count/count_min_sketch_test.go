package count

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CountMinSketch(t *testing.T) {
	numHashes := int8(3)
	numBuckets := int32(5)
	seed := int64(421)
	//insertedWeights := int64(0)
	cms, err := NewCountMinSketch(numBuckets, numHashes, seed)
	assert.NoError(t, err)
	x := "x"

	estimate := cms.GetEstimateString(x)
	assert.Equal(t, int64(0), estimate)

	weight := int64(9)
	err = cms.UpdateString(x, weight)
	assert.NoError(t, err)

	estimate = cms.GetEstimateString(x)
	fmt.Println(estimate)
}
