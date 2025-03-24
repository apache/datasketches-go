package cpc

import (
	"github.com/apache/datasketches-go/internal"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamingCheck(t *testing.T) {
	// Input parameters
	lgMinK := 10
	lgMaxK := 10
	trials := 10
	ppoN := 1

	sVal := NewStreamingValidation(lgMinK, lgMaxK, trials, ppoN, nil, nil)

	sVal.Start()
}

func TestMatrixCouponCountCheck(t *testing.T) {
	pat := uint64(0xA5A5A5A55A5A5A5A)
	length := 16
	arr := make([]uint64, length)
	for i := range arr {
		arr[i] = pat
	}

	trueCount := uint64(length) * uint64(bits.OnesCount64(pat))
	testCount := CountCoupons(arr)

	assert.Equal(t, trueCount, testCount, "bit counts should match")
}

func TestCompressionCharacterizationCheck(t *testing.T) {
	// Input parameters
	lgMinK := 10
	lgMaxK := 10
	lgMaxT := 5 // Trials at start
	lgMinT := 2 // Trials at end
	lgMulK := 7
	uPPO := 1
	incLgK := 1

	cc := NewCompressionCharacterization(
		lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK,
		nil,
		nil,
	)
	assert.NoError(t, cc.doRangeOfLgK())
}

func TestSingleRowColCheck(t *testing.T) {
	lgK := 20
	srcSketch, _ := NewCpcSketchWithDefault(lgK)

	rowCol := 54746379
	err := srcSketch.rowColUpdate(rowCol)
	assert.NoError(t, err)
	t.Log(srcSketch.String()) // or some debug

	state, err := NewCpcCompressedStateFromSketch(srcSketch)
	assert.NoError(t, err)
	t.Log(state)

	uncSketch, err := NewCpcSketch(state.LgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	err = state.uncompress(uncSketch)
	assert.NoError(t, err)
	t.Log(uncSketch.String())
}

func TestMergingValidationCheck(t *testing.T) {
	lgMinK := 10
	lgMaxK := 10
	lgMulK := 5
	uPPO := 1
	incLgK := 1

	mv := NewMergingValidation(lgMinK, lgMaxK, lgMulK, uPPO, incLgK, nil, nil)
	assert.NoError(t, mv.Start())
}

func TestQuickMergingValidationCheck(t *testing.T) {
	lgMinK := 10
	lgMaxK := 10
	incLgK := 1

	qmv := NewQuickMergingValidation(lgMinK, lgMaxK, incLgK, nil, nil)
	assert.NoError(t, qmv.Start())
}

func TestCheckPwrLaw10NextDouble(t *testing.T) {
	got := pwrLaw10NextDouble(1, 10.0)
	assert.Equal(t, 100.0, got, "pwrLaw10NextDouble(1,10.0) should return 100.0")
}
