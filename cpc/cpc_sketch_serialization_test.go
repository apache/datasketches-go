/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cpc

import (
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/testutil"
)

func TestGenerateGoSnapshots(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	nArr := []int{0, 100, 200, 2000, 20000}
	flavorArr := []CpcFlavor{CpcFlavorEmpty, CpcFlavorSparse, CpcFlavorHybrid, CpcFlavorPinned, CpcFlavorSliding}
	for flavorIdx, n := range nArr {
		sketch, err := NewCpcSketchWithDefault(11)
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			assert.NoError(t, sketch.UpdateUint64(uint64(i)))
		}
		assert.Equal(t, sketch.getFlavor(), flavorArr[flavorIdx])

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)

		sl, err := sketch.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/cpc_n%d_go.sk", internal.GoPath, n), sl, 0644)
		assert.NoError(t, err)
	}
}

func TestJavaCompat(t *testing.T) {
	t.Run("Java CPC", func(t *testing.T) {
		nArr := []int{0, 100, 200, 2000, 20000}
		flavorArr := []CpcFlavor{CpcFlavorEmpty, CpcFlavorSparse, CpcFlavorHybrid, CpcFlavorPinned, CpcFlavorSliding}
		for flavorIdx, n := range nArr {
			filename := fmt.Sprintf("%s/cpc_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			sketch, err := NewCpcSketchFromSliceWithDefault(bytes)
			assert.NoError(t, err)
			assert.Equal(t, sketch.getFlavor(), flavorArr[flavorIdx])
			assert.InDelta(t, float64(n), sketch.GetEstimate(), float64(n)*0.02)
		}
	})
}

func TestCppCompat(t *testing.T) {
	t.Run("C++ CPC", func(t *testing.T) {
		nArr := []int{0, 100, 200, 2000, 20000}
		flavorArr := []CpcFlavor{CpcFlavorEmpty, CpcFlavorSparse, CpcFlavorHybrid, CpcFlavorPinned, CpcFlavorSliding}
		for flavorIdx, n := range nArr {
			filename := fmt.Sprintf("%s/cpc_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			sketch, err := NewCpcSketchFromSliceWithDefault(bytes)
			assert.NoError(t, err)
			assert.Equal(t, sketch.getFlavor(), flavorArr[flavorIdx])
			assert.InDelta(t, float64(n), sketch.GetEstimate(), float64(n)*0.02)
		}
	})
}

func newUpdatedSketch(t *testing.T, lgK, n int) *CpcSketch {
	t.Helper()
	sketch, err := NewCpcSketchWithDefault(lgK)
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		assert.NoError(t, sketch.UpdateUint64(uint64(i)))
	}
	return sketch
}

func toCompactSlice(t *testing.T, sketch *CpcSketch) []byte {
	t.Helper()
	b, err := sketch.ToCompactSlice()
	assert.NoError(t, err)
	return b
}

func reencodePairs(t *testing.T, sketch *CpcSketch, mutate func(pairs []int, k int)) []byte {
	t.Helper()
	state, err := NewCpcCompressedStateFromSketch(sketch)
	assert.NoError(t, err)
	pairs, err := uncompressTheSurprisingValues(state)
	assert.NoError(t, err)
	k := 1 << state.LgK
	mutate(pairs, k)

	numPairs := len(pairs)
	numBaseBits, err := golombChooseNumberOfBaseBits(k+numPairs, numPairs)
	assert.NoError(t, err)
	// rows may reach k, so leave room for twice the usual unary bits
	bufLen, err := safeLengthForCompressedPairBuf(2*k, numPairs, numBaseBits)
	assert.NoError(t, err)
	state.CsvStream = make([]int, bufLen)
	state.CsvLengthInts, err = lowLevelCompressPairs(pairs, numPairs, numBaseBits, state.CsvStream)
	assert.NoError(t, err)

	image, err := state.exportToMemory()
	assert.NoError(t, err)
	return image
}

func TestDeserializeCorrupt(t *testing.T) {
	t.Run("Truncated preamble", func(t *testing.T) {
		image := toCompactSlice(t, newUpdatedSketch(t, 10, 100))
		_, err := NewCpcSketchFromSliceWithDefault(image[:8])
		assert.ErrorContains(t, err, "insufficient Image Bytes")
	})

	t.Run("Corrupt lgK", func(t *testing.T) {
		image := toCompactSlice(t, newUpdatedSketch(t, 11, 100))
		for _, lgK := range []byte{0, 3, 27, 255} {
			corrupt := slices.Clone(image)
			corrupt[loFieldLgK] = lgK
			_, err := NewCpcSketchFromSliceWithDefault(corrupt)
			assert.Error(t, err, "lgK %d", lgK)
		}
	})

	t.Run("Corrupt numCoupons", func(t *testing.T) {
		image := toCompactSlice(t, newUpdatedSketch(t, 11, 100))
		assert.Equal(t, CpcFormatSparseHybridHip, getFormat(image))
		offset := hiFieldOffset[CpcFormatSparseHybridHip][hiFieldNumCoupons]
		numCoupons := binary.LittleEndian.Uint32(image[offset:])

		// more pairs than the compressed table holds: the decoder must not read past it
		corrupt := slices.Clone(image)
		binary.LittleEndian.PutUint32(corrupt[offset:], numCoupons+50)
		_, err := NewCpcSketchFromSliceWithDefault(corrupt)
		assert.Error(t, err)

		// more coupons than the k x 64 bit matrix can hold
		corrupt = slices.Clone(image)
		binary.LittleEndian.PutUint32(corrupt[offset:], 64*2048+1)
		_, err = NewCpcSketchFromSliceWithDefault(corrupt)
		assert.Error(t, err)
	})

	t.Run("Size check uses the format of the image", func(t *testing.T) {
		image := toCompactSlice(t, newUpdatedSketch(t, 11, 2000))
		assert.Equal(t, CpcFormatPinnedSlidingHip, getFormat(image))
		offset := hiFieldOffset[CpcFormatPinnedSlidingHip][hiFieldNumSV]
		assert.NotZero(t, binary.LittleEndian.Uint32(image[offset:]))

		// numSV = 0 makes the decoded fields look like format 5, whose preamble is 8 bytes shorter
		corrupt := slices.Clone(image)
		binary.LittleEndian.PutUint32(corrupt[offset:], 0)
		_, err := NewCpcSketchFromSliceWithDefault(corrupt[:len(corrupt)-8])
		assert.ErrorContains(t, err, "insufficient Image Bytes")
	})

	t.Run("numSV exceeds numCoupons", func(t *testing.T) {
		image := toCompactSlice(t, newUpdatedSketch(t, 11, 2000))
		assert.Equal(t, CpcFormatPinnedSlidingHip, getFormat(image))
		numCoupons := binary.LittleEndian.Uint32(image[hiFieldOffset[CpcFormatPinnedSlidingHip][hiFieldNumCoupons]:])
		binary.LittleEndian.PutUint32(image[hiFieldOffset[CpcFormatPinnedSlidingHip][hiFieldNumSV]:], numCoupons+1)
		_, err := NewCpcSketchFromSliceWithDefault(image)
		assert.ErrorContains(t, err, "exceeds numCoupons")
	})

	t.Run("Row out of range", func(t *testing.T) {
		sketch := newUpdatedSketch(t, 11, 100)
		assert.Equal(t, CpcFlavorSparse, sketch.getFlavor())
		image := reencodePairs(t, sketch, func(pairs []int, k int) {
			last := len(pairs) - 1
			pairs[last] = k<<6 | pairs[last]&63
		})
		_, err := NewCpcSketchFromSliceWithDefault(image)
		assert.ErrorContains(t, err, "row index")
	})

	t.Run("Sliding column out of range", func(t *testing.T) {
		sketch := newUpdatedSketch(t, 11, 20000)
		assert.Equal(t, CpcFlavorSliding, sketch.getFlavor())
		image := reencodePairs(t, sketch, func(pairs []int, k int) {
			last := len(pairs) - 1
			pairs[last] |= 63 // permuted columns are below 56
		})
		_, err := NewCpcSketchFromSliceWithDefault(image)
		assert.ErrorContains(t, err, "uncompressSlidingFlavor: invalid pair value")
	})
}

func TestNegativeIntEquivalence(t *testing.T) {
	// Create a new CPC sketch with default parameters
	sk, err := NewCpcSketchWithDefault(11)
	assert.NoError(t, err)

	// Update with -1 as a byte, short, int, and long
	var b int8 = -1
	err = sk.UpdateInt64(int64(b))
	assert.NoError(t, err)

	var s int16 = -1
	err = sk.UpdateInt64(int64(s))
	assert.NoError(t, err)

	var i int32 = -1
	err = sk.UpdateInt64(int64(i))
	assert.NoError(t, err)

	var l int64 = -1
	err = sk.UpdateInt64(l)
	assert.NoError(t, err)

	// Check that the estimate is 1 (since -1 in all forms is the same hash)
	assert.InDelta(t, 1.0, sk.GetEstimate(), 0.01)

	// Write out the binary so that Java/C++ can read it
	err = os.MkdirAll(internal.GoPath, os.ModePerm)
	assert.NoError(t, err)
	bytes, err := sk.ToCompactSlice()
	assert.NoError(t, err)
	err = os.WriteFile(fmt.Sprintf("%s/cpc_negative_one_go.sk", internal.GoPath), bytes, 0644)
	assert.NoError(t, err)
}
