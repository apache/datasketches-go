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

package sampling

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVarOptItemsUnion(t *testing.T) {
	_, err := NewVarOptItemsUnion[int](0)
	assert.ErrorContains(t, err, "k must be at least 1")

	union, err := NewVarOptItemsUnion[int](16)
	assert.NoError(t, err)
	assert.Equal(t, 16, union.maxK)
}

func TestVarOptItemsUnion_ResultEmpty(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, 8, result.K())
	assert.Equal(t, int64(0), result.N())
	assert.True(t, result.IsEmpty())
}

func TestVarOptItemsUnion_UpdateSketchExactMode(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int](16)
	assert.NoError(t, err)

	for i := 0; i < 8; i++ {
		assert.NoError(t, sketch.Update(i, float64(i+1)))
	}

	union, err := NewVarOptItemsUnion[int](16)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(sketch))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), result.N())
	assert.Equal(t, 8, result.NumSamples())
	assert.Equal(t, 8, result.H())
	assert.Equal(t, 0, result.R())
}

func TestVarOptItemsUnion_UpdateSketchSamplingWithExtremeHeavyItem(t *testing.T) {
	const k = 16

	sketch1, err := NewVarOptItemsSketch[int](uint(k))
	assert.NoError(t, err)
	for i := 0; i < 500; i++ {
		assert.NoError(t, sketch1.Update(i, 1.0))
	}
	assert.NoError(t, sketch1.Update(-1, 1e12))
	assert.Greater(t, sketch1.R(), 0)

	sketch2, err := NewVarOptItemsSketch[int](uint(k))
	assert.NoError(t, err)
	for i := 1000; i < 1500; i++ {
		assert.NoError(t, sketch2.Update(i, 1.0))
	}
	assert.Greater(t, sketch2.R(), 0)

	union, err := NewVarOptItemsUnion[int](k)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(sketch1))
	assert.NoError(t, union.UpdateSketch(sketch2))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1001), result.N())
	assert.Equal(t, k, result.K())
	assert.LessOrEqual(t, result.NumSamples(), k)

	foundHeavy := false
	for sample := range result.All() {
		if sample.Item == -1 {
			foundHeavy = true
			break
		}
	}
	assert.True(t, foundHeavy, "extreme heavy item should be retained in union result")
}

func TestVarOptItemsUnion_UpdateSketchIdenticalSamplingSketches(t *testing.T) {
	const k = 16
	const n = 1000

	base, err := NewVarOptItemsSketch[int64](uint(k))
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		assert.NoError(t, base.Update(int64(i), 1.0))
	}
	assert.Greater(t, base.R(), 0)

	data, err := base.ToSlice(Int64SerDe{})
	assert.NoError(t, err)
	clone, err := NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)

	union, err := NewVarOptItemsUnion[int64](k)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(base))
	assert.NoError(t, union.UpdateSketch(clone))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2*n), result.N())
	assert.Equal(t, k, result.K())
	assert.LessOrEqual(t, result.NumSamples(), k)

	ss, err := result.EstimateSubsetSum(func(_ int64) bool { return true })
	assert.NoError(t, err)
	assert.InDelta(t, float64(2*n), ss.TotalSketchWeight, 1e-9)
}

func TestVarOptItemsUnion_UpdateSketchDifferentKWeightedItems(t *testing.T) {
	smallK := 8
	largeK := 32

	small, err := NewVarOptItemsSketch[int](uint(smallK))
	assert.NoError(t, err)
	totalWeight := 0.0
	for i := 1; i <= 200; i++ {
		w := float64(i)
		totalWeight += w
		assert.NoError(t, small.Update(i, w))
	}
	assert.Greater(t, small.R(), 0)

	large, err := NewVarOptItemsSketch[int](uint(largeK))
	assert.NoError(t, err)
	for i := 1; i <= 400; i++ {
		w := float64(i) * 0.5
		totalWeight += w
		assert.NoError(t, large.Update(10000+i, w))
	}
	assert.Greater(t, large.R(), 0)

	union, err := NewVarOptItemsUnion[int](largeK)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(small))
	assert.NoError(t, union.UpdateSketch(large))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(600), result.N())
	assert.GreaterOrEqual(t, result.K(), 1)
	assert.LessOrEqual(t, result.K(), largeK)
	assert.LessOrEqual(t, result.NumSamples(), largeK)

	ss, err := result.EstimateSubsetSum(func(_ int) bool { return true })
	assert.NoError(t, err)
	assert.InDelta(t, totalWeight, ss.TotalSketchWeight, 1e-9)
}

func TestVarOptItemsUnion_UpdateSketchNilNoop(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	assert.NoError(t, union.UpdateSketch(nil))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result.N())
}

func TestVarOptItemsUnion_Reset(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int](8)
	assert.NoError(t, err)
	for i := 0; i < 4; i++ {
		assert.NoError(t, sketch.Update(i, float64(i+1)))
	}

	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(sketch))

	assert.NoError(t, union.Reset())

	result, err := union.Result()
	assert.NoError(t, err)
	assert.True(t, result.IsEmpty())
	assert.Equal(t, 8, result.K())
}

func TestVarOptItemsUnion_ResultPseudoExactMarkedResolution(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	// Construct a pseudo-exact gadget: r=0 with marked items in H.
	for i := 1; i <= 4; i++ {
		assert.NoError(t, union.gadget.update(i, float64(i), true))
	}
	union.n = 4
	union.outerTauDenom = int64(union.gadget.numMarksInH)

	out, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, int64(4), out.N())
	assert.Equal(t, 4, out.K())
	assert.Equal(t, 0, out.H())
	assert.Equal(t, 4, out.R())
	assert.Nil(t, out.marks)
	assert.Equal(t, uint32(0), out.numMarksInH)
}

func TestVarOptItemsUnion_ResultMigrateMarkedItemsByDecreasingK(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	// Construct a compact, valid estimation-mode gadget with one marked item in H.
	// Layout: [H=0] [gap=1] [R=2], with k=2, h=1, r=1.
	union.gadget = &VarOptItemsSketch[int]{
		data:         []int{100, 0, 1},
		weights:      []float64{10.0, -1.0, -1.0},
		marks:        []bool{true, false, false},
		k:            2,
		n:            10,
		h:            1,
		m:            0,
		r:            1,
		totalWeightR: 5.0,
		rf:           varOptDefaultResizeFactor,
		numMarksInH:  1,
	}
	union.n = 10

	out, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, int64(10), out.N())
	assert.Nil(t, out.marks)
	assert.Equal(t, uint32(0), out.numMarksInH)
}
