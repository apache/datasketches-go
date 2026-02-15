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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVarOptItemsSketch(t *testing.T) {
	t.Run("valid K", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[string](16)
		assert.NoError(t, err)
		assert.Equal(t, 16, sketch.K())
		assert.Equal(t, int64(0), sketch.N())
		assert.True(t, sketch.IsEmpty())
	})

	t.Run("K is too large", func(t *testing.T) {
		_, err := NewVarOptItemsSketch[string](varOptMaxK + 1)
		assert.ErrorContains(t, err, "k must be at least 1 and less than 2^31 - 1")
	})
}

func TestVarOptItemsSketch_NumSamples(t *testing.T) {
	t.Run("empty sketch returns 0", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)
		assert.Equal(t, 0, sketch.NumSamples())
	})

	t.Run("fewer items than k returns number of items", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)
		for i := 1; i <= 5; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, 5, sketch.NumSamples())
	})

	t.Run("exactly k items returns k", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)
		for i := 1; i <= 10; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, 10, sketch.NumSamples())
	})

	t.Run("more than k items returns k", func(t *testing.T) {
		k := 100
		sketch, err := NewVarOptItemsSketch[int](uint(k))
		assert.NoError(t, err)
		for i := 1; i <= 200; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, k, sketch.NumSamples())
	})
}

func TestVarOptItemsSketch_Reset(t *testing.T) {
	t.Run("exact mode", func(t *testing.T) {
		k := 10
		sketch, err := NewVarOptItemsSketch[int](uint(k))
		assert.NoError(t, err)
		for i := 1; i <= 5; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, int64(5), sketch.N())
		assert.Equal(t, k, sketch.K())
		assert.False(t, sketch.IsEmpty())

		sketch.Reset()

		assert.Equal(t, 10, sketch.K())
		assert.Equal(t, int64(0), sketch.N())
		assert.Equal(t, 0, sketch.NumSamples())
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, 0, sketch.H())
		assert.Equal(t, 0, sketch.R())
	})

	t.Run("estimation mode", func(t *testing.T) {
		k := 100
		sketch, err := NewVarOptItemsSketch[int](uint(k))
		assert.NoError(t, err)
		for i := 1; i <= 200; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, int64(200), sketch.N())
		assert.Equal(t, k, sketch.K())
		assert.Equal(t, k, sketch.NumSamples())

		sketch.Reset()

		assert.Equal(t, k, sketch.K())
		assert.Equal(t, int64(0), sketch.N())
		assert.Equal(t, 0, sketch.NumSamples())
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, 0, sketch.H())
		assert.Equal(t, 0, sketch.R())
	})
}

func TestVarOptItemsSketch_All(t *testing.T) {
	t.Run("empty sketch", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		count := 0
		for range sketch.All() {
			count++
		}
		assert.Equal(t, 0, count)
	})

	t.Run("exact mode", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		expectedWeights := map[int]float64{}
		for i := 1; i <= 5; i++ {
			w := float64(i) * 10.0
			err = sketch.Update(i, w)
			assert.NoError(t, err)
			expectedWeights[i] = w
		}

		count := 0
		for sample := range sketch.All() {
			w, ok := expectedWeights[sample.Item]
			assert.True(t, ok, "unexpected item %d", sample.Item)
			assert.Equal(t, w, sample.Weight)
			count++
		}
		assert.Equal(t, 5, count)
	})

	t.Run("estimation mode", func(t *testing.T) {
		k := 100
		sketch, err := NewVarOptItemsSketch[int](uint(k))
		assert.NoError(t, err)
		for i := 1; i <= 200; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}

		hCount := sketch.H()
		rCount := sketch.R()
		assert.Equal(t, k, hCount+rCount)

		tau := sketch.totalWeightR / float64(rCount)

		idx := 0
		for sample := range sketch.All() {
			assert.True(t, sample.Weight > 0, "weight should be positive")
			if idx >= hCount {
				// R region items should have weight == tau
				assert.InDelta(t, tau, sample.Weight, 1e-10)
			}
			idx++
		}
		assert.Equal(t, k, idx)
	})

	t.Run("early break stops iteration", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)
		for i := 1; i <= 5; i++ {
			err = sketch.Update(i, float64(i))
			assert.NoError(t, err)
		}

		count := 0
		for range sketch.All() {
			count++
			if count == 3 {
				break
			}
		}
		assert.Equal(t, 3, count)
	})
}

func TestVarOptItemsSketch_Update(t *testing.T) {
	t.Run("negative weight", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		err = sketch.Update(1, -1.0)
		assert.ErrorContains(t, err, "weight must be strictly positive and finite")
		assert.Equal(t, int64(0), sketch.N())
	})

	t.Run("zero weight", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		err = sketch.Update(1, 0.0)
		assert.ErrorContains(t, err, "weight must be strictly positive and finite")
		assert.Equal(t, int64(0), sketch.N())
	})

	t.Run("NaN weight", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		err = sketch.Update(1, math.NaN())
		assert.ErrorContains(t, err, "weight must be strictly positive and finite")
		assert.Equal(t, int64(0), sketch.N())
	})

	t.Run("positive infinity weight", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		err = sketch.Update(1, math.Inf(1))
		assert.ErrorContains(t, err, "weight must be strictly positive and finite")
		assert.Equal(t, int64(0), sketch.N())
	})

	t.Run("negative infinity weight", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		err = sketch.Update(1, math.Inf(-1))
		assert.ErrorContains(t, err, "weight must be strictly positive and finite")
		assert.Equal(t, int64(0), sketch.N())
	})

	t.Run("exact mode", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](10)
		assert.NoError(t, err)

		inputWeightSum := float64(0)
		for i := 1; i <= 5; i++ {
			w := float64(i)

			err = sketch.Update(i, w)
			assert.NoError(t, err)

			inputWeightSum += w
		}

		outputWeightSum := float64(0)
		for sample := range sketch.All() {
			outputWeightSum += sample.Weight
		}

		assert.Equal(t, 5, sketch.H())
		assert.Equal(t, 0, sketch.R())
		assert.False(t, sketch.IsEmpty())

		// check cumulative weight
		weightRatio := outputWeightSum / inputWeightSum
		assert.InDelta(t, weightRatio, 1.0, 1e-10)
	})

	t.Run("estimation mode", func(t *testing.T) {
		k := 100
		sketch, err := NewVarOptItemsSketch[int](uint(k))
		assert.NoError(t, err)

		inputWeightSum := float64(0)
		for i := 1; i <= 200; i++ {
			w := float64(i)

			err = sketch.Update(i, w)
			assert.NoError(t, err)

			inputWeightSum += w
		}

		outputWeightSum := float64(0)
		for sample := range sketch.All() {
			outputWeightSum += sample.Weight
		}

		assert.Equal(t, int64(200), sketch.N())
		assert.Equal(t, k, sketch.H()+sketch.R())
		assert.True(t, sketch.totalWeightR > 0)

		// check cumulative weight
		weightRatio := outputWeightSum / inputWeightSum
		assert.InDelta(t, weightRatio, 1.0, 1e-10)
	})
}

func TestVarOptItemsSketch_EstimateSubsetSum(t *testing.T) {
	k := 10
	sketch, err := NewVarOptItemsSketch[int64](uint(k))
	assert.NoError(t, err)

	// empty sketch
	summary, err := sketch.EstimateSubsetSum(func(i int64) bool {
		return true
	})
	assert.NoError(t, err)
	assert.Equal(t, 0.0, summary.Estimate)
	assert.Equal(t, 0.0, summary.TotalSketchWeight)

	// exact mode
	weightSum := 0.0
	for i := 1; i < k; i++ {
		err := sketch.Update(int64(i), float64(i))
		assert.NoError(t, err)

		weightSum += float64(i)
	}

	summary, err = sketch.EstimateSubsetSum(func(i int64) bool {
		return true
	})
	assert.NoError(t, err)
	assert.Equal(t, weightSum, summary.Estimate)
	assert.Equal(t, weightSum, summary.LowerBound)
	assert.Equal(t, weightSum, summary.UpperBound)
	assert.Equal(t, weightSum, summary.TotalSketchWeight)

	// estimation mode
	for i := k; i < k+2; i++ {
		err = sketch.Update(int64(i), float64(i))
		assert.NoError(t, err)

		weightSum += float64(i)
	}

	summary, err = sketch.EstimateSubsetSum(func(i int64) bool {
		return true
	})
	assert.NoError(t, err)
	assert.Equal(t, weightSum, summary.Estimate)
	assert.Equal(t, weightSum, summary.UpperBound)
	assert.Less(t, summary.LowerBound, weightSum)
	assert.Equal(t, weightSum, summary.TotalSketchWeight)

	summary, err = sketch.EstimateSubsetSum(func(i int64) bool {
		return false
	})
	assert.NoError(t, err)
	assert.Equal(t, 0.0, summary.Estimate)
	assert.Equal(t, 0.0, summary.LowerBound)
	assert.Greater(t, summary.UpperBound, 0.0)
	assert.Equal(t, weightSum, summary.TotalSketchWeight)

	// finally, a non-degenerate predicate
	// insert negative items with identical weights, filter for negative weights only
	for i := 1; i < k+2; i++ {
		err := sketch.Update(int64(-i), float64(i))
		assert.NoError(t, err)

		weightSum += float64(i)
	}

	summary, err = sketch.EstimateSubsetSum(func(i int64) bool {
		return i < 0
	})
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, summary.Estimate, summary.LowerBound)
	assert.LessOrEqual(t, summary.Estimate, summary.UpperBound)

	assert.Less(t, summary.LowerBound, weightSum/1.4)
	assert.Greater(t, summary.UpperBound, weightSum/2.6)
	assert.Equal(t, weightSum, summary.TotalSketchWeight)
}
