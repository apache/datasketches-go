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

package tuple

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/theta"
)

func TestNewArrayOfNumbersCompactSketch(t *testing.T) {
	t.Run("Empty Source", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		assert.NotNil(t, sketch)
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())
		assert.True(t, sketch.IsOrdered())
	})

	t.Run("Single Entry Source", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		assert.NotNil(t, sketch)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(1), sketch.NumRetained())
		assert.True(t, sketch.IsOrdered())
	})

	t.Run("Multiple Entries Unordered", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})
		_ = source.UpdateInt64(3, []float64{50.0, 60.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		assert.NotNil(t, sketch)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())
		assert.False(t, sketch.IsOrdered())
	})

	t.Run("Multiple Entries Ordered", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})
		_ = source.UpdateInt64(3, []float64{50.0, 60.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, true)
		assert.NoError(t, err)

		assert.NotNil(t, sketch)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())
		assert.True(t, sketch.IsOrdered())

		// Verify entries are sorted by hash
		var prevHash uint64
		for hash := range sketch.All() {
			assert.Greater(t, hash, prevHash)
			prevHash = hash
		}
	})
}

func TestArrayOfNumbersCompactSketch_Compact(t *testing.T) {
	t.Run("Unordered", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})

		sketch, err := source.Compact(false)
		assert.NoError(t, err)

		assert.NotNil(t, sketch)
		assert.False(t, sketch.IsOrdered())
		assert.Equal(t, uint32(2), sketch.NumRetained())
	})

	t.Run("Ordered", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})

		sketch, err := source.Compact(true)
		assert.NoError(t, err)

		assert.NotNil(t, sketch)
		assert.True(t, sketch.IsOrdered())
		assert.Equal(t, uint32(2), sketch.NumRetained())
	})
}

func TestArrayOfNumbersCompactSketch_Estimate(t *testing.T) {
	source, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)
	_ = source.UpdateInt64(1, []float64{10.0, 20.0})
	_ = source.UpdateInt64(2, []float64{30.0, 40.0})
	_ = source.UpdateInt64(3, []float64{50.0, 60.0})

	sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
	assert.NoError(t, err)

	assert.Equal(t, 3.0, sketch.Estimate())
}

func TestArrayOfNumbersCompactSketch_LowerBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})
		_ = source.UpdateInt64(3, []float64{50.0, 60.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		lb, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		assert.Equal(t, 3.0, lb)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		lb, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		assert.LessOrEqual(t, lb, estimate)
	})
}

func TestArrayOfNumbersCompactSketch_UpperBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})
		_ = source.UpdateInt64(3, []float64{50.0, 60.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		ub, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		assert.Equal(t, 3.0, ub)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		ub, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, estimate)
	})
}

func TestArrayOfNumbersCompactSketch_Theta(t *testing.T) {
	source, err := NewArrayOfNumbersUpdateSketch[float64](
		2, WithUpdateSketchLgK(5),
	)
	assert.NoError(t, err)
	for i := 0; i < 100; i++ {
		_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
	}

	sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
	assert.NoError(t, err)

	assert.Less(t, sketch.Theta(), 1.0)
	assert.Less(t, sketch.Theta64(), theta.MaxTheta)
}

func TestArrayOfNumbersCompactSketch_String(t *testing.T) {
	t.Run("Without Items", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, true)
		assert.NoError(t, err)

		result := sketch.String(false)
		assert.Contains(t, result, "### Tuple sketch summary:")
		assert.Contains(t, result, "num retained entries : 2")
		assert.Contains(t, result, "empty?               : false")
		assert.Contains(t, result, "ordered?             : true")
		assert.NotContains(t, result, "### Retained entries")
	})

	t.Run("With Items", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, true)
		assert.NoError(t, err)

		result := sketch.String(true)
		assert.Contains(t, result, "### Tuple sketch summary:")
		assert.Contains(t, result, "### Retained entries")
		assert.Contains(t, result, "### End retained entries")
	})
}

func TestArrayOfNumbersCompactSketch_All(t *testing.T) {
	source, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)
	_ = source.UpdateInt64(1, []float64{10.0, 20.0})
	_ = source.UpdateInt64(2, []float64{30.0, 40.0})
	_ = source.UpdateInt64(3, []float64{50.0, 60.0})

	sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
	assert.NoError(t, err)

	count := 0
	seen := make(map[uint64]bool)
	for hash, summary := range sketch.All() {
		count++
		seen[hash] = true
		assert.NotZero(t, hash)
		assert.NotNil(t, summary)
		assert.Len(t, summary.Values(), 2)
	}

	assert.Equal(t, 3, count)
	assert.Equal(t, 3, len(seen))
}

func TestArrayOfNumbersCompactSketch_SeedHash(t *testing.T) {
	source, err := NewArrayOfNumbersUpdateSketch[float64](
		2, WithUpdateSketchSeed(12345),
	)
	assert.NoError(t, err)
	_ = source.UpdateInt64(1, []float64{10.0, 20.0})

	sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
	assert.NoError(t, err)

	seedHash, err := sketch.SeedHash()
	assert.NoError(t, err)
	assert.NotZero(t, seedHash)
}

func TestArrayOfNumbersCompactSketch_IsEstimationMode(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, theta.MaxTheta, sketch.Theta64())
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		assert.True(t, sketch.IsEstimationMode())
		assert.Less(t, sketch.Theta64(), theta.MaxTheta)
	})
}

func TestArrayOfNumbersCompactSketch_LowerBoundFromSubset(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 10; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		lb, err := sketch.LowerBoundFromSubset(2, 5)
		assert.NoError(t, err)
		assert.Equal(t, 5.0, lb)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.True(t, sketch.IsEstimationMode())

		subsetSize := uint32(10)
		lb, err := sketch.LowerBoundFromSubset(2, subsetSize)
		assert.NoError(t, err)
		assert.Less(t, lb, float64(subsetSize)/sketch.Theta())
	})

	t.Run("Subset Larger Than Retained", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 5; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		// Request more than retained - should be capped
		lb, err := sketch.LowerBoundFromSubset(2, 100)
		assert.NoError(t, err)
		assert.Equal(t, 5.0, lb)
	})
}

func TestArrayOfNumbersCompactSketch_UpperBoundFromSubset(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 10; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		ub, err := sketch.UpperBoundFromSubset(2, 5)
		assert.NoError(t, err)
		assert.Equal(t, 5.0, ub)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)
		assert.True(t, sketch.IsEstimationMode())

		subsetSize := uint32(10)
		ub, err := sketch.UpperBoundFromSubset(2, subsetSize)
		assert.NoError(t, err)
		assert.Greater(t, ub, float64(subsetSize)/sketch.Theta())
	})

	t.Run("Subset Larger Than Retained", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 5; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch, err := NewArrayOfNumbersCompactSketch[float64](source, false)
		assert.NoError(t, err)

		ub, err := sketch.UpperBoundFromSubset(2, 100)
		assert.NoError(t, err)
		assert.Equal(t, 5.0, ub)
	})
}
