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

package theta

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewQuickSelectUpdateSketch(t *testing.T) {
	t.Run("No Options And Empty", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		assert.True(t, updateSketch.IsEmpty())
		assert.False(t, updateSketch.IsEstimationMode())
		assert.Equal(t, 1.0, updateSketch.Theta())
		assert.Equal(t, 0.0, updateSketch.Estimate())
		lb, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
		assert.True(t, updateSketch.IsOrdered())
	})

	t.Run("With Options", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(10),
			WithUpdateSketchResizeFactor(ResizeX2),
			WithUpdateSketchP(0.5),
			WithUpdateSketchSeed(12345),
		)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint8(10), sketch.LgK())
		assert.Equal(t, ResizeX2, sketch.ResizeFactor())
		assert.Equal(t, float32(0.5), sketch.table.p)
		assert.Equal(t, uint64(12345), sketch.table.seed)
	})

	t.Run("Non Empty No Retained Keys", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.001))
		assert.NoError(t, err)
		assertUpdate(t, updateSketch.UpdateInt64(1))

		assert.Zero(t, updateSketch.NumRetained())
		assert.False(t, updateSketch.IsEmpty())
		assert.True(t, updateSketch.IsEstimationMode())
		assert.Equal(t, 0.0, updateSketch.Estimate())
		lb, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Greater(t, ub, 0.0)

		updateSketch.Reset()
		assert.True(t, updateSketch.IsEmpty())
		assert.False(t, updateSketch.IsEstimationMode())
		assert.Equal(t, 1.0, updateSketch.Theta())
		assert.Equal(t, 0.0, updateSketch.Estimate())
		lb, err = updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err = updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
	})

	t.Run("Invalid Lgk", func(t *testing.T) {
		_, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(3))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lg_k must not be less than")

		_, err = NewQuickSelectUpdateSketch(WithUpdateSketchLgK(30))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lg_k must not be greater than")
	})

	t.Run("Invalid P", func(t *testing.T) {
		_, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.0))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sampling probability must be between 0 and 1")

		_, err = NewQuickSelectUpdateSketch(WithUpdateSketchP(1.5))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sampling probability must be between 0 and 1")
	})
}

func TestQuickSelectUpdateSketch_Theta64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
	assert.NoError(t, err)

	// Empty sketch has MaxTheta
	assert.Equal(t, MaxTheta, sketch.Theta64())

	initialTheta := sketch.table.theta
	assert.Equal(t, MaxTheta, initialTheta)

	// Insert many values to trigger rebuild
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i))
	}

	assert.Less(t, sketch.table.theta, initialTheta)
	assert.Equal(t, sketch.table.theta, sketch.Theta64())
}

func TestQuickSelectUpdateSketch_SeedHash(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(12345))
	assert.NoError(t, err)

	seedHash, err := sketch.SeedHash()
	assert.NoError(t, err)
	assert.NotZero(t, seedHash)
}

func TestQuickSelectUpdateSketch_UpdateUint64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateUint64(100)
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())

	err = sketch.UpdateUint64(100)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateInt64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateInt64(-100)
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())

	err = sketch.UpdateInt64(-100)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateInt32(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateInt32(42)
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())

	err = sketch.UpdateInt32(42)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateString(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateString("")
	assert.ErrorIs(t, err, ErrUpdateEmptyString)

	err = sketch.UpdateString("hello")
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateString("hello")
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateBytes(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateBytes([]byte{1, 2, 3})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateBytes([]byte{1, 2, 3})
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateFloat64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateFloat64(3.14)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateFloat64(3.14)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateFloat32(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateFloat32(3.14)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateFloat32(3.14)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_Estimate(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	assert.Equal(t, 0.0, sketch.Estimate())

	err = sketch.UpdateInt64(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sketch.Estimate())
}

func TestQuickSelectUpdateSketch_LowerBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, float64(sketch.NumRetained()), lb)

		for _, stdDevs := range []uint8{1, 2, 3} {
			lb, err = sketch.LowerBound(stdDevs)
			assert.NoError(t, err)
			assert.Equal(t, float64(sketch.NumRetained()), lb)
		}
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.LessOrEqual(t, lb, estimate)

		lb1, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		lb2, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		lb3, err := sketch.LowerBound(3)
		assert.NoError(t, err)

		assert.GreaterOrEqual(t, lb1, lb2)
		assert.GreaterOrEqual(t, lb2, lb3)
	})

	t.Run("Invalid NumStdDevs", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		_, err = sketch.LowerBound(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")

		_, err = sketch.LowerBound(4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")
	})
}

func TestQuickSelectUpdateSketch_UpperBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		ub, err = sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, float64(sketch.NumRetained()), ub)

		for _, stdDevs := range []uint8{1, 2, 3} {
			ub, err = sketch.UpperBound(stdDevs)
			assert.NoError(t, err)
			assert.Equal(t, float64(sketch.NumRetained()), ub)
		}
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, estimate)

		// Higher standard deviations should give higher upper bounds
		ub1, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		ub2, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		ub3, err := sketch.UpperBound(3)
		assert.NoError(t, err)

		assert.LessOrEqual(t, ub1, ub2)
		assert.LessOrEqual(t, ub2, ub3)
	})

	t.Run("Invalid NumStdDevs", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		_, err = sketch.UpperBound(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")

		_, err = sketch.UpperBound(4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")
	})
}

func TestQuickSelectUpdateSketch_IsEstimationMode(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, MaxTheta, sketch.Theta64())

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, MaxTheta, sketch.Theta64())
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())
	})
}

func TestQuickSelectUpdateSketch_Theta(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sketch.Theta())

	// Add enough items to trigger rebuild
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i))
	}

	theta := sketch.Theta()
	assert.Less(t, theta, 1.0)
}

func TestQuickSelectUpdateSketch_All(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	count := 0
	for range sketch.All() {
		count++
	}
	assert.Equal(t, 0, count)

	// Add items and verify iteration
	values := []int64{1, 2, 3, 4, 5}
	for _, v := range values {
		_ = sketch.UpdateInt64(v)
	}
	count = 0
	for range sketch.All() {
		count++
	}
	assert.Equal(t, len(values), count)

	seen := make(map[uint64]bool)
	for hash := range sketch.All() {
		seen[hash] = true
		assert.NotZero(t, hash)
	}
	assert.Equal(t, len(values), len(seen))

	// Test early break
	count = 0
	for range sketch.All() {
		count++
		if count == 2 {
			break
		}
	}
	assert.Equal(t, 2, count)
}

func TestQuickSelectUpdateSketch_String(t *testing.T) {
	t.Run("Without Items", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(8))
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		result := sketch.String(false)

		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "num retained entries : 10")
		assert.Contains(t, result, "empty?               : false")
		assert.Contains(t, result, "ordered?             : false")
		assert.Contains(t, result, "estimation mode?     : false")
		assert.Contains(t, result, "lg nominal size      : 8")
		assert.Contains(t, result, "### End sketch summary")

		assert.NotContains(t, result, "### Retained entries")
	})

	t.Run("With Items", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		_ = sketch.UpdateInt64(100)
		_ = sketch.UpdateInt64(200)

		result := sketch.String(true)

		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "num retained entries : 2")
		assert.Contains(t, result, "### End sketch summary")

		assert.Contains(t, result, "### Retained entries")
		assert.Contains(t, result, "### End retained entries")
	})
}

func TestQuickSelectUpdateSketch_SingleItem(t *testing.T) {
	updateSketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)
	assert.NoError(t, updateSketch.UpdateInt64(1))

	assert.False(t, updateSketch.IsEmpty())
	assert.False(t, updateSketch.IsEstimationMode())
	assert.Equal(t, 1.0, updateSketch.Theta())
	assert.Equal(t, 1.0, updateSketch.Estimate())
	lb, err := updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, lb)
	ub, err := updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ub)
	assert.True(t, updateSketch.IsOrdered())
}

func TestQuickSelectUpdateSketch_ResizeExact(t *testing.T) {
	updateSketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	for i := 0; i < 2000; i++ {
		assert.NoError(t, updateSketch.UpdateInt64(int64(i)))
	}

	assert.False(t, updateSketch.IsEmpty())
	assert.False(t, updateSketch.IsEstimationMode())
	assert.Equal(t, 1.0, updateSketch.Theta())
	assert.Equal(t, 2000.0, updateSketch.Estimate())
	lb, err := updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 2000.0, lb)
	ub, err := updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 2000.0, ub)
	assert.False(t, updateSketch.IsOrdered())

	updateSketch.Reset()
	assert.True(t, updateSketch.IsEmpty())
	assert.False(t, updateSketch.IsEstimationMode())
	assert.Equal(t, 1.0, updateSketch.Theta())
	assert.Equal(t, 0.0, updateSketch.Estimate())
	lb, err = updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, lb)
	ub, err = updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, ub)
	assert.True(t, updateSketch.IsOrdered())
}

func TestQuickSelectUpdateSketch_Estimation(t *testing.T) {
	updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchResizeFactor(ResizeX1))
	assert.NoError(t, err)

	n := 8000
	for i := 0; i < n; i++ {
		assertUpdate(t, updateSketch.UpdateInt64(int64(i)))
	}

	assert.False(t, updateSketch.IsEmpty())
	assert.True(t, updateSketch.IsEstimationMode())
	assert.Less(t, updateSketch.Theta(), 1.0)
	assert.InEpsilon(t, n, updateSketch.Estimate(), 0.01)
	lb, err := updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Less(t, lb, float64(n))
	ub, err := updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Greater(t, ub, float64(n))

	k := uint32(1) << DefaultLgK
	assert.GreaterOrEqual(t, updateSketch.NumRetained(), k)

	updateSketch.Trim()
	assert.Equal(t, k, updateSketch.NumRetained())
}

func TestUpdateSketch_Compact(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		compactSketch := updateSketch.Compact(true)
		assert.True(t, compactSketch.IsEmpty())
		assert.False(t, compactSketch.IsEstimationMode())
		assert.Equal(t, 1.0, compactSketch.Theta())
		assert.Equal(t, 0.0, compactSketch.Estimate())
		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
		assert.True(t, compactSketch.IsOrdered())

		assert.True(t, updateSketch.Compact(false).IsOrdered())
	})

	t.Run("Non Empty No Retained Keys", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.001))
		assert.NoError(t, err)
		assertUpdate(t, updateSketch.UpdateInt64(1))

		compactSketch := updateSketch.Compact(true)
		assert.Zero(t, compactSketch.NumRetained())
		assert.False(t, compactSketch.IsEmpty())
		assert.True(t, compactSketch.IsEstimationMode())
		assert.Equal(t, 0.0, compactSketch.Estimate())
		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Greater(t, ub, 0.0)
	})

	t.Run("Single Item", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		assert.NoError(t, updateSketch.UpdateInt64(1))

		compactSketch := updateSketch.Compact(true)
		assert.False(t, compactSketch.IsEmpty())
		assert.False(t, compactSketch.IsEstimationMode())
		assert.Equal(t, 1.0, compactSketch.Theta())
		assert.Equal(t, 1.0, compactSketch.Estimate())
		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, lb)
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, ub)
		assert.True(t, compactSketch.IsOrdered())

		assert.True(t, updateSketch.Compact(false).IsOrdered())
	})

	t.Run("Estimation", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchResizeFactor(ResizeX1))
		assert.NoError(t, err)

		n := 8000
		for i := 0; i < n; i++ {
			assertUpdate(t, updateSketch.UpdateInt64(int64(i)))
		}
		updateSketch.Trim()

		compactSketch := updateSketch.Compact(true)
		assert.False(t, compactSketch.IsEmpty())
		assert.True(t, compactSketch.IsOrdered())
		assert.True(t, compactSketch.IsEstimationMode())
		assert.Less(t, compactSketch.Theta(), 1.0)
		assert.InEpsilon(t, n, compactSketch.Estimate(), 0.01)

		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Less(t, lb, float64(n))
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, float64(n))
	})
}

func TestQuickSelectUpdateSketch_CompactTrimmed(t *testing.T) {
	const k = uint32(1) << DefaultLgK

	newSketch := func(t *testing.T, n int) *QuickSelectUpdateSketch {
		t.Helper()
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			assertUpdate(t, sketch.UpdateInt64(int64(i)))
		}
		return sketch
	}

	sortedEntries := func(s *CompactSketch) []uint64 {
		entries := slices.Collect(s.All())
		slices.Sort(entries)
		return entries
	}

	t.Run("Ordered And Trimmed Combinations", func(t *testing.T) {
		n := 40000
		sketch := newSketch(t, n)
		retainedBefore := sketch.NumRetained()
		thetaBefore := sketch.Theta64()
		assert.True(t, sketch.IsEstimationMode())
		assert.Greater(t, retainedBefore, k)

		reference := newSketch(t, n)
		reference.Trim()
		expected := reference.CompactOrdered()
		expectedEntries := sortedEntries(expected)
		assert.Equal(t, k, expected.NumRetained())

		// case 1: ordered, not trimmed
		c1 := sketch.Compact(true)
		assert.True(t, c1.IsOrdered())
		assert.Equal(t, retainedBefore, c1.NumRetained())
		assert.Equal(t, thetaBefore, c1.Theta64())

		// case 2: unordered, not trimmed
		c2 := sketch.Compact(false)
		assert.False(t, c2.IsOrdered())
		assert.Equal(t, retainedBefore, c2.NumRetained())
		assert.Equal(t, thetaBefore, c2.Theta64())

		// case 3: ordered and trimmed
		c3 := sketch.CompactTrimmed(true)
		assert.True(t, c3.IsOrdered())
		assert.Equal(t, k, c3.NumRetained())
		assert.Less(t, c3.Theta64(), thetaBefore)
		assert.Equal(t, expected.Theta64(), c3.Theta64())
		c3Entries := slices.Collect(c3.All())
		assert.True(t, slices.IsSorted(c3Entries))
		assert.Equal(t, expectedEntries, c3Entries)
		for _, entry := range c3Entries {
			assert.Less(t, entry, c3.Theta64())
		}

		// case 4: unordered and trimmed: same set and theta, no sort
		c4 := sketch.CompactTrimmed(false)
		assert.False(t, c4.IsOrdered())
		assert.Equal(t, k, c4.NumRetained())
		assert.Equal(t, expected.Theta64(), c4.Theta64())
		assert.Equal(t, expectedEntries, sortedEntries(c4))

		// the source sketch must be untouched by any of the four
		assert.Equal(t, retainedBefore, sketch.NumRetained())
		assert.Equal(t, thetaBefore, sketch.Theta64())
	})

	t.Run("Serialization Matches Trim Then Compact", func(t *testing.T) {
		n := 40000
		sketch := newSketch(t, n)
		reference := newSketch(t, n)
		reference.Trim()

		for _, compressed := range []bool{false, true} {
			var trimmedBuf, referenceBuf bytes.Buffer
			trimmedEncoder := NewEncoder(&trimmedBuf, compressed)
			assert.NoError(t, trimmedEncoder.Encode(sketch.CompactTrimmed(true)))
			referenceEncoder := NewEncoder(&referenceBuf, compressed)
			assert.NoError(t, referenceEncoder.Encode(reference.CompactOrdered()))

			assert.Equal(t, referenceBuf.Bytes(), trimmedBuf.Bytes(), "compressed=%v", compressed)
		}
	})

	t.Run("Exact Mode Converts To Estimation", func(t *testing.T) {
		n := 5000
		sketch := newSketch(t, n)
		assert.Greater(t, sketch.NumRetained(), k)
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, 1.0, sketch.Theta())

		exact := sketch.Compact(true)
		assert.False(t, exact.IsEstimationMode())
		assert.Equal(t, uint32(n), exact.NumRetained())
		assert.Equal(t, float64(n), exact.Estimate())

		trimmed := sketch.CompactTrimmed(true)
		assert.True(t, trimmed.IsEstimationMode())
		assert.Equal(t, k, trimmed.NumRetained())
		assert.Less(t, trimmed.Theta(), 1.0)
		assert.NotEqual(t, float64(n), trimmed.Estimate())

		lb, err := trimmed.LowerBound(3)
		assert.NoError(t, err)
		assert.LessOrEqual(t, lb, float64(n))
		ub, err := trimmed.UpperBound(3)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, float64(n))
	})

	t.Run("Bounds Widen In Estimation Mode", func(t *testing.T) {
		sketch := newSketch(t, 40000)
		assert.True(t, sketch.IsEstimationMode())
		assert.Greater(t, sketch.NumRetained(), k)

		plain := sketch.Compact(true)
		trimmed := sketch.CompactTrimmed(true)
		assert.Equal(t, k, trimmed.NumRetained())
		assert.Greater(t, plain.NumRetained(), trimmed.NumRetained())

		width := func(s *CompactSketch) float64 {
			lb, err := s.LowerBound(2)
			assert.NoError(t, err)
			ub, err := s.UpperBound(2)
			assert.NoError(t, err)
			return ub - lb
		}
		assert.Greater(t, width(trimmed), width(plain))
	})

	t.Run("Empty", func(t *testing.T) {
		sketch := newSketch(t, 0)

		result := sketch.CompactTrimmed(true)
		assert.True(t, result.IsEmpty())
		assert.Zero(t, result.NumRetained())
		assert.Equal(t, 1.0, result.Theta())
		assert.True(t, result.IsOrdered())
		assert.True(t, sketch.CompactTrimmed(false).IsOrdered())
	})

	t.Run("Non Empty No Retained Keys", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.001))
		assert.NoError(t, err)
		assertUpdate(t, sketch.UpdateInt64(1))

		result := sketch.CompactTrimmed(true)
		assert.False(t, result.IsEmpty())
		assert.Zero(t, result.NumRetained())
		assert.Equal(t, sketch.Theta64(), result.Theta64())
		assert.True(t, result.IsEstimationMode())
	})

	t.Run("Below K", func(t *testing.T) {
		sketch := newSketch(t, 100)
		assert.False(t, sketch.IsEstimationMode())

		result := sketch.CompactTrimmed(true)
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, uint32(100), result.NumRetained())
		assert.Equal(t, sketch.Theta64(), result.Theta64())
		assert.Equal(t, 100.0, result.Estimate())
		assert.True(t, slices.IsSorted(slices.Collect(result.All())))
		assert.False(t, sketch.CompactTrimmed(false).IsOrdered())

		single := newSketch(t, 1)
		assert.True(t, single.CompactTrimmed(false).IsOrdered())
	})
}
