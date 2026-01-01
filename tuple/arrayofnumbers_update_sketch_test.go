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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/theta"
)

func TestNewArrayOfNumbersUpdateSketch(t *testing.T) {
	t.Run("Default And Empty", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		assert.True(t, sketch.IsEmpty())
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, 1.0, sketch.Theta())
		assert.Equal(t, 0.0, sketch.Estimate())
		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
		assert.True(t, sketch.IsOrdered())
	})

	t.Run("With Options", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			3,
			WithUpdateSketchLgK(10),
			WithUpdateSketchResizeFactor(theta.ResizeX2),
			WithUpdateSketchP(0.5),
			WithUpdateSketchSeed(12345),
		)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint8(10), sketch.LgK())
		assert.Equal(t, theta.ResizeX2, sketch.ResizeFactor())
		assert.Equal(t, float32(0.5), sketch.table.p)
		assert.Equal(t, uint64(12345), sketch.table.seed)
	})

	t.Run("Non Empty No Retained Keys", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchP(0.001),
		)
		assert.NoError(t, err)
		sketch.UpdateInt64(1, []float64{1.0, 2.0})

		assert.Zero(t, sketch.NumRetained())
		assert.False(t, sketch.IsEmpty())
		assert.True(t, sketch.IsEstimationMode())
		assert.Equal(t, 0.0, sketch.Estimate())
		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Greater(t, ub, 0.0)

		sketch.Reset()
		assert.True(t, sketch.IsEmpty())
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, 0.0, sketch.Estimate())
		lb, err = sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err = sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
	})

	t.Run("Invalid LgK", func(t *testing.T) {
		_, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(3),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lg_k must not be less than")

		_, err = NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(30),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lg_k must not be greater than")
	})

	t.Run("Invalid P", func(t *testing.T) {
		_, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchP(0.0),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sampling probability must be between 0 and 1")

		_, err = NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchP(1.5),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sampling probability must be between 0 and 1")
	})
}

func TestArrayOfNumbersUpdateSketch_Theta64(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](
		2, WithUpdateSketchLgK(5),
	)
	assert.NoError(t, err)

	initialTheta := sketch.Theta64()
	assert.Equal(t, theta.MaxTheta, initialTheta)

	// Insert many values to trigger rebuild
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
	}

	assert.Less(t, sketch.Theta64(), initialTheta)
}

func TestArrayOfNumbersUpdateSketch_SeedHash(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](
		2, WithUpdateSketchSeed(12345),
	)
	assert.NoError(t, err)

	seedHash, err := sketch.SeedHash()
	assert.NoError(t, err)
	assert.NotZero(t, seedHash)
}

func TestArrayOfNumbersUpdateSketch_UpdateUint32(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateUint32(100, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.values)
	}

	// using the same key
	err = sketch.UpdateUint32(100, []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.values)
	}
}

func TestArrayOfNumbersUpdateSketch_UpdateInt64(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateInt64(-100, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.values)
	}

	// using the same key
	err = sketch.UpdateInt64(-100, []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.Values())
	}
}

func TestArrayOfNumbersUpdateSketch_UpdateInt32(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateInt32(42, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.Values())
	}

	// using the same key
	err = sketch.UpdateInt32(42, []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.Values())
	}
}

func TestArrayOfNumbersUpdateSketch_UpdateString(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateString("", []float64{10.0, 20.0})
	assert.ErrorIs(t, err, ErrUpdateEmptyString)

	err = sketch.UpdateString("hello", []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.Values())
	}

	// using the same key
	err = sketch.UpdateString("hello", []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.Values())
	}
}

func TestArrayOfNumbersUpdateSketch_UpdateBytes(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateBytes([]byte{1, 2, 3}, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.Values())
	}

	// using the same key
	err = sketch.UpdateBytes([]byte{1, 2, 3}, []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.Values())
	}
}

func TestArrayOfNumbersUpdateSketch_UpdateFloat64(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateFloat64(3.14, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.Values())
	}

	// using the same key
	err = sketch.UpdateFloat64(3.14, []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.Values())
	}
}

func TestArrayOfNumbersUpdateSketch_UpdateFloat32(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	err = sketch.UpdateFloat32(3.14, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{10.0, 20.0}, summary.Values())
	}

	// using the same key
	err = sketch.UpdateFloat32(3.14, []float64{5.0, 10.0})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())
	for _, summary := range sketch.All() {
		assert.Equal(t, []float64{15.0, 30.0}, summary.Values())
	}
}

func TestArrayOfNumbersUpdateSketch_Estimate(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	assert.Equal(t, 0.0, sketch.Estimate())

	err = sketch.UpdateInt64(1, []float64{10.0, 20.0})
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sketch.Estimate())
}

func TestArrayOfNumbersUpdateSketch_LowerBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
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
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
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
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		_, err = sketch.LowerBound(0)
		assert.ErrorContains(t, err, "numStdDevs must be 1, 2 or 3")

		_, err = sketch.LowerBound(4)
		assert.ErrorContains(t, err, "numStdDevs must be 1, 2 or 3")
	})
}

func TestArrayOfNumbersUpdateSketch_UpperBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
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
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
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
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5),
		)
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		_, err = sketch.UpperBound(0)
		assert.ErrorContains(t, err, "numStdDevs must be 1, 2 or 3")

		_, err = sketch.UpperBound(4)
		assert.ErrorContains(t, err, "numStdDevs must be 1, 2 or 3")
	})
}

func TestArrayOfNumbersUpdateSketch_IsEstimationMode(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, theta.MaxTheta, sketch.Theta64())

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, theta.MaxTheta, sketch.Theta64())
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		assert.True(t, sketch.IsEstimationMode())
	})
}

func TestArrayOfNumbersUpdateSketch_Theta(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](
		2, WithUpdateSketchLgK(5),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sketch.Theta())

	// Add enough items to trigger rebuild
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
	}

	thetaVal := sketch.Theta()
	assert.Less(t, thetaVal, 1.0)
}

func TestArrayOfNumbersUpdateSketch_All(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	count := 0
	for range sketch.All() {
		count++
	}
	assert.Equal(t, 0, count)

	values := []int64{1, 2, 3, 4, 5}
	for _, v := range values {
		_ = sketch.UpdateInt64(v, []float64{float64(v), float64(v * 2)})
	}
	count = 0
	for range sketch.All() {
		count++
	}
	assert.Equal(t, len(values), count)

	seen := make(map[uint64]bool)
	for hash, summary := range sketch.All() {
		seen[hash] = true
		assert.NotZero(t, hash)
		assert.Len(t, summary.Values(), 2)
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

func TestArrayOfNumbersUpdateSketch_String(t *testing.T) {
	t.Run("Without Items", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(8),
		)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		result := sketch.String(false)

		assert.Contains(t, result, "### Tuple sketch summary:")
		assert.Contains(t, result, "num retained hashes : 10")
		assert.Contains(t, result, "empty?               : false")
		assert.Contains(t, result, "ordered?             : false")
		assert.Contains(t, result, "estimation mode?     : false")
		assert.Contains(t, result, "lg nominal size      : 8")
		assert.Contains(t, result, "### End sketch summary")

		assert.NotContains(t, result, "### Retained entries")
	})

	t.Run("With Items", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		_ = sketch.UpdateInt64(100, []float64{10.0, 20.0})
		_ = sketch.UpdateInt64(200, []float64{30.0, 40.0})

		result := sketch.String(true)

		assert.Contains(t, result, "### Tuple sketch summary:")
		assert.Contains(t, result, "num retained hashes : 2")
		assert.Contains(t, result, "### End sketch summary")

		assert.Contains(t, result, "### Retained entries")
		assert.Contains(t, result, "### End retained entries")
	})
}

func TestArrayOfNumbersUpdateSketch_SingleItem(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)
	sketch.UpdateInt64(1, []float64{10.0, 20.0})

	assert.False(t, sketch.IsEmpty())
	assert.False(t, sketch.IsEstimationMode())
	assert.Equal(t, 1.0, sketch.Theta())
	assert.Equal(t, 1.0, sketch.Estimate())
	lb, err := sketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, lb)
	ub, err := sketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ub)
	assert.True(t, sketch.IsOrdered())
}

func TestArrayOfNumbersUpdateSketch_Reset(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](1)
	assert.NoError(t, err)
	sketch.UpdateInt64(1, []float64{1.0})

	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())

	sketch.Reset()

	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, uint32(0), sketch.NumRetained())
}

func TestArrayOfNumbersUpdateSketch_ResizeExact(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)

	for i := 0; i < 2000; i++ {
		sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
	}

	assert.False(t, sketch.IsEmpty())
	assert.False(t, sketch.IsEstimationMode())
	assert.Equal(t, 1.0, sketch.Theta())
	assert.Equal(t, 2000.0, sketch.Estimate())
	lb, err := sketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 2000.0, lb)
	ub, err := sketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 2000.0, ub)
	assert.False(t, sketch.IsOrdered())

	sketch.Reset()
	assert.True(t, sketch.IsEmpty())
	assert.False(t, sketch.IsEstimationMode())
	assert.Equal(t, 0.0, sketch.Estimate())
	lb, err = sketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, lb)
	ub, err = sketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, ub)
	assert.True(t, sketch.IsOrdered())
}

func TestArrayOfNumbersUpdateSketch_Estimation(t *testing.T) {
	lgK := uint8(5)
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](
		2,
		WithUpdateSketchLgK(lgK),
		WithUpdateSketchResizeFactor(theta.ResizeX2),
	)
	assert.NoError(t, err)

	n := 200
	for i := 0; i < n; i++ {
		sketch.UpdateString(fmt.Sprintf("key%d", i), []float64{1.0, 2.0})
	}

	assert.False(t, sketch.IsEmpty())
	assert.True(t, sketch.IsEstimationMode())
	assert.Less(t, sketch.Theta(), 1.0)

	estimate := sketch.Estimate()
	lb, err := sketch.LowerBound(1)
	assert.NoError(t, err)
	assert.LessOrEqual(t, lb, estimate)
	ub, err := sketch.UpperBound(1)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, ub, estimate)

	k := uint32(1) << lgK
	assert.GreaterOrEqual(t, sketch.NumRetained(), k)

	sketch.Trim()
	assert.Equal(t, k, sketch.NumRetained())
}

func TestArrayOfNumbersUpdateSketch_DifferentNumberTypes(t *testing.T) {
	t.Run("Int32", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[int32](3)
		assert.NoError(t, err)

		err = sketch.UpdateInt64(100, []int32{1, 2, 3})
		assert.NoError(t, err)

		err = sketch.UpdateInt64(100, []int32{4, 5, 6})
		assert.NoError(t, err)

		assert.Equal(t, uint32(1), sketch.NumRetained())
		for _, summary := range sketch.All() {
			assert.Equal(t, []int32{5, 7, 9}, summary.Values())
		}
	})

	t.Run("Int64", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[int64](2)
		assert.NoError(t, err)

		err = sketch.UpdateInt64(100, []int64{100, 200})
		assert.NoError(t, err)

		err = sketch.UpdateInt64(100, []int64{50, 100})
		assert.NoError(t, err)

		assert.Equal(t, uint32(1), sketch.NumRetained())
		for _, summary := range sketch.All() {
			assert.Equal(t, []int64{150, 300}, summary.Values())
		}
	})

	t.Run("Float32", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float32](2)
		assert.NoError(t, err)

		err = sketch.UpdateInt64(100, []float32{1.5, 2.5})
		assert.NoError(t, err)

		err = sketch.UpdateInt64(100, []float32{0.5, 0.5})
		assert.NoError(t, err)

		assert.Equal(t, uint32(1), sketch.NumRetained())
		for _, summary := range sketch.All() {
			assert.Equal(t, []float32{2.0, 3.0}, summary.Values())
		}
	})
}

func TestArrayOfNumbersUpdateSketch_MultipleValues(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](5)
	assert.NoError(t, err)

	err = sketch.UpdateInt64(1, []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	assert.NoError(t, err)

	err = sketch.UpdateInt64(2, []float64{10.0, 20.0, 30.0, 40.0, 50.0})
	assert.NoError(t, err)

	assert.Equal(t, uint32(2), sketch.NumRetained())

	totalSum := make([]float64, 5)
	for _, summary := range sketch.All() {
		for i, v := range summary.Values() {
			totalSum[i] += v
		}
	}
	assert.Equal(t, []float64{11.0, 22.0, 33.0, 44.0, 55.0}, totalSum)
}
