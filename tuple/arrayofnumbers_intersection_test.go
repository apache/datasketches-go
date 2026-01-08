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
)

type arrayOfNumberSumPolicy[V Number] struct{}

func (p *arrayOfNumberSumPolicy[V]) Apply(internalSummary *ArrayOfNumbersSummary[V], incomingSummary *ArrayOfNumbersSummary[V]) {
	internalSummary.Update(incomingSummary.Values())
}

func TestArrayOfNumberSketchIntersection(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		assert.False(t, intersection.HasResult())

		_, err := intersection.Result(true)
		assert.NotNil(t, err)
	})

	t.Run("Empty, Empty", func(t *testing.T) {
		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err = intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Empty Compact, Empty Compact", func(t *testing.T) {
		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(compact1)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(compact2)
		assert.NoError(t, err)

		result, err = intersection.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Empty, Exact", func(t *testing.T) {
		empty, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		exact, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(empty)
		assert.NoError(t, err)
		err = intersection.Update(exact)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Exact Compact", func(t *testing.T) {
		empty, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		exact, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		emptyCompact, err := empty.Compact(false)
		assert.NoError(t, err)
		exactCompact, err := exact.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(emptyCompact)
		assert.NoError(t, err)
		err = intersection.Update(exactCompact)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(aCompact)
		assert.NoError(t, err)
		err = intersection.Update(bCompact)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty, Estimation Mode", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Estimation Mode Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(aCompact)
		assert.NoError(t, err)
		err = intersection.Update(bCompact)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Exact, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(aCompact)
		assert.NoError(t, err)
		err = intersection.Update(bCompact)
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Estimation Mode, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Estimation Mode Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)
		err = intersection.Update(aCompact)
		assert.NoError(t, err)
		err = intersection.Update(bCompact)
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(compact1)
		assert.NoError(t, err)

		err = intersection.Update(compact2)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Half Overlap Ordered", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		compact1, err := sketch1.Compact(true)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(true)
		assert.NoError(t, err)
		err = intersection.Update(compact1)
		assert.NoError(t, err)

		err = intersection.Update(compact2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Disjoint", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Disjoint", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(compact1)
		assert.NoError(t, err)

		err = intersection.Update(compact2)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		assert.InEpsilon(t, 5000.0, result.Estimate(), 0.02)
	})

	t.Run("Estimation Mode Compact Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(compact1)
		assert.NoError(t, err)

		err = intersection.Update(compact2)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		assert.InEpsilon(t, 5000.0, result.Estimate(), 0.02)
	})

	t.Run("Estimation Mode Disjoint", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Compact Disjoint", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(compact1)
		assert.NoError(t, err)

		err = intersection.Update(compact2)
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Exact Mode, Estimation Mode Full Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4), []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Compact, Estimation Mode Compact Full Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4), []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(aCompact)
		assert.NoError(t, err)
		err = intersection.Update(bCompact)
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Non Empty No Retained Keys, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6), []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Non Empty No Retained Keys Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6), []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = intersection.Update(aCompact)
		assert.NoError(t, err)
		err = intersection.Update(bCompact)
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Seed Mismatch", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		sketch.UpdateInt64(1, []float64{1.0, 2.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2, WithIntersectionSeed(123))
		err = intersection.Update(sketch)
		assert.ErrorContains(t, err, "seed hash mismatch")
	})

	t.Run("NumValuesInSummary Mismatch", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](3)
		assert.NoError(t, err)
		sketch.UpdateInt64(1, []float64{1.0, 2.0, 3.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2) // Expects 2
		err = intersection.Update(sketch)
		assert.ErrorContains(t, err, "numValuesInSummary does not match")
	})

	t.Run("Policy", func(t *testing.T) {
		policy := &arrayOfNumberSumPolicy[float64]{}
		intersection := NewArrayOfNumbersSketchIntersection[float64](policy, 2)

		returnedPolicy := intersection.Policy()
		assert.NotNil(t, returnedPolicy)
		assert.Equal(t, policy, returnedPolicy)
	})

	t.Run("OrderedResult Method", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.OrderedResult()
		assert.NoError(t, err)
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 50.0, result.Estimate())
	})

	t.Run("Summary Sum Policy Applied", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		sketch1.UpdateInt64(1, []float64{10.0, 100.0})
		sketch1.UpdateInt64(2, []float64{20.0, 200.0})
		sketch1.UpdateInt64(3, []float64{30.0, 300.0})

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		sketch2.UpdateInt64(2, []float64{5.0, 50.0})
		sketch2.UpdateInt64(3, []float64{15.0, 150.0})
		sketch2.UpdateInt64(4, []float64{25.0, 250.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2)
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		// Keys 2 and 3 are common
		assert.Equal(t, uint32(2), result.NumRetained())

		totalSum := []float64{0.0, 0.0}
		for _, summary := range result.All() {
			values := summary.Values()
			totalSum[0] += values[0]
			totalSum[1] += values[1]
		}
		// Key 2: (20+5, 200+50) = (25, 250), Key 3: (30+15, 300+150) = (45, 450)
		// Total = (70, 700)
		assert.InDelta(t, 70.0, totalSum[0], 1e-10)
		assert.InDelta(t, 700.0, totalSum[1], 1e-10)
	})

	t.Run("With Custom Seed", func(t *testing.T) {
		customSeed := uint64(12345)

		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(customSeed))
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(customSeed))
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 2, WithIntersectionSeed(customSeed))
		err := intersection.Update(sketch1)
		assert.NoError(t, err)
		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 50.0, result.Estimate())
	})

	t.Run("NumValuesInSummary Preserved", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](3)
		sketch1.UpdateInt64(1, []float64{1.0, 2.0, 3.0})
		sketch1.UpdateInt64(2, []float64{4.0, 5.0, 6.0})

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](3)
		sketch2.UpdateInt64(1, []float64{7.0, 8.0, 9.0})
		sketch2.UpdateInt64(2, []float64{10.0, 11.0, 12.0})

		intersection := NewArrayOfNumbersSketchIntersection[float64](&arrayOfNumberSumPolicy[float64]{}, 3)
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint8(3), result.NumValuesInSummary())
	})

	t.Run("Different Number Types Int32", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[int32](2)
		sketch1.UpdateInt64(1, []int32{10, 20})
		sketch1.UpdateInt64(2, []int32{30, 40})

		sketch2, _ := NewArrayOfNumbersUpdateSketch[int32](2)
		sketch2.UpdateInt64(1, []int32{5, 10})
		sketch2.UpdateInt64(2, []int32{15, 20})

		intersection := NewArrayOfNumbersSketchIntersection[int32](&arrayOfNumberSumPolicy[int32]{}, 2)
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(2), result.NumRetained())

		totalSum := []int32{0, 0}
		for _, summary := range result.All() {
			values := summary.Values()
			totalSum[0] += values[0]
			totalSum[1] += values[1]
		}
		// Key 1: (10+5, 20+10) = (15, 30), Key 2: (30+15, 40+20) = (45, 60)
		// Total = (60, 90)
		assert.Equal(t, int32(60), totalSum[0])
		assert.Equal(t, int32(90), totalSum[1])
	})
}
