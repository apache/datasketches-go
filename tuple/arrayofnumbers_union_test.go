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

type arrayOfNumbersUnionSumPolicy[V Number] struct{}

func (p *arrayOfNumbersUnionSumPolicy[V]) Apply(internalSummary *ArrayOfNumbersSummary[V], incomingSummary *ArrayOfNumbersSummary[V]) {
	internalSummary.Update(incomingSummary.Values())
}

func TestArrayOfNumbersSketchUnion(t *testing.T) {
	t.Run("Empty, Empty", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)

		err = union.Update(sketch1)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		err = union.Update(sketch2)
		assert.NoError(t, err)

		result, err = union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
	})

	t.Run("Empty Compact, Empty Compact", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)

		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		err = union.Update(compact1)
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = union.Update(compact2)
		assert.NoError(t, err)

		result, err = union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
	})

	t.Run("Empty, Exact", func(t *testing.T) {
		empty, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(empty)
		assert.NoError(t, err)
		err = union.Update(exact)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Exact Compact", func(t *testing.T) {
		empty, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		emptyCompact, err := empty.Compact(false)
		assert.NoError(t, err)
		exactCompact, err := exact.Compact(false)
		assert.NoError(t, err)
		err = union.Update(emptyCompact)
		assert.NoError(t, err)
		err = union.Update(exactCompact)
		assert.NoError(t, err)

		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = union.Update(aCompact)
		assert.NoError(t, err)
		err = union.Update(bCompact)
		assert.NoError(t, err)

		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty, Estimation Mode", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)
		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Estimation Mode Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = union.Update(aCompact)
		assert.NoError(t, err)
		err = union.Update(bCompact)
		assert.NoError(t, err)

		result, err := union.Result(false)
		assert.NoError(t, err)
		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = union.Update(aCompact)
		assert.NoError(t, err)
		err = union.Update(bCompact)
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = union.Update(aCompact)
		assert.NoError(t, err)
		err = union.Update(bCompact)
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)

		err = union.Update(sketch2)
		assert.NoError(t, err)

		sketch3, err := union.Result(true)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
		assert.Equal(t, 1500.0, sketch3.Estimate())

		// Test reset
		union.Reset()
		sketch3, err = union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), sketch3.NumRetained())
		assert.True(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
	})

	t.Run("Exact Mode Compact Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = union.Update(compact1)
		assert.NoError(t, err)

		err = union.Update(compact2)
		assert.NoError(t, err)

		sketch3, err := union.Result(false)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
		assert.Equal(t, 1500.0, sketch3.Estimate())
	})

	t.Run("Estimation Mode Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)

		err = union.Update(sketch2)
		assert.NoError(t, err)

		sketch3, err := union.Result(true)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.True(t, sketch3.IsEstimationMode())

		assert.InEpsilon(t, 15000.0, sketch3.Estimate(), 0.01)

		// Test reset
		union.Reset()
		sketch3, err = union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), sketch3.NumRetained())
		assert.True(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
	})

	t.Run("Estimation Mode Compact Half Overlap", func(t *testing.T) {
		sketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		sketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		compact1, err := sketch1.Compact(false)
		assert.NoError(t, err)
		compact2, err := sketch2.Compact(false)
		assert.NoError(t, err)
		err = union.Update(compact1)
		assert.NoError(t, err)

		err = union.Update(compact2)
		assert.NoError(t, err)

		sketch3, err := union.Result(false)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.True(t, sketch3.IsEstimationMode())

		assert.InEpsilon(t, 15000.0, sketch3.Estimate(), 0.01)
	})

	t.Run("Exact Mode, Estimation Mode Full Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Compact, Estimation Mode Compact Full Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = union.Update(aCompact)
		assert.NoError(t, err)
		err = union.Update(bCompact)
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Non Empty No Retained Keys, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(3, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Non Empty No Retained Keys Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(3, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		assert.NoError(t, err)
		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)
		err = union.Update(aCompact)
		assert.NoError(t, err)
		err = union.Update(bCompact)
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Seed Mismatch", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		sketch.UpdateInt64(1, []float64{1.0, 2.0}) // non-empty should not be ignored

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionSeed(123))
		assert.NoError(t, err)
		err = union.Update(sketch)
		assert.ErrorContains(t, err, "seed hash mismatch")
	})

	t.Run("NumValuesInSummary Mismatch", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](3) // Different numValuesInSummary
		assert.NoError(t, err)
		sketch.UpdateInt64(1, []float64{1.0, 2.0, 3.0})

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2) // Expects 2
		assert.NoError(t, err)
		err = union.Update(sketch)
		assert.ErrorContains(t, err, "numValuesInSummary does not match")
	})

	t.Run("Larger K", func(t *testing.T) {
		updateSketch1, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 16384; i++ {
			updateSketch1.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		updateSketch2, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 26384; i++ {
			updateSketch2.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		updateSketch3, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 86384; i++ {
			updateSketch3.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		// First union
		union1, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(16))
		assert.NoError(t, err)
		err = union1.Update(updateSketch2)
		assert.NoError(t, err)
		err = union1.Update(updateSketch1)
		assert.NoError(t, err)
		err = union1.Update(updateSketch3)
		assert.NoError(t, err)

		result1, err := union1.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, updateSketch3.Estimate(), result1.Estimate())

		// Second union with different order
		union2, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(16))
		assert.NoError(t, err)
		err = union2.Update(updateSketch1)
		assert.NoError(t, err)
		err = union2.Update(updateSketch3)
		assert.NoError(t, err)
		err = union2.Update(updateSketch2)
		assert.NoError(t, err)

		result2, err := union2.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, updateSketch3.Estimate(), result2.Estimate())
	})

	t.Run("lgK Too Small", func(t *testing.T) {
		_, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(theta.MinLgK-1))
		assert.ErrorContains(t, err, "lgK must not be less than")
	})

	t.Run("lgK Too Large", func(t *testing.T) {
		_, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(theta.MaxLgK+1))
		assert.ErrorContains(t, err, "lgK must not be greater than")
	})

	t.Run("P Is Zero", func(t *testing.T) {
		_, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionSketchP(0))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("P Is Negative", func(t *testing.T) {
		_, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionSketchP(-0.5))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("P Greater Than 1", func(t *testing.T) {
		_, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionSketchP(1.5))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("Valid Minimum lgK", func(t *testing.T) {
		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(theta.MinLgK))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("Valid Maximum lgK", func(t *testing.T) {
		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(theta.MaxLgK))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("With Resize Factor", func(t *testing.T) {
		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionResizeFactor(theta.ResizeX4))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("Result With QuickSelect Triggered", func(t *testing.T) {
		union, _ := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionLgK(8))

		sketch, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(10))
		for i := 0; i < 2000; i++ {
			sketch.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		union.Update(sketch)
		result, err := union.Result(true)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Result should have at most nominalNum entries
		assert.LessOrEqual(t, result.NumRetained(), uint32(256))
	})

	t.Run("OrderedResult Method", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(12))
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}

		union, _ := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		union.Update(sketch1)
		union.Update(sketch2)

		result, err := union.OrderedResult()
		assert.NoError(t, err)
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 150.0, result.Estimate())
	})

	t.Run("Summary Sum Policy Applied", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		sketch1.UpdateInt64(1, []float64{10.0, 100.0})
		sketch1.UpdateInt64(2, []float64{20.0, 200.0})

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		sketch2.UpdateInt64(1, []float64{5.0, 50.0})
		sketch2.UpdateInt64(2, []float64{15.0, 150.0})
		sketch2.UpdateInt64(3, []float64{25.0, 250.0})

		union, _ := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2)
		union.Update(sketch1)
		union.Update(sketch2)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(3), result.NumRetained())

		totalSum := []float64{0.0, 0.0}
		for _, summary := range result.All() {
			values := summary.Values()
			totalSum[0] += values[0]
			totalSum[1] += values[1]
		}
		// Key 1: (10+5, 100+50) = (15, 150)
		// Key 2: (20+15, 200+150) = (35, 350)
		// Key 3: (25, 250)
		// Total = (75, 750)
		assert.InDelta(t, 75.0, totalSum[0], 1e-10)
		assert.InDelta(t, 750.0, totalSum[1], 1e-10)
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

		union, err := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 2, WithUnionSeed(customSeed))
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)
		err = union.Update(sketch2)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 150.0, result.Estimate())
	})

	t.Run("NumValuesInSummary Preserved", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[float64](3)
		sketch1.UpdateInt64(1, []float64{1.0, 2.0, 3.0})
		sketch1.UpdateInt64(2, []float64{4.0, 5.0, 6.0})

		sketch2, _ := NewArrayOfNumbersUpdateSketch[float64](3)
		sketch2.UpdateInt64(1, []float64{7.0, 8.0, 9.0})
		sketch2.UpdateInt64(2, []float64{10.0, 11.0, 12.0})

		union, _ := NewArrayOfNumbersSketchUnion[float64](&arrayOfNumbersUnionSumPolicy[float64]{}, 3)
		union.Update(sketch1)
		union.Update(sketch2)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint8(3), result.NumValuesInSummary())
	})

	t.Run("Different Number Types Int32", func(t *testing.T) {
		sketch1, _ := NewArrayOfNumbersUpdateSketch[int32](2)
		sketch1.UpdateInt64(1, []int32{10, 20})
		sketch1.UpdateInt64(2, []int32{30, 40})

		sketch2, _ := NewArrayOfNumbersUpdateSketch[int32](2)
		sketch2.UpdateInt64(1, []int32{5, 10})
		sketch2.UpdateInt64(3, []int32{15, 20})

		union, _ := NewArrayOfNumbersSketchUnion[int32](&arrayOfNumbersUnionSumPolicy[int32]{}, 2)
		union.Update(sketch1)
		union.Update(sketch2)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(3), result.NumRetained())

		totalSum := []int32{0, 0}
		for _, summary := range result.All() {
			values := summary.Values()
			totalSum[0] += values[0]
			totalSum[1] += values[1]
		}
		// Key 1: (10+5, 20+10) = (15, 30)
		// Key 2: (30, 40)
		// Key 3: (15, 20)
		// Total = (60, 90)
		assert.Equal(t, int32(60), totalSum[0])
		assert.Equal(t, int32(90), totalSum[1])
	})
}
