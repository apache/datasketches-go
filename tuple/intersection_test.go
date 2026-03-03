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

// sumPolicy is a test policy that sums the values of matching summaries.
type sumPolicy struct{}

func (p *sumPolicy) Apply(internalSummary *int32Summary, incomingSummary *int32Summary) {
	internalSummary.value += incomingSummary.value
}

func TestIntersection(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		intersection := NewIntersection[*int32Summary](&sumPolicy{})
		assert.False(t, intersection.HasResult())

		_, err := intersection.Result(true)
		assert.NotNil(t, err)
	})

	t.Run("Empty, Empty", func(t *testing.T) {
		intersection := NewIntersection[*int32Summary](&sumPolicy{})
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)

		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
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
		intersection := NewIntersection[*int32Summary](&sumPolicy{})
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
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

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
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
		empty, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		exact, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		empty, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		exact, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](
			newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](
			newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](
			newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](
			newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](
			newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](
			newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4), 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4), 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4), 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4), 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3), 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6), 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3), 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6), 1)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
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
		sketch, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		sketch.UpdateInt64(1, 1) // non-empty should not be ignored

		intersection := NewIntersection[*int32Summary](&sumPolicy{}, WithIntersectionSeed(123))
		err = intersection.Update(sketch)
		assert.ErrorContains(t, err, "seed hash mismatch")
	})

	t.Run("Policy", func(t *testing.T) {
		policy := &sumPolicy{}
		intersection := NewIntersection[*int32Summary](policy)

		returnedPolicy := intersection.Policy()
		assert.NotNil(t, returnedPolicy)
		assert.Equal(t, policy, returnedPolicy)
	})

	t.Run("OrderedResult Method", func(t *testing.T) {
		sketch1, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i), 1)
		}

		sketch2, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i), 1)
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.OrderedResult()
		assert.NoError(t, err)
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 50.0, result.Estimate())
	})

	t.Run("Summary Sum Policy Applied", func(t *testing.T) {
		sketch1, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		sketch1.UpdateInt64(1, 10)
		sketch1.UpdateInt64(2, 20)
		sketch1.UpdateInt64(3, 30)

		sketch2, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		sketch2.UpdateInt64(2, 5)
		sketch2.UpdateInt64(3, 15)
		sketch2.UpdateInt64(4, 25)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		// Keys 2 and 3 are common
		assert.Equal(t, uint32(2), result.NumRetained())

		totalSum := int32(0)
		for _, summary := range result.All() {
			totalSum += summary.value
		}
		// Key 2: 20 + 5 = 25, Key 3: 30 + 15 = 45, Total = 70
		assert.Equal(t, int32(70), totalSum)
	})

	t.Run("With Custom Seed", func(t *testing.T) {
		customSeed := uint64(12345)

		sketch1, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchSeed(customSeed))
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i), 1)
		}

		sketch2, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchSeed(customSeed))
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i), 1)
		}

		intersection := NewIntersection[*int32Summary](&sumPolicy{}, WithIntersectionSeed(customSeed))
		err := intersection.Update(sketch1)
		assert.NoError(t, err)
		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 50.0, result.Estimate())
	})
}

// keepFirstPolicy is a test policy that keeps the first summary value.
type keepFirstPolicy struct{}

func (p *keepFirstPolicy) Apply(internalSummary *int32Summary, incomingSummary *int32Summary) {}

func TestIntersectionWithDifferentPolicies(t *testing.T) {
	t.Run("Keep First Policy", func(t *testing.T) {
		sketch1, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		sketch1.UpdateInt64(1, 100)
		sketch1.UpdateInt64(2, 200)

		sketch2, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		sketch2.UpdateInt64(1, 1)
		sketch2.UpdateInt64(2, 2)

		intersection := NewIntersection[*int32Summary](&keepFirstPolicy{})
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		totalSum := int32(0)
		for _, summary := range result.All() {
			totalSum += summary.value
		}
		assert.Equal(t, int32(300), totalSum) // 100 + 200
	})

	t.Run("Sum Policy", func(t *testing.T) {
		sketch1, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		sketch1.UpdateInt64(1, 100)
		sketch1.UpdateInt64(2, 200)

		sketch2, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		sketch2.UpdateInt64(1, 1)
		sketch2.UpdateInt64(2, 2)

		intersection := NewIntersection[*int32Summary](&sumPolicy{})
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		totalSum := int32(0)
		for _, summary := range result.All() {
			totalSum += summary.value
		}
		assert.Equal(t, int32(303), totalSum) // (100+1) + (200+2)
	})

	t.Run("Sum With SummaryMergeFunc", func(t *testing.T) {
		sketch1, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
		)
		assert.NoError(t, err)
		sketch1.UpdateInt64(1, 100)
		sketch1.UpdateInt64(2, 200)
		sketch1.UpdateInt64(3, 300)

		sketch2, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
		)
		assert.NoError(t, err)
		sketch2.UpdateInt64(2, 5)
		sketch2.UpdateInt64(3, 15)
		sketch2.UpdateInt64(4, 25)

		intersection := NewIntersectionWithSummaryMergeFunc[int32ValueSummary](
			func(internal, incoming int32ValueSummary) int32ValueSummary {
				internal.value += incoming.value
				return internal
			},
		)
		err = intersection.Update(sketch1)
		assert.NoError(t, err)
		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(2), result.NumRetained())

		totalSum := int32(0)
		for _, summary := range result.All() {
			totalSum += summary.value
		}
		// Key 2: 200 + 5 = 205, Key 3: 300 + 15 = 315, Total = 520
		assert.Equal(t, int32(520), totalSum)
	})

	t.Run("Exact Mode Half Overlap With SummaryMergeFunc", func(t *testing.T) {
		sketch1, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
		)
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
		)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		intersection := NewIntersectionWithSummaryMergeFunc[int32ValueSummary](
			func(internal, incoming int32ValueSummary) int32ValueSummary {
				internal.value += incoming.value
				return internal
			},
		)
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
}
