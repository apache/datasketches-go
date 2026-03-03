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

// unionSumPolicy sums the values of matching summaries during union
type unionSumPolicy struct{}

func (p *unionSumPolicy) Apply(internalSummary *int32Summary, incomingSummary *int32Summary) {
	internalSummary.value += incomingSummary.value
}

// unionKeepFirstPolicy keeps the first value during union
type unionKeepFirstPolicy struct{}

func (p *unionKeepFirstPolicy) Apply(_ *int32Summary, _ *int32Summary) {}

func TestUnion(t *testing.T) {
	t.Run("Empty, Empty", func(t *testing.T) {
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)

		err = union.Update(sketch1)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)

		compact1, err := NewCompactSketch[*int32Summary](sketch1, false)
		assert.NoError(t, err)
		err = union.Update(compact1)
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		compact2, err := NewCompactSketch[*int32Summary](sketch2, false)
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
		empty, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		empty, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		emptyCompact, err := NewCompactSketch[*int32Summary](empty, false)
		assert.NoError(t, err)
		exactCompact, err := NewCompactSketch[*int32Summary](exact, false)
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		aCompact, err := NewCompactSketch[*int32Summary](a, false)
		assert.NoError(t, err)
		bCompact, err := NewCompactSketch[*int32Summary](b, false)
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		aCompact, err := NewCompactSketch[*int32Summary](a, false)
		assert.NoError(t, err)
		bCompact, err := NewCompactSketch[*int32Summary](b, false)
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		aCompact, err := NewCompactSketch[*int32Summary](a, false)
		assert.NoError(t, err)
		bCompact, err := NewCompactSketch[*int32Summary](b, false)
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		aCompact, err := NewCompactSketch[*int32Summary](a, false)
		assert.NoError(t, err)
		bCompact, err := NewCompactSketch[*int32Summary](b, false)
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		compact1, err := NewCompactSketch[*int32Summary](sketch1, false)
		assert.NoError(t, err)
		compact2, err := NewCompactSketch[*int32Summary](sketch2, false)
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value), 1)
			value++
		}

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		compact1, err := NewCompactSketch[*int32Summary](sketch1, false)
		assert.NoError(t, err)
		compact2, err := NewCompactSketch[*int32Summary](sketch2, false)
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		aCompact, err := NewCompactSketch[*int32Summary](a, false)
		assert.NoError(t, err)
		bCompact, err := NewCompactSketch[*int32Summary](b, false)
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(3, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
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
		a, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(3, 1)

		b, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, 1)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		aCompact, err := NewCompactSketch[*int32Summary](a, false)
		assert.NoError(t, err)
		bCompact, err := NewCompactSketch[*int32Summary](b, false)
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
		sketch, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		sketch.UpdateInt64(1, 1) // non-empty should not be ignored

		union, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionSeed(123))
		assert.NoError(t, err)
		err = union.Update(sketch)
		assert.ErrorContains(t, err, "seed hash mismatch")
	})

	t.Run("Larger K", func(t *testing.T) {
		updateSketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 16384; i++ {
			updateSketch1.UpdateInt64(int64(i), 1)
		}

		updateSketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 26384; i++ {
			updateSketch2.UpdateInt64(int64(i), 1)
		}

		updateSketch3, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 86384; i++ {
			updateSketch3.UpdateInt64(int64(i), 1)
		}

		// First union
		union1, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(16))
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
		union2, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(16))
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
		_, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(theta.MinLgK-1))
		assert.ErrorContains(t, err, "lgK must not be less than")
	})

	t.Run("lgK Too Large", func(t *testing.T) {
		_, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(theta.MaxLgK+1))
		assert.ErrorContains(t, err, "lgK must not be greater than")
	})

	t.Run("P Is Zero", func(t *testing.T) {
		_, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionSketchP(0))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("P Is Negative", func(t *testing.T) {
		_, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionSketchP(-0.5))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("P Greater Than 1", func(t *testing.T) {
		_, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionSketchP(1.5))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("Valid Minimum lgK", func(t *testing.T) {
		union, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(theta.MinLgK))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("Valid Maximum lgK", func(t *testing.T) {
		union, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(theta.MaxLgK))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("With Resize Factor", func(t *testing.T) {
		union, err := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionResizeFactor(theta.ResizeX4))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("Result With QuickSelect Triggered", func(t *testing.T) {
		union, _ := NewUnion[*int32Summary](&unionSumPolicy{}, WithUnionLgK(8))

		sketch, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(10))
		for i := 0; i < 2000; i++ {
			sketch.UpdateInt64(int64(i), 1)
		}

		union.Update(sketch)
		result, err := union.Result(true)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Result should have at most nominalNum entries
		assert.LessOrEqual(t, result.NumRetained(), uint32(256))
	})

	t.Run("Policy Returns Consistent Instance", func(t *testing.T) {
		policy := &unionSumPolicy{}
		union, _ := NewUnion[*int32Summary](policy)
		policy1 := union.Policy()
		policy2 := union.Policy()
		assert.Equal(t, policy1, policy2)
		assert.Equal(t, policy, policy1)
	})

	t.Run("OrderedResult Method", func(t *testing.T) {
		sketch1, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i), 1)
		}

		sketch2, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i), 1)
		}

		union, _ := NewUnion[*int32Summary](&unionSumPolicy{})
		union.Update(sketch1)
		union.Update(sketch2)

		result, err := union.OrderedResult()
		assert.NoError(t, err)
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 150.0, result.Estimate())
	})
}

func TestUnionWithDifferentPolicies(t *testing.T) {
	t.Run("Keep First Policy", func(t *testing.T) {
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		sketch1.UpdateInt64(1, 100)
		sketch1.UpdateInt64(2, 200)

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		sketch2.UpdateInt64(1, 1)
		sketch2.UpdateInt64(2, 2)

		union, err := NewUnion[*int32Summary](&unionKeepFirstPolicy{})
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)
		err = union.Update(sketch2)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(2), result.NumRetained())

		values := make([]int32, 0)
		for _, s := range result.All() {
			values = append(values, s.value)
		}
		assert.Contains(t, values, int32(100))
		assert.Contains(t, values, int32(200))
		assert.NotContains(t, values, int32(1))
		assert.NotContains(t, values, int32(2))
	})

	t.Run("Sum Policy", func(t *testing.T) {
		sketch1, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		sketch1.UpdateInt64(1, 100)
		sketch1.UpdateInt64(2, 200)

		sketch2, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		sketch2.UpdateInt64(1, 1)
		sketch2.UpdateInt64(2, 2)

		union, err := NewUnion[*int32Summary](&unionSumPolicy{})
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)
		err = union.Update(sketch2)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(2), result.NumRetained())

		values := make([]int32, 0)
		for _, s := range result.All() {
			values = append(values, s.value)
		}
		assert.Contains(t, values, int32(101)) // 100 + 1
		assert.Contains(t, values, int32(202)) // 200 + 2
	})

	t.Run("Sum With SummaryMergeFunc", func(t *testing.T) {
		sketch1, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
			WithUpdateSketchLgK(12),
		)
		assert.NoError(t, err)
		sketch1.UpdateInt64(1, 100)
		sketch1.UpdateInt64(2, 200)

		sketch2, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
			WithUpdateSketchLgK(12),
		)
		assert.NoError(t, err)
		sketch2.UpdateInt64(1, 1)
		sketch2.UpdateInt64(2, 2)

		union, err := NewUnionWithSummaryMergeFunc[int32ValueSummary](
			func(internal, incoming int32ValueSummary) int32ValueSummary {
				internal.value += incoming.value
				return internal
			},
		)
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)
		err = union.Update(sketch2)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(2), result.NumRetained())

		values := make([]int32, 0)
		for _, s := range result.All() {
			values = append(values, s.value)
		}
		assert.Contains(t, values, int32(101)) // 100 + 1
		assert.Contains(t, values, int32(202)) // 200 + 2
	})

	t.Run("Exact Mode Half Overlap With SummaryMergeFunc", func(t *testing.T) {
		sketch1, err := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
			WithUpdateSketchLgK(12),
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
			WithUpdateSketchLgK(12),
		)
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value), 1)
			value++
		}

		union, err := NewUnionWithSummaryMergeFunc[int32ValueSummary](
			func(internal, incoming int32ValueSummary) int32ValueSummary {
				internal.value += incoming.value
				return internal
			},
		)
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)
		err = union.Update(sketch2)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 1500.0, result.Estimate())
	})
}
