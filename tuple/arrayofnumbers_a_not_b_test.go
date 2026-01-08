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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/theta"
)

func TestArrayOfNumbersSketchANotB(t *testing.T) {
	t.Run("A Empty B Empty", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.Zero(t, result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("A Empty Compact B Empty Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		aCompact, err := a.Compact(false)
		assert.NoError(t, err)
		bCompact, err := b.Compact(false)
		assert.NoError(t, err)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.Zero(t, result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("A Empty B Exact", func(t *testing.T) {
		sketchA, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		sketchB, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		_ = sketchB.UpdateInt64(1, []float64{1.0, 2.0})
		_ = sketchB.UpdateInt64(2, []float64{3.0, 4.0})
		assert.False(t, sketchB.IsEstimationMode())

		result, err := ArrayOfNumbersSketchANotB[float64](sketchA, sketchB, theta.DefaultSeed, false)
		assert.NoError(t, err)
		assert.True(t, result.IsEmpty())
		assert.Equal(t, uint32(0), result.NumRetained())
	})

	t.Run("A Empty Compact B Exact Compact", func(t *testing.T) {
		sketchA, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		sketchB, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		_ = sketchB.UpdateInt64(1, []float64{1.0, 2.0})
		_ = sketchB.UpdateInt64(2, []float64{3.0, 4.0})
		assert.False(t, sketchB.IsEstimationMode())

		aCompact, _ := sketchA.Compact(false)
		bCompact, _ := sketchB.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)
		assert.True(t, result.IsEmpty())
		assert.Equal(t, uint32(0), result.NumRetained())
	})

	t.Run("A Exact B Empty", func(t *testing.T) {
		sketchA, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		sketchB, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		_ = sketchA.UpdateInt64(1, []float64{1.0, 2.0})
		_ = sketchA.UpdateInt64(2, []float64{3.0, 4.0})
		assert.False(t, sketchA.IsEstimationMode())

		result, err := ArrayOfNumbersSketchANotB[float64](sketchA, sketchB, theta.DefaultSeed, false)
		assert.NoError(t, err)
		assert.False(t, result.IsEmpty())
		assert.Equal(t, uint32(2), result.NumRetained())
	})

	t.Run("A Exact Compact B Empty Compact", func(t *testing.T) {
		sketchA, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		sketchB, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		_ = sketchA.UpdateInt64(1, []float64{1.0, 2.0})
		_ = sketchA.UpdateInt64(2, []float64{3.0, 4.0})
		assert.False(t, sketchA.IsEstimationMode())

		aCompact, _ := sketchA.Compact(false)
		bCompact, _ := sketchB.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)
		assert.False(t, result.IsEmpty())
		assert.Equal(t, uint32(2), result.NumRetained())
	})

	t.Run("A Empty, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Empty Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Empty", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(6, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Empty Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(6, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Empty, B Estimation Mode", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Empty Compact, B Estimation Mode Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode, B Empty", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode Compact, B Empty Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Exact, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Exact Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Exact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		a.UpdateInt64(6, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Exact Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](
			2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		a.UpdateInt64(6, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6, []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
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

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Estimation Mode", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(6, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Estimation Mode Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(6, []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4, []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Disjoint", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 1000.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Disjoint", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 1000.0, result.Estimate())
	})

	t.Run("Exact Mode Half Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value = 500
		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Half Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value = 500
		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.False(t, result.IsOrdered())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Full Overlap", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			sketch.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		result, err := ArrayOfNumbersSketchANotB[float64](sketch, sketch, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Full Overlap", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			sketch.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		compact, _ := sketch.Compact(false)
		result, err := ArrayOfNumbersSketchANotB[float64](compact, compact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Disjoint", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		margin := 10000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-10000.0), margin)
	})

	t.Run("Estimation Mode Compact Disjoint", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		margin := 10000.0 * 0.02
		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Less(t, math.Abs(result.Estimate()-10000.0), margin)
	})

	t.Run("Estimation Mode Half Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value = 5000
		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		margin := 5000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-5000.0), margin)
	})

	t.Run("Estimation Mode Compact Half Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value = 5000
		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		margin := 5000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-5000.0), margin)
	})

	t.Run("Estimation Mode Full Overlap", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		result, err := ArrayOfNumbersSketchANotB[float64](sketch, sketch, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Compact Full Overlap", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(value), []float64{1.0, 2.0})
			value++
		}

		compact, _ := sketch.Compact(false)
		result, err := ArrayOfNumbersSketchANotB[float64](compact, compact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("A Exact Mode, B Estimation Mode Full Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4), []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Exact Mode Compact, B Estimation Mode Compact Full Overlap", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4), []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6), []float64{1.0, 2.0})

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3), []float64{1.0, 2.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6), []float64{1.0, 2.0})

		aCompact, _ := a.Compact(false)
		bCompact, _ := b.Compact(false)

		result, err := ArrayOfNumbersSketchANotB[float64](aCompact, bCompact, theta.DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Seed Mismatch A", func(t *testing.T) {
		seed1 := uint64(9001)
		seed2 := uint64(12345)

		sketchA, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(seed2))
		sketchB, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(seed1))

		_ = sketchA.UpdateInt64(1, []float64{1.0, 2.0})
		_ = sketchB.UpdateInt64(2, []float64{1.0, 2.0})

		_, err := ArrayOfNumbersSketchANotB[float64](sketchA, sketchB, seed1, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sketch A seed hash mismatch")
	})

	t.Run("Seed Mismatch B", func(t *testing.T) {
		seed1 := uint64(9001)
		seed2 := uint64(12345)

		sketchA, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(seed1))
		sketchB, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(seed2))

		_ = sketchA.UpdateInt64(1, []float64{1.0, 2.0})
		_ = sketchB.UpdateInt64(2, []float64{1.0, 2.0})

		_, err := ArrayOfNumbersSketchANotB[float64](sketchA, sketchB, seed1, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sketch B seed hash mismatch")
	})

	t.Run("Seed Mismatch Both", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		sketch.UpdateInt64(1, []float64{1.0, 2.0}) // non-empty should not be ignored

		_, err = ArrayOfNumbersSketchANotB[float64](sketch, sketch, 123, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")
	})

	t.Run("Summary Preserved After ANotB", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)

		_ = a.UpdateInt64(1, []float64{10.0, 100.0})
		_ = a.UpdateInt64(2, []float64{20.0, 200.0})
		_ = a.UpdateInt64(3, []float64{30.0, 300.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = b.UpdateInt64(2, []float64{50.0, 500.0}) // Only remove key 2

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(2), result.NumRetained())
		assert.False(t, result.IsEmpty())

		summarySum := []float64{0.0, 0.0}
		for _, summary := range result.All() {
			values := summary.Values()
			assert.Len(t, values, 2)
			summarySum[0] += values[0]
			summarySum[1] += values[1]
		}
		// keys 1 (10, 100) and 3 (30, 300) = (40, 400)
		assert.InDelta(t, 40.0, summarySum[0], 1e-10)
		assert.InDelta(t, 400.0, summarySum[1], 1e-10)
	})

	t.Run("NumValuesInSummary Preserved", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[float64](3)
		assert.NoError(t, err)
		_ = a.UpdateInt64(1, []float64{1.0, 2.0, 3.0})

		b, err := NewArrayOfNumbersUpdateSketch[float64](3)
		assert.NoError(t, err)

		result, err := ArrayOfNumbersSketchANotB[float64](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, uint8(3), result.NumValuesInSummary())
	})

	t.Run("Different Number Types Int32", func(t *testing.T) {
		a, err := NewArrayOfNumbersUpdateSketch[int32](2)
		assert.NoError(t, err)

		_ = a.UpdateInt64(1, []int32{10, 20})
		_ = a.UpdateInt64(2, []int32{30, 40})

		b, err := NewArrayOfNumbersUpdateSketch[int32](2)
		assert.NoError(t, err)
		_ = b.UpdateInt64(2, []int32{50, 60})

		result, err := ArrayOfNumbersSketchANotB[int32](a, b, theta.DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(1), result.NumRetained())
		for _, summary := range result.All() {
			assert.Equal(t, []int32{10, 20}, summary.Values())
		}
	})
}
