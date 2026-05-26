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

	"github.com/stretchr/testify/require"
)

const varOptItemsUnionEpsilon = 1e-10

func TestNewVarOptItemsUnion(t *testing.T) {
	t.Run("bad max k", func(t *testing.T) {
		_, err := NewVarOptItemsUnion[int](0)
		require.Error(t, err)

		_, err = NewVarOptItemsUnion[int](uint(varOptMaxK) + 1)
		require.Error(t, err)
	})

	t.Run("k equals one empty result", func(t *testing.T) {
		union, err := NewVarOptItemsUnion[int](1)
		require.NoError(t, err)

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, 1, result.K())
		require.Equal(t, int64(0), result.N())
		require.Equal(t, 0, result.H())
		require.Equal(t, 0, result.R())
	})

	t.Run("k equals one exact result", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int](1)
		require.NoError(t, err)
		require.NoError(t, sketch.Update(1, 1.0))

		union, err := NewVarOptItemsUnion[int](1)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch))

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, 1, result.K())
		require.Equal(t, int64(1), result.N())
		require.Equal(t, 1, result.H())
		require.Equal(t, 0, result.R())
	})
}

func TestVarOptItemsUnion_Update(t *testing.T) {
	t.Run("nil sketch is no-op", func(t *testing.T) {
		union, err := NewVarOptItemsUnion[int](10)
		require.NoError(t, err)

		var nilSketch *VarOptItemsSketch[int]
		require.NoError(t, union.Update(nilSketch))
		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), result.N())

		sketch, err := NewVarOptItemsSketch[int](10)
		require.NoError(t, err)
		require.NoError(t, sketch.Update(1, 1.0))
		require.NoError(t, union.Update(sketch))

		before := copyVarOptItemsUnion(union)
		require.NoError(t, union.Update(nilSketch))
		compareVarOptItemsUnionsExact(t, before, union)
	})

	t.Run("empty sketch", func(t *testing.T) {
		const k = 2048

		sketch, err := NewVarOptItemsSketch[string](k)
		require.NoError(t, err)

		union, err := NewVarOptItemsUnion[string](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch))

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), result.N())
		require.Equal(t, 0, result.H())
		require.Equal(t, 0, result.R())
		require.True(t, math.IsNaN(result.tau()))
	})

	t.Run("two exact sketches", func(t *testing.T) {
		const (
			n = 4 // 2n < k
			k = 10
		)

		sketch1, err := NewVarOptItemsSketch[int](k)
		require.NoError(t, err)
		sketch2, err := NewVarOptItemsSketch[int](k)
		require.NoError(t, err)

		for i := 1; i <= n; i++ {
			require.NoError(t, sketch1.Update(i, float64(i)))
			require.NoError(t, sketch2.Update(-i, float64(i)))
		}

		union, err := NewVarOptItemsUnion[int](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch1))
		require.NoError(t, union.Update(sketch2))

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(2*n), result.N())
		require.Equal(t, 2*n, result.H())
		require.Equal(t, 0, result.R())
	})

	t.Run("heavy sampling sketch", func(t *testing.T) {
		const (
			n1 = 20
			k1 = 10
			n2 = 6
			k2 = 5
		)

		sketch1, err := NewVarOptItemsSketch[int](k1)
		require.NoError(t, err)
		sketch2, err := NewVarOptItemsSketch[int](k2)
		require.NoError(t, err)

		for i := 1; i <= n1; i++ {
			require.NoError(t, sketch1.Update(i, float64(i)))
		}

		for i := 1; i < n2; i++ {
			require.NoError(t, sketch2.Update(-i, float64(i)+1000.0))
		}
		require.NoError(t, sketch2.Update(-n2, 1000000.0))

		union, err := NewVarOptItemsUnion[int](k1)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch1))
		require.NoError(t, union.Update(sketch2))

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(n1+n2), result.N())
		require.Equal(t, k2, result.K())
		require.Equal(t, 1, result.H())
		require.Equal(t, k2-1, result.R())

		union.Reset()
		require.Equal(t, 0.0, union.outerTau())
		result, err = union.Result()
		require.NoError(t, err)
		require.Equal(t, k1, result.K())
		require.Equal(t, int64(0), result.N())
	})

	t.Run("identical sampling sketches", func(t *testing.T) {
		const (
			k = 20
			n = 50
		)

		sketch := newUnweightedLongsVarOptItemsSketch(t, k, n)

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch))
		require.NoError(t, union.Update(sketch))

		result, err := union.Result()
		require.NoError(t, err)
		expectedWeight := 2.0 * n
		require.Equal(t, int64(2*n), result.N())
		require.InDelta(t, expectedWeight, result.totalWeightR, varOptItemsUnionEpsilon)

		// Add another sketch such that sketchTau < outerTau.
		sketch = newUnweightedLongsVarOptItemsSketch(t, k, k+1)
		require.NoError(t, union.Update(sketch))
		result, err = union.Result()
		require.NoError(t, err)
		expectedWeight = (2.0 * n) + k + 1
		require.Equal(t, int64((2*n)+k+1), result.N())
		require.InDelta(t, expectedWeight, result.totalWeightR, varOptItemsUnionEpsilon)

		union.Reset()
		require.Equal(t, 0.0, union.outerTau())
		result, err = union.Result()
		require.NoError(t, err)
		require.Equal(t, k, result.K())
		require.Equal(t, int64(0), result.N())
	})

	t.Run("small sampling sketch", func(t *testing.T) {
		const (
			kSmall = 16
			n1     = 32
			n2     = 64
			kMax   = 128
		)

		sketch := newUnweightedLongsVarOptItemsSketch(t, kSmall, n1)
		require.NoError(t, sketch.Update(-1, float64(n1^2)))

		union, err := NewVarOptItemsUnion[int64](kMax)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch))

		sketch = newUnweightedLongsVarOptItemsSketch(t, kSmall, n2)
		require.NoError(t, union.Update(sketch))

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(n1+n2+1), result.N())
		require.InDelta(t, 96.0, result.totalWeightR, varOptItemsUnionEpsilon)
	})
}

func TestVarOptItemsUnion_UpdateReservoirItemsSketch(t *testing.T) {
	t.Run("nil reservoir sketch is no-op on empty union", func(t *testing.T) {
		union, err := NewVarOptItemsUnion[int64](10)
		require.NoError(t, err)

		var nilReservoir *ReservoirItemsSketch[int64]
		require.NoError(t, union.UpdateReservoirItemsSketch(nilReservoir))

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), result.N())
		require.Equal(t, 0, result.H())
		require.Equal(t, 0, result.R())
	})

	t.Run("exact reservoir boundary", func(t *testing.T) {
		const k = 5

		reservoir, err := NewReservoirItemsSketch[int64](k)
		require.NoError(t, err)
		for i := int64(0); i < k; i++ {
			require.NoError(t, reservoir.Update(i))
		}

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.UpdateReservoirItemsSketch(reservoir))
		require.Equal(t, 0.0, union.outerTau())

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(k), result.N())
		require.Equal(t, k, result.H())
		require.Equal(t, 0, result.R())
		for sample := range result.All() {
			require.InDelta(t, 1.0, sample.Weight, varOptItemsUnionEpsilon)
		}
	})

	t.Run("exact reservoir sketch", func(t *testing.T) {
		const k = 20
		n := int64(2 * k)

		baseVarOpt, err := NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		for i := int64(1); i <= n; i++ {
			require.NoError(t, baseVarOpt.Update(-i, float64(i)))
		}
		require.NoError(t, baseVarOpt.Update(-n-1, float64(n*n)))
		require.NoError(t, baseVarOpt.Update(-n-2, float64(n*n)))
		require.NoError(t, baseVarOpt.Update(-n-3, float64(n*n)))

		union1, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union1.Update(baseVarOpt))

		union2 := copyVarOptItemsUnion(union1)
		compareVarOptItemsUnionsExact(t, union1, union2)

		varOpt, err := NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		reservoir, err := NewReservoirItemsSketch[int64](k)
		require.NoError(t, err)

		var nilReservoir *ReservoirItemsSketch[int64]
		require.NoError(t, union2.UpdateReservoirItemsSketch(nilReservoir))
		require.NoError(t, union2.UpdateReservoirItemsSketch(reservoir))
		compareVarOptItemsUnionsExact(t, union1, union2)

		for i := int64(1); i < int64(k-1); i++ {
			require.NoError(t, reservoir.Update(i))
			require.NoError(t, varOpt.Update(i, 1.0))
		}

		require.NoError(t, union1.Update(varOpt))
		require.NoError(t, union2.UpdateReservoirItemsSketch(reservoir))
		compareVarOptItemsUnionsEquivalent(t, union1, union2)
	})

	t.Run("sampling reservoir sketch", func(t *testing.T) {
		const k = 20
		n := int64(k * k)

		union1, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		union2, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		compareVarOptItemsUnionsExact(t, union1, union2)

		varOpt, err := NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		reservoir, err := NewReservoirItemsSketch[int64](k)
		require.NoError(t, err)

		for i := int64(1); i < n; i++ {
			require.NoError(t, reservoir.Update(i))
			require.NoError(t, varOpt.Update(i, 1.0))
		}

		require.NoError(t, union1.Update(varOpt))
		require.NoError(t, union2.UpdateReservoirItemsSketch(reservoir))
		compareVarOptItemsUnionsEquivalent(t, union1, union2)

		require.NoError(t, union1.Update(varOpt))
		require.NoError(t, union2.UpdateReservoirItemsSketch(reservoir))
		compareVarOptItemsUnionsEquivalent(t, union1, union2)

		newVarOpt, err := NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		for i := int64(1); i <= n; i++ {
			require.NoError(t, newVarOpt.Update(-i, float64(i)))
		}
		require.NoError(t, newVarOpt.Update(-n-1, float64(n*n)))
		require.NoError(t, newVarOpt.Update(-n-2, float64(n*n)))
		require.NoError(t, newVarOpt.Update(-n-3, float64(n*n)))

		require.NoError(t, union1.Update(newVarOpt))
		require.NoError(t, union2.Update(newVarOpt))
		compareVarOptItemsUnionsEquivalent(t, union1, union2)
	})

	t.Run("reservoir various tau values", func(t *testing.T) {
		const k = 20
		n := int64(2 * k)

		baseVarOpt, err := NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		for i := int64(1); i <= n; i++ {
			require.NoError(t, baseVarOpt.Update(-i, 1.0))
		}

		union1, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union1.Update(baseVarOpt))

		union2 := copyVarOptItemsUnion(union1)
		compareVarOptItemsUnionsExact(t, union1, union2)

		varOpt, err := NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		reservoir, err := NewReservoirItemsSketch[int64](k)
		require.NoError(t, err)
		for i := int64(1); i < 2*n; i++ {
			require.NoError(t, reservoir.Update(i))
			require.NoError(t, varOpt.Update(i, 1.0))
		}

		require.NoError(t, union1.Update(varOpt))
		require.NoError(t, union2.UpdateReservoirItemsSketch(reservoir))
		compareVarOptItemsUnionsEquivalent(t, union1, union2)

		varOpt, err = NewVarOptItemsSketch[int64](k)
		require.NoError(t, err)
		reservoir, err = NewReservoirItemsSketch[int64](k)
		require.NoError(t, err)
		for i := int64(1); i <= int64(k+1); i++ {
			require.NoError(t, reservoir.Update(i))
			require.NoError(t, varOpt.Update(i, 1.0))
		}

		require.NoError(t, union1.Update(varOpt))
		require.NoError(t, union2.UpdateReservoirItemsSketch(reservoir))
		compareVarOptItemsUnionsEquivalent(t, union1, union2)
	})
}

func TestVarOptItemsUnion_Result(t *testing.T) {
	t.Run("simple gadget estimation mode", func(t *testing.T) {
		const k = 5

		sketch1 := newUnweightedLongsVarOptItemsSketch(t, 10, 4)
		sketch2 := newUnweightedLongsVarOptItemsSketch(t, 10, 4)

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch1))
		require.NoError(t, union.Update(sketch2))
		require.Zero(t, union.gadget.numMarksInH)
		require.Greater(t, union.gadget.r, 0)

		result, err := union.Result()
		require.NoError(t, err)
		require.Equal(t, int64(8), result.N())
		require.Equal(t, k, result.K())
		require.Greater(t, result.R(), 0)
		require.Nil(t, result.marks)
		require.InDelta(t, 8.0, totalVarOptItemsWeight(result), varOptItemsUnionEpsilon)
	})

	t.Run("result can be called repeatedly without mutating migration gadget", func(t *testing.T) {
		const (
			kSmall = 16
			n1     = 32
			n2     = 64
			kMax   = 128
		)

		sketch1 := newUnweightedLongsVarOptItemsSketch(t, kSmall, n1)
		require.NoError(t, sketch1.Update(-1, float64(n1^2)))
		sketch2 := newUnweightedLongsVarOptItemsSketch(t, kSmall, n2)

		union, err := NewVarOptItemsUnion[int64](kMax)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch1))
		require.NoError(t, union.Update(sketch2))

		beforeGadget := copyVarOptItemsSketch(union.gadget)
		result1, err := union.Result()
		require.NoError(t, err)
		requireVarOptItemsSketchStateEqual(t, beforeGadget, union.gadget)

		result2, err := union.Result()
		require.NoError(t, err)
		requireVarOptItemsSketchStateEqual(t, beforeGadget, union.gadget)
		require.Equal(t, result1.K(), result2.K())
		require.Equal(t, result1.N(), result2.N())
		require.Equal(t, result1.H(), result2.H())
		require.Equal(t, result1.R(), result2.R())
		require.InDelta(t, totalVarOptItemsWeight(result1), totalVarOptItemsWeight(result2), varOptItemsUnionEpsilon)
	})

	t.Run("can update after result", func(t *testing.T) {
		const (
			kSmall = 16
			n1     = 32
			n2     = 64
			n3     = 12
			kMax   = 128
		)

		sketch1 := newUnweightedLongsVarOptItemsSketch(t, kSmall, n1)
		require.NoError(t, sketch1.Update(-1, float64(n1^2)))
		sketch2 := newUnweightedLongsVarOptItemsSketch(t, kSmall, n2)
		sketch3 := newUnweightedLongsVarOptItemsSketch(t, kMax, n3)

		union, err := NewVarOptItemsUnion[int64](kMax)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch1))
		require.NoError(t, union.Update(sketch2))
		_, err = union.Result()
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch3))

		expected, err := NewVarOptItemsUnion[int64](kMax)
		require.NoError(t, err)
		require.NoError(t, expected.Update(sketch1))
		require.NoError(t, expected.Update(sketch2))
		require.NoError(t, expected.Update(sketch3))

		compareVarOptItemsUnionsEquivalent(t, expected, union)
	})
}

func TestVarOptItemsUnion_String(t *testing.T) {
	t.Run("empty nil gadget", func(t *testing.T) {
		union := &VarOptItemsUnion[int]{}
		require.Equal(t, "", union.String())
	})

	t.Run("summary", func(t *testing.T) {
		union, err := NewVarOptItemsUnion[int](10)
		require.NoError(t, err)

		sketch, err := NewVarOptItemsSketch[int](10)
		require.NoError(t, err)
		require.NoError(t, sketch.Update(1, 1.0))
		require.NoError(t, union.Update(sketch))

		summary := union.String()
		require.Contains(t, summary, "### VarOptItemsUnion Summary:")
		require.Contains(t, summary, "Max k: 10")
		require.Contains(t, summary, "Gadget summary:")
		require.Contains(t, summary, "### END UNION SUMMARY")
	})
}

func newUnweightedLongsVarOptItemsSketch(t *testing.T, k int, n int) *VarOptItemsSketch[int64] {
	t.Helper()

	sketch, err := NewVarOptItemsSketch[int64](uint(k))
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, sketch.Update(int64(i), 1.0))
	}
	return sketch
}

func compareVarOptItemsUnionsExact[T comparable](t *testing.T, u1, u2 *VarOptItemsUnion[T]) {
	t.Helper()

	require.InDelta(t, u1.outerTau(), u2.outerTau(), varOptItemsUnionEpsilon)

	sketch1, err := u1.Result()
	require.NoError(t, err)
	sketch2, err := u2.Result()
	require.NoError(t, err)

	require.Equal(t, sketch1.N(), sketch2.N())
	require.Equal(t, sketch1.H(), sketch2.H())
	require.Equal(t, sketch1.R(), sketch2.R())

	samples1 := collectVarOptItemsUnionSamples(sketch1)
	samples2 := collectVarOptItemsUnionSamples(sketch2)
	require.Len(t, samples2, len(samples1))

	for i := range samples1 {
		require.InDelta(t, samples1[i].Weight, samples2[i].Weight, varOptItemsUnionEpsilon)
		require.Equal(t, samples1[i].Item, samples2[i].Item)
	}
}

func compareVarOptItemsUnionsEquivalent[T comparable](t *testing.T, u1, u2 *VarOptItemsUnion[T]) {
	t.Helper()

	require.InDelta(t, u1.outerTau(), u2.outerTau(), varOptItemsUnionEpsilon)

	sketch1, err := u1.Result()
	require.NoError(t, err)
	sketch2, err := u2.Result()
	require.NoError(t, err)

	require.Equal(t, sketch1.N(), sketch2.N())
	require.Equal(t, sketch1.H(), sketch2.H())
	require.Equal(t, sketch1.R(), sketch2.R())

	samples1 := collectVarOptItemsUnionSamples(sketch1)
	samples2 := collectVarOptItemsUnionSamples(sketch2)
	require.Len(t, samples2, len(samples1))

	for i := range samples1 {
		require.InDelta(t, samples1[i].Weight, samples2[i].Weight, varOptItemsUnionEpsilon)
	}
	for i := 0; i < sketch1.H(); i++ {
		require.Equal(t, samples1[i].Item, samples2[i].Item)
	}
}

func collectVarOptItemsUnionSamples[T any](sketch *VarOptItemsSketch[T]) []Sample[T] {
	samples := make([]Sample[T], 0, sketch.NumSamples())
	for sample := range sketch.All() {
		samples = append(samples, sample)
	}
	return samples
}

func totalVarOptItemsWeight[T any](sketch *VarOptItemsSketch[T]) float64 {
	total := 0.0
	for sample := range sketch.All() {
		total += sample.Weight
	}
	return total
}

func requireVarOptItemsSketchStateEqual[T comparable](t *testing.T, expected, actual *VarOptItemsSketch[T]) {
	t.Helper()

	require.Equal(t, expected.k, actual.k)
	require.Equal(t, expected.n, actual.n)
	require.Equal(t, expected.h, actual.h)
	require.Equal(t, expected.m, actual.m)
	require.Equal(t, expected.r, actual.r)
	require.InDelta(t, expected.totalWeightR, actual.totalWeightR, varOptItemsUnionEpsilon)
	require.Equal(t, expected.rf, actual.rf)
	require.Equal(t, expected.numMarksInH, actual.numMarksInH)
	require.Equal(t, expected.data, actual.data)
	require.Equal(t, expected.marks, actual.marks)
	require.Len(t, actual.weights, len(expected.weights))
	for i := range expected.weights {
		require.InDelta(t, expected.weights[i], actual.weights[i], varOptItemsUnionEpsilon)
	}
}

func copyVarOptItemsUnion[T any](src *VarOptItemsUnion[T]) *VarOptItemsUnion[T] {
	return &VarOptItemsUnion[T]{
		gadget:        copyVarOptItemsSketch(src.gadget),
		k:             src.k,
		n:             src.n,
		outerTauNumer: src.outerTauNumer,
		outerTauDenom: src.outerTauDenom,
	}
}

func copyVarOptItemsSketch[T any](src *VarOptItemsSketch[T]) *VarOptItemsSketch[T] {
	if src == nil {
		return nil
	}

	var marks []bool
	if src.marks != nil {
		marks = make([]bool, len(src.marks), cap(src.marks))
		copy(marks, src.marks)
	}

	data := make([]T, len(src.data), cap(src.data))
	copy(data, src.data)
	weights := make([]float64, len(src.weights), cap(src.weights))
	copy(weights, src.weights)

	return &VarOptItemsSketch[T]{
		data:         data,
		weights:      weights,
		marks:        marks,
		k:            src.k,
		n:            src.n,
		h:            src.h,
		m:            src.m,
		r:            src.r,
		totalWeightR: src.totalWeightR,
		rf:           src.rf,
		numMarksInH:  src.numMarksInH,
	}
}
