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

// jaccardSumPolicy sums the values of matching summaries
type jaccardSumPolicy struct{}

func (p *jaccardSumPolicy) Apply(internalSummary *int32Summary, incomingSummary *int32Summary) {
	internalSummary.value += incomingSummary.value
}

func assertJaccardBoundsInvariant(t *testing.T, jc JaccardSimilarityResult) {
	t.Helper()
	assert.LessOrEqual(t, jc.LowerBound, jc.Estimate)
	assert.LessOrEqual(t, jc.Estimate, jc.UpperBound)
}

func TestJaccard(t *testing.T) {
	policy := &jaccardSumPolicy{}

	t.Run("Empty", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Only SketchA Empty", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 100; i++ {
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Only SketchB Empty", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 100; i++ {
			skA.UpdateInt64(int64(i), 1)
		}

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Same Sketch Exact Mode", func(t *testing.T) {
		sk, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sk.UpdateInt64(int64(i), 1)
		}

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}

		// update sketch
		jc, err := Jaccard(sk, sk, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)

		// compact sketch
		compactSk, err := NewCompactSketch[*int32Summary](sk, true)
		assert.NoError(t, err)
		jc, err = Jaccard(compactSk, compactSk, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Full Overlap Exact Mode", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}

		// update sketches
		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		jc, err = Jaccard(compactA, compactB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Disjoint Exact Mode", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i+1000), 1)
		}

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}

		// update sketches
		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		jc, err = Jaccard(compactA, compactB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Half Overlap Estimation Mode", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i+5000), 1)
		}

		expectedValue := 0.33
		margin := 0.01

		// update sketches
		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertJaccardBoundsInvariant(t, jc)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		jc, err = Jaccard(compactA, compactB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Half Overlap Estimation Mode Custom Seed", func(t *testing.T) {
		seed := uint64(123)
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i+5000), 1)
		}

		expectedValue := 0.33
		margin := 0.01

		// update sketches
		jc, err := Jaccard(skA, skB, policy, seed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertJaccardBoundsInvariant(t, jc)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		jc, err = Jaccard(compactA, compactB, policy, seed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Identical Sets Different Objects", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Single Element Same", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(42, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(42, 1)

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Single Element Different", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(42, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(99, 1)

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Two Elements One Overlap", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(1, 1)
		skA.UpdateInt64(2, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(2, 1)
		skB.UpdateInt64(3, 1)

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		// J = |A ∩ B| / |A ∪ B| = 1 / 3 ≈ 0.333
		assert.InDelta(t, 0.333, jc.Estimate, 0.01)
		assert.InDelta(t, 0.333, jc.LowerBound, 0.01)
		assert.InDelta(t, 0.333, jc.UpperBound, 0.01)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Two Elements No Overlap", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(1, 1)
		skA.UpdateInt64(2, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(3, 1)
		skB.UpdateInt64(4, 1)

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Two Elements Full Overlap", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(1, 1)
		skA.UpdateInt64(2, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(1, 1)
		skB.UpdateInt64(2, 1)

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertJaccardBoundsInvariant(t, jc)
	})

	t.Run("Subset Relationship - Small Subset of Large", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		for i := 0; i < 100; i++ {
			skA.UpdateInt64(int64(i), 1)
		}
		for i := 0; i < 1000; i++ {
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assertJaccardBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 100 / 1000 = 0.1
		assert.InDelta(t, 0.1, jc.Estimate, 0.01)
	})

	t.Run("Subset Relationship - Large Subset of Larger", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		for i := 0; i < 5000; i++ {
			skA.UpdateInt64(int64(i), 1)
		}
		for i := 0; i < 10000; i++ {
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assertJaccardBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 5000 / 10000 = 0.5
		assert.InDelta(t, 0.5, jc.Estimate, 0.02)
	})

	t.Run("Subset Relationship - Reversed Order", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
		}
		for i := 0; i < 100; i++ {
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assertJaccardBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 100 / 1000 = 0.1
		assert.InDelta(t, 0.1, jc.Estimate, 0.01)
	})

	t.Run("Subset Relationship - Almost Complete Subset", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
		}
		for i := 0; i < 900; i++ {
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assertJaccardBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 900 / 1000 = 0.9
		assert.InDelta(t, 0.9, jc.Estimate, 0.01)
	})

	t.Run("Mismatched Seeds - Error Case", func(t *testing.T) {
		seedA := uint64(123)
		seedB := uint64(456)

		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seedA))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seedB))
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, seedA)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")

		assert.Equal(t, JaccardSimilarityResult{}, jc)
	})

	t.Run("Mismatched Seeds - Same Seed for Jaccard as Sketches", func(t *testing.T) {
		seed := uint64(789)

		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, seed)
		assert.NoError(t, err)
		assertJaccardBoundsInvariant(t, jc)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
	})

	t.Run("Mismatched Seeds - Both Sketches Wrong Seed Different from Jaccard Seed", func(t *testing.T) {
		sketchSeed := uint64(111)
		jaccardSeed := uint64(222)

		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(sketchSeed))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(sketchSeed))
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		jc, err := Jaccard(skA, skB, policy, jaccardSeed)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")

		assert.Equal(t, JaccardSimilarityResult{}, jc)
	})
}

func TestIsExactlyEqual(t *testing.T) {
	policy := &jaccardSumPolicy{}

	t.Run("Empty", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Only SketchA Empty", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 100; i++ {
			skB.UpdateInt64(int64(i), 1)
		}

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Only SketchB Empty", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 100; i++ {
			skA.UpdateInt64(int64(i), 1)
		}

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Same Sketch Exact Mode", func(t *testing.T) {
		sk, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sk.UpdateInt64(int64(i), 1)
		}

		// update sketch
		result, err := IsExactlyEqual(sk, sk, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		// compact sketch
		compactSk, err := NewCompactSketch[*int32Summary](sk, true)
		assert.NoError(t, err)
		result, err = IsExactlyEqual(compactSk, compactSk, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Full Overlap Exact Mode", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		// update sketches
		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		result, err = IsExactlyEqual(compactA, compactB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Disjoint Exact Mode", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i+1000), 1)
		}

		// update sketches
		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		result, err = IsExactlyEqual(compactA, compactB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Half Overlap Estimation Mode", func(t *testing.T) {
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i+5000), 1)
		}

		// update sketches
		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)

		// compact sketches
		compactA, err := NewCompactSketch[*int32Summary](skA, true)
		assert.NoError(t, err)
		compactB, err := NewCompactSketch[*int32Summary](skB, true)
		assert.NoError(t, err)
		result, err = IsExactlyEqual(compactA, compactB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Identical Sets Different Objects", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Single Element Same", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(42, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(42, 1)

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Single Element Different", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(42, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(99, 1)

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Two Elements One Overlap", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(1, 1)
		skA.UpdateInt64(2, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(2, 1)
		skB.UpdateInt64(3, 1)

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Two Elements No Overlap", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(1, 1)
		skA.UpdateInt64(2, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(3, 1)
		skB.UpdateInt64(4, 1)

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Two Elements Full Overlap", func(t *testing.T) {
		skA, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skA.UpdateInt64(1, 1)
		skA.UpdateInt64(2, 1)

		skB, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12))
		skB.UpdateInt64(1, 1)
		skB.UpdateInt64(2, 1)

		result, err := IsExactlyEqual(skA, skB, policy, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Custom Seed", func(t *testing.T) {
		seed := uint64(123)
		skA, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		skB, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(12), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i), 1)
			skB.UpdateInt64(int64(i), 1)
		}

		result, err := IsExactlyEqual(skA, skB, policy, seed)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

func TestIsSimilarity(t *testing.T) {
	policy := &jaccardSumPolicy{}

	// The distribution is quite tight, about +/- 0.7%, which is pretty good since the accuracy of the
	// underlying sketch is about +/- 1.56%.
	t.Run("Default Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.95)
		threshold := 0.943

		expected, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i), 1)
		}

		actual, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i), 1)
		}

		result, err := IsSimilar(actual, expected, policy, threshold, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsSimilar(actual, actual, policy, threshold, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Custom Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.95)
		threshold := 0.943
		seed := uint64(1234)

		expected, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i), 1)
		}

		actual, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i), 1)
		}

		result, err := IsSimilar(actual, expected, policy, threshold, seed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsSimilar(actual, actual, policy, threshold, seed)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

func TestIsDissimilar(t *testing.T) {
	policy := &jaccardSumPolicy{}

	// The distribution is much looser here, about +/- 14%. This is due to the fact that intersections lose accuracy
	// as the ratio of intersection to the union becomes a small number.
	t.Run("Default Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.05)
		threshold := 0.061

		expected, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i), 1)
		}

		actual, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i), 1)
		}

		result, err := IsDissimilar(actual, expected, policy, threshold, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsDissimilar(actual, actual, policy, threshold, theta.DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Custom Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.05)
		threshold := 0.061
		seed := uint64(1234)

		expected, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i), 1)
		}

		actual, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i), 1)
		}

		result, err := IsDissimilar(actual, expected, policy, threshold, seed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsDissimilar(actual, actual, policy, threshold, seed)
		assert.NoError(t, err)
		assert.False(t, result)
	})
}
