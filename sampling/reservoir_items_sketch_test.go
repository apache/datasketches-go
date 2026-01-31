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
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestNewReservoirItemsSketch(t *testing.T) {
	sketch, err := NewReservoirItemsSketch[int64](10)
	assert.NoError(t, err)
	assert.NotNil(t, sketch)
	assert.Equal(t, 10, sketch.K())
	assert.Equal(t, int64(0), sketch.N())
	assert.True(t, sketch.IsEmpty())
}

func TestReservoirItemsSketchWithStrings(t *testing.T) {
	sketch, err := NewReservoirItemsSketch[string](5)
	assert.NoError(t, err)

	sketch.Update("apple")
	sketch.Update("banana")
	sketch.Update("cherry")

	assert.Equal(t, int64(3), sketch.N())
	assert.Equal(t, 3, sketch.NumSamples())

	samples := sketch.Samples()
	assert.Contains(t, samples, "apple")
	assert.Contains(t, samples, "banana")
	assert.Contains(t, samples, "cherry")
}

func TestReservoirItemsSketchWithStruct(t *testing.T) {
	type Event struct {
		ID   int
		Name string
	}

	sketch, err := NewReservoirItemsSketch[Event](5)
	assert.NoError(t, err)

	sketch.Update(Event{1, "login"})
	sketch.Update(Event{2, "logout"})
	sketch.Update(Event{3, "click"})

	assert.Equal(t, int64(3), sketch.N())
	samples := sketch.Samples()
	assert.Len(t, samples, 3)
}

func TestReservoirItemsSketchInvalidK(t *testing.T) {
	_, err := NewReservoirItemsSketch[int64](0)
	assert.ErrorContains(t, err, "k must be at least 2")

	_, err = NewReservoirItemsSketch[int64](1)
	assert.ErrorContains(t, err, "k must be at least 2")
}

func TestReservoirItemsSketch_Update(t *testing.T) {
	t.Run("BelowKStoresAllItems", func(t *testing.T) {
		sketch, err := NewReservoirItemsSketch[int64](10)
		assert.NoError(t, err)

		for i := int64(1); i <= 5; i++ {
			sketch.Update(i)
		}

		assert.Equal(t, int64(5), sketch.N())
		assert.Equal(t, 5, sketch.NumSamples())
		assert.Equal(t, 1.0, sketch.ImplicitSampleWeight())

		samples := sketch.Samples()
		for i := int64(1); i <= 5; i++ {
			assert.Contains(t, samples, i)
		}
	})

	t.Run("AtKStoresKItems", func(t *testing.T) {
		sketch, err := NewReservoirItemsSketch[int64](8)
		assert.NoError(t, err)

		for i := int64(1); i <= 8; i++ {
			sketch.Update(i)
		}

		assert.Equal(t, int64(8), sketch.N())
		assert.Equal(t, 8, sketch.NumSamples())
		assert.Equal(t, 1.0, sketch.ImplicitSampleWeight())

		samples := sketch.Samples()
		for i := int64(1); i <= 8; i++ {
			assert.Contains(t, samples, i)
		}
	})

	t.Run("AboveKMaintainsKAndIncrementsN", func(t *testing.T) {
		k := 10
		total := 1000

		sketch, err := NewReservoirItemsSketch[int64](k)
		assert.NoError(t, err)

		for i := 1; i <= total; i++ {
			sketch.Update(int64(i))
		}

		assert.Equal(t, int64(total), sketch.N())
		assert.Equal(t, k, sketch.NumSamples())
		assert.Equal(t, float64(total)/float64(k), sketch.ImplicitSampleWeight())

		samples := sketch.Samples()
		seen := make(map[int64]struct{}, len(samples))
		for _, sample := range samples {
			assert.True(t, sample >= 1 && sample <= int64(total))
			_, exists := seen[sample]
			assert.False(t, exists)
			seen[sample] = struct{}{}
		}
	})
}

func TestReservoirItemsSketchReset(t *testing.T) {
	k := 1024

	sketch, err := NewReservoirItemsSketch[int64](k)
	assert.NoError(t, err)

	ceilingLgK, _ := internal.ExactLog2(common.CeilingPowerOf2(k))
	initialLgSize := startingSubMultiple(
		ceilingLgK, int(math.Log2(float64(defaultResizeFactor))), minLgArrItems,
	)
	expectedInitialCap := adjustedSamplingAllocationSize(k, 1<<initialLgSize)

	for i := int64(1); i <= int64(expectedInitialCap)+1; i++ {
		sketch.Update(i)
	}

	assert.Greater(t, cap(sketch.data), expectedInitialCap)

	sketch.Reset()

	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, int64(0), sketch.N())
	assert.Equal(t, k, sketch.K())
	assert.Equal(t, 0, len(sketch.data))
	assert.Equal(t, expectedInitialCap, cap(sketch.data))
}

func TestReservoirItemsSketchGetSamplesIsCopy(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)
	sketch.Update(42)

	samples1 := sketch.Samples()
	samples2 := sketch.Samples()

	// Modify samples1
	samples1[0] = 999

	// samples2 and internal data should be unchanged
	assert.NotEqual(t, samples1[0], samples2[0])
	assert.Equal(t, int64(42), samples2[0])
}

func TestReservoirItemsSketchResizeFactorSerialization(t *testing.T) {
	sketch, err := NewReservoirItemsSketch[int64](10, WithReservoirItemsSketchResizeFactor(ResizeX2))
	assert.NoError(t, err)
	sketch.Update(1)

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, byte(0x42), data[0]) // ResizeX2 (0x40) + preambleIntsNonEmpty (0x02)

	restored, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, ResizeX2, restored.rf)
}

func TestReservoirItemsSketchEstimateSubsetSum(t *testing.T) {
	t.Run("EmptySketch", func(t *testing.T) {
		sketch, err := NewReservoirItemsSketch[int64](10)
		assert.NoError(t, err)

		summary, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
		assert.NoError(t, err)
		assert.Equal(t, 0.0, summary.LowerBound)
		assert.Equal(t, 0.0, summary.Estimate)
		assert.Equal(t, 0.0, summary.UpperBound)
		assert.Equal(t, 0.0, summary.TotalSketchWeight)
	})

	t.Run("ExactMode", func(t *testing.T) {
		sketch, err := NewReservoirItemsSketch[int64](10)
		assert.NoError(t, err)
		for i := int64(1); i <= 5; i++ {
			sketch.Update(i)
		}

		summary, err := sketch.EstimateSubsetSum(func(v int64) bool { return v%2 == 0 })
		assert.NoError(t, err)
		assert.Equal(t, 2.0, summary.LowerBound)
		assert.Equal(t, 2.0, summary.Estimate)
		assert.Equal(t, 2.0, summary.UpperBound)
		assert.Equal(t, 5.0, summary.TotalSketchWeight)
	})

	t.Run("EstimationModePredicateNeverMatches", func(t *testing.T) {
		rand.Seed(7)
		sketch, err := NewReservoirItemsSketch[int64](10)
		assert.NoError(t, err)
		for i := int64(1); i <= 100; i++ {
			sketch.Update(i)
		}

		summary, err := sketch.EstimateSubsetSum(func(int64) bool { return false })
		assert.NoError(t, err)
		assert.Equal(t, 0.0, summary.Estimate)
		assert.Equal(t, 0.0, summary.LowerBound)
		assert.True(t, summary.UpperBound >= 0.0 && summary.UpperBound <= 1.0)
		assert.Equal(t, float64(sketch.NumSamples()), summary.TotalSketchWeight)
	})

	t.Run("EstimationModePredicateAlwaysMatches", func(t *testing.T) {
		rand.Seed(11)
		sketch, err := NewReservoirItemsSketch[int64](10)
		assert.NoError(t, err)
		for i := int64(1); i <= 100; i++ {
			sketch.Update(i)
		}

		summary, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
		assert.NoError(t, err)
		assert.Equal(t, 1.0, summary.Estimate)
		assert.Equal(t, 1.0, summary.UpperBound)
		assert.True(t, summary.LowerBound >= 0.0 && summary.LowerBound <= 1.0)
		assert.Equal(t, float64(sketch.NumSamples()), summary.TotalSketchWeight)
	})

	t.Run("EstimationModePredicatePartiallyMatches", func(t *testing.T) {
		rand.Seed(23)
		sketch, err := NewReservoirItemsSketch[int64](10)
		assert.NoError(t, err)
		for i := int64(1); i <= 100; i++ {
			sketch.Update(i)
		}

		samples := sketch.Samples()
		trueCount := 0
		for _, v := range samples {
			if v%2 == 0 {
				trueCount++
			}
		}
		expectedEstimate := float64(trueCount) / float64(len(samples))

		summary, err := sketch.EstimateSubsetSum(func(v int64) bool { return v%2 == 0 })
		assert.NoError(t, err)
		assert.InDelta(t, expectedEstimate, summary.Estimate, 0.0)
		assert.True(t, summary.LowerBound >= 0.0 && summary.LowerBound <= 1.0)
		assert.True(t, summary.UpperBound >= 0.0 && summary.UpperBound <= 1.0)
		assert.True(t, summary.LowerBound <= summary.Estimate)
		assert.True(t, summary.Estimate <= summary.UpperBound)
		assert.Equal(t, float64(sketch.NumSamples()), summary.TotalSketchWeight)
	})
}

func TestReservoirItemsSketchLegacySerVerEmpty(t *testing.T) {
	data := make([]byte, 8)
	data[0] = 0xC0 | preambleIntsEmpty
	data[1] = 1 // legacy serVer
	data[2] = byte(internal.FamilyEnum.ReservoirItems.Id)
	data[3] = flagEmpty
	binary.LittleEndian.PutUint16(data[4:], 0x5000) // p=10, i=0 => k=1024

	sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, 1024, sketch.K())
	assert.Equal(t, ResizeX8, sketch.rf)
}
