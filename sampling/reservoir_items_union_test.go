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
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper function to create a basic sketch with n items and capacity k
func getBasicSketch(n int64, k int) *ReservoirItemsSketch[int64] {
	sketch, _ := NewReservoirItemsSketch[int64](k)
	for i := int64(0); i < n; i++ {
		sketch.Update(i)
	}
	return sketch
}

// === Migrated tests from reservoir_items_sketch_test.go ===

func TestReservoirItemsUnion(t *testing.T) {
	sketch1, _ := NewReservoirItemsSketch[int64](10)
	sketch2, _ := NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 500; i++ {
		sketch1.Update(i)
	}
	for i := int64(501); i <= 1000; i++ {
		sketch2.Update(i)
	}

	union, err := NewReservoirItemsUnion[int64](10)
	assert.NoError(t, err)

	union.UpdateSketch(sketch1)
	union.UpdateSketch(sketch2)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, 10, result.NumSamples())
}

func TestReservoirItemsUnionWithStrings(t *testing.T) {
	sketch1, _ := NewReservoirItemsSketch[string](5)
	sketch2, _ := NewReservoirItemsSketch[string](5)

	sketch1.Update("a")
	sketch1.Update("b")
	sketch1.Update("c")

	sketch2.Update("x")
	sketch2.Update("y")
	sketch2.Update("z")

	union, _ := NewReservoirItemsUnion[string](5)
	union.UpdateSketch(sketch1)
	union.UpdateSketch(sketch2)

	result, _ := union.Result()
	assert.LessOrEqual(t, result.NumSamples(), 5)
}

func TestReservoirItemsUnionWithEmptySketch(t *testing.T) {
	sketch1, _ := NewReservoirItemsSketch[int64](10)
	emptySketch, _ := NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 5; i++ {
		sketch1.Update(i)
	}

	union, _ := NewReservoirItemsUnion[int64](10)
	union.UpdateSketch(sketch1)
	union.UpdateSketch(emptySketch) // Should not affect result

	result, _ := union.Result()
	assert.Equal(t, 5, result.NumSamples())
}

func TestReservoirItemsUnionWithNilSketch(t *testing.T) {
	union, _ := NewReservoirItemsUnion[int64](10)
	union.Update(42)
	union.UpdateSketch(nil) // Should not panic

	result, _ := union.Result()
	assert.Equal(t, 1, result.NumSamples())
}

// === New tests based on Java's ReservoirItemsUnionTest.java ===

// TestReservoirItemsUnionDownsampledUpdate tests the scenario where
// input sketch has K > union's maxK, requiring downsampling.
// Based on Java's checkDownsampledUpdate.
func TestReservoirItemsUnionDownsampledUpdate(t *testing.T) {
	const bigK = 1024
	const smallK = 256
	const n = 2048

	sketch1 := getBasicSketch(n, smallK)
	sketch2 := getBasicSketch(2*n, bigK)

	union, err := NewReservoirItemsUnion[int64](smallK)
	assert.NoError(t, err)
	assert.Equal(t, smallK, union.MaxK())

	union.UpdateSketch(sketch1)
	result, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, smallK, result.K())

	union.UpdateSketch(sketch2)
	result, err = union.Result()
	assert.NoError(t, err)
	assert.Equal(t, smallK, result.K())
	assert.Equal(t, smallK, result.NumSamples())
}

// TestReservoirItemsUnionWeightedMerge tests merging two sketches that are
// both in sampling mode (N > K).
// Based on Java's checkWeightedMerge.
func TestReservoirItemsUnionWeightedMerge(t *testing.T) {
	const k = 1024
	const n1 = 16384
	const n2 = 2048

	sketch1 := getBasicSketch(n1, k)
	sketch2 := getBasicSketch(n2, k)

	// First merge order: sketch1 then sketch2
	union, err := NewReservoirItemsUnion[int64](k)
	assert.NoError(t, err)

	union.UpdateSketch(sketch1)
	union.UpdateSketch(sketch2)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, k, result.K())
	assert.Equal(t, int64(n1+n2), result.N())
	assert.Equal(t, k, result.NumSamples())

	// Reversed merge order should yield same N
	union2, err := NewReservoirItemsUnion[int64](k)
	assert.NoError(t, err)

	union2.UpdateSketch(sketch2)
	union2.UpdateSketch(sketch1)

	result2, err := union2.Result()
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	assert.Equal(t, k, result2.K())
	assert.Equal(t, int64(n1+n2), result2.N())
	assert.Equal(t, k, result2.NumSamples())
}

// TestReservoirItemsUnionGadgetInitialization tests how the union initializes
// or updates its internal gadget based on the first input sketch.
// Based on Java's checkNewGadget.
func TestReservoirItemsUnionGadgetInitialization(t *testing.T) {
	const maxK = 1024
	const bigK = 1536
	const smallK = 128

	// Test case 1: Input K > maxK, in exact mode
	// Result should use maxK
	t.Run("InputK>MaxK_ExactMode", func(t *testing.T) {
		bigKSketch := getBasicSketch(int64(maxK/2), bigK) // n=512, k=1536, exact mode
		union, err := NewReservoirItemsUnion[int64](maxK)
		assert.NoError(t, err)

		union.UpdateSketch(bigKSketch)
		result, err := union.Result()
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, maxK, result.K())
		assert.Equal(t, int64(maxK/2), result.N())
	})

	// Test case 2: Input K < maxK and in sampling mode
	// Result should preserve input's K (Java behavior)
	t.Run("InputK<MaxK_SamplingMode", func(t *testing.T) {
		smallKSketch := getBasicSketch(int64(maxK), smallK) // n=1024, k=128, sampling mode
		union, err := NewReservoirItemsUnion[int64](maxK)
		assert.NoError(t, err)

		union.UpdateSketch(smallKSketch)
		result, err := union.Result()
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Java behavior: preserve smaller K when input is in sampling mode
		assert.Less(t, result.K(), maxK)
		assert.Equal(t, smallK, result.K())
		assert.Equal(t, int64(maxK), result.N())
	})

	// Test case 3: Input K < maxK and in exact mode
	// Result should use maxK
	t.Run("InputK<MaxK_ExactMode", func(t *testing.T) {
		smallKExactSketch := getBasicSketch(int64(smallK), smallK) // n=128, k=128, exact mode
		union, err := NewReservoirItemsUnion[int64](maxK)
		assert.NoError(t, err)

		union.UpdateSketch(smallKExactSketch)
		result, err := union.Result()
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, maxK, result.K())
		assert.Equal(t, int64(smallK), result.N())
	})
}

// TestReservoirItemsUnionStandardMerge tests merging sketches where at least
// one is in exact mode (N <= K).
// Based on Java's checkStandardMergeNoCopy.
func TestReservoirItemsUnionStandardMerge(t *testing.T) {
	const k = 1024
	const n1 = 256
	const n2 = 256

	sketch1 := getBasicSketch(n1, k)
	sketch2 := getBasicSketch(n2, k)

	union, err := NewReservoirItemsUnion[int64](k)
	assert.NoError(t, err)

	union.UpdateSketch(sketch1)
	union.UpdateSketch(sketch2)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, k, result.K())
	assert.Equal(t, int64(n1+n2), result.N())
	// Both in exact mode, so all samples should be preserved
	assert.Equal(t, n1+n2, result.NumSamples())

	// Add a third sketch that will push into sampling mode
	const n3 = 2048
	sketch3 := getBasicSketch(n3, k)
	union.UpdateSketch(sketch3)

	result, err = union.Result()
	assert.NoError(t, err)
	assert.Equal(t, k, result.K())
	assert.Equal(t, int64(n1+n2+n3), result.N())
	assert.Equal(t, k, result.NumSamples())
}

// === Serialization tests ===

func TestReservoirItemsUnionSerialization(t *testing.T) {
	t.Run("EmptyUnion", func(t *testing.T) {
		union, err := NewReservoirItemsUnion[int64](100)
		assert.NoError(t, err)

		bytes, err := union.ToSlice(Int64SerDe{})
		assert.NoError(t, err)
		assert.Equal(t, 8, len(bytes)) // Empty union is 8 bytes

		// Deserialize
		restored, err := NewReservoirItemsUnionFromSlice[int64](bytes, Int64SerDe{})
		assert.NoError(t, err)
		assert.Equal(t, 100, restored.MaxK())

		result, err := restored.Result()
		assert.NoError(t, err)
		assert.True(t, result.IsEmpty())
	})

	t.Run("NonEmptyUnion", func(t *testing.T) {
		union, err := NewReservoirItemsUnion[int64](100)
		assert.NoError(t, err)

		// Add some items
		for i := int64(0); i < 50; i++ {
			union.Update(i)
		}

		bytes, err := union.ToSlice(Int64SerDe{})
		assert.NoError(t, err)
		assert.Greater(t, len(bytes), 8) // Non-empty should be larger

		// Deserialize
		restored, err := NewReservoirItemsUnionFromSlice[int64](bytes, Int64SerDe{})
		assert.NoError(t, err)
		assert.Equal(t, 100, restored.MaxK())

		result, err := restored.Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(50), result.N())
		assert.Equal(t, 50, result.NumSamples())
	})

	t.Run("RoundTripWithSketch", func(t *testing.T) {
		const k = 64
		const n = 1000

		// Create sketch and add to union
		sketch := getBasicSketch(n, k)
		union, err := NewReservoirItemsUnion[int64](k)
		assert.NoError(t, err)
		union.UpdateSketch(sketch)

		// Serialize
		bytes, err := union.ToSlice(Int64SerDe{})
		assert.NoError(t, err)

		// Deserialize
		restored, err := NewReservoirItemsUnionFromSlice[int64](bytes, Int64SerDe{})
		assert.NoError(t, err)

		result, err := restored.Result()
		assert.NoError(t, err)
		assert.Equal(t, k, result.K())
		assert.Equal(t, int64(n), result.N())
		assert.Equal(t, k, result.NumSamples())
	})
}

// === Additional tests based on Java's ReservoirItemsUnionTest ===

// TestReservoirItemsUnionUpdateFromRaw tests the UpdateFromRaw method.
// Based on Java's checkListInputUpdate.
func TestReservoirItemsUnionUpdateFromRaw(t *testing.T) {
	const k = 32
	const n = 64

	union, err := NewReservoirItemsUnion[int64](k)
	assert.NoError(t, err)

	// Create raw data
	data := make([]int64, k)
	for i := 0; i < k; i++ {
		data[i] = int64(i)
	}

	union.UpdateFromRaw(n, k, data)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(n), result.N())
	assert.Equal(t, k, result.K())

	// Second update with larger k (should downsample)
	data2 := make([]int64, 2*k)
	for i := 0; i < 2*k; i++ {
		data2[i] = int64(i)
	}
	union.UpdateFromRaw(10*n, 2*k, data2)

	result, err = union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(11*n), result.N()) // n + 10n
	assert.Equal(t, k, result.K())           // should have downsampled
}

// TestReservoirItemsUnionEmpty tests behavior of an empty union.
// Based on Java's checkEmptyUnion.
func TestReservoirItemsUnionEmpty(t *testing.T) {
	union, err := NewReservoirItemsUnion[int64](100)
	assert.NoError(t, err)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsEmpty())
	assert.Equal(t, 100, result.K())
}

// TestReservoirItemsUnionInstantiation tests basic instantiation.
// Based on Java's checkInstantiation.
func TestReservoirItemsUnionInstantiation(t *testing.T) {
	// Valid k
	union, err := NewReservoirItemsUnion[int64](100)
	assert.NoError(t, err)
	assert.NotNil(t, union)
	assert.Equal(t, 100, union.MaxK())

	// Invalid k (too small)
	_, err = NewReservoirItemsUnion[int64](0)
	assert.Error(t, err)
}

// TestReservoirItemsUnionResetWithSmallK tests reset behavior.
// Based on Java's checkUnionResetWithInitialSmallK.
func TestReservoirItemsUnionResetWithSmallK(t *testing.T) {
	const maxK = 100
	const smallK = 25

	union, err := NewReservoirItemsUnion[int64](maxK)
	assert.NoError(t, err)

	// Add sketch with small K in sampling mode
	sketch := getBasicSketch(1000, smallK) // n=1000, k=25, sampling mode
	union.UpdateSketch(sketch)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, smallK, result.K()) // Should preserve smallK

	// Reset
	union.Reset()

	result, err = union.Result()
	assert.NoError(t, err)
	assert.True(t, result.IsEmpty())
	assert.Equal(t, maxK, result.K()) // After reset, should be maxK
}

// TestReservoirItemsUnionDeserializationErrors tests error handling during deserialization.
// Based on Java's checkBadSerVer, checkBadFamily, checkBadPreLongs.
func TestReservoirItemsUnionDeserializationErrors(t *testing.T) {
	t.Run("TooShortData", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03} // Only 3 bytes
		_, err := NewReservoirItemsUnionFromSlice[int64](data, Int64SerDe{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})

	t.Run("BadSerVer", func(t *testing.T) {
		data := make([]byte, 8)
		data[0] = 1    // preamble longs
		data[1] = 99   // invalid ser ver
		data[2] = 12   // family ID (RESERVOIR_UNION)
		data[3] = 0x04 // empty flag
		data[4] = 100  // maxK low byte
		_, err := NewReservoirItemsUnionFromSlice[int64](data, Int64SerDe{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "version")
	})

	t.Run("BadFamily", func(t *testing.T) {
		data := make([]byte, 8)
		data[0] = 1    // preamble longs
		data[1] = 2    // ser ver
		data[2] = 99   // invalid family ID
		data[3] = 0x04 // empty flag
		data[4] = 100  // maxK low byte
		_, err := NewReservoirItemsUnionFromSlice[int64](data, Int64SerDe{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "family")
	})

	t.Run("BadPreLongs", func(t *testing.T) {
		data := make([]byte, 8)
		data[0] = 5    // invalid preamble longs (should be 1)
		data[1] = 2    // ser ver
		data[2] = 12   // family ID (RESERVOIR_UNION)
		data[3] = 0x04 // empty flag
		data[4] = 100  // maxK low byte
		_, err := NewReservoirItemsUnionFromSlice[int64](data, Int64SerDe{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "preamble")
	})
}

// TestReservoirItemsUnionString tests the String() method.
func TestReservoirItemsUnionString(t *testing.T) {
	union, _ := NewReservoirItemsUnion[int64](100)
	str := union.String()
	assert.Contains(t, str, "ReservoirItemsUnion")
	assert.Contains(t, str, "Max k: 100")
	assert.Contains(t, str, "Gadget is nil")

	// Add some data
	union.Update(42)
	str = union.String()
	assert.Contains(t, str, "Gadget N:")
	assert.Contains(t, str, "Gadget K:")
}
