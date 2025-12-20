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

package filters

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalidConstructorArguments(t *testing.T) {
	// numBits = 0
	_, err := NewBloomFilterBySize(0, 3)
	assert.Error(t, err)

	// numHashes = 0
	_, err = NewBloomFilterBySize(64, 0)
	assert.Error(t, err)

	// numBits too large (overflow)
	_, err = NewBloomFilterBySize(1<<60, 3)
	assert.Error(t, err)

	// Invalid FPP
	_, err = NewBloomFilterByAccuracy(1000, 0.0)
	assert.Error(t, err)

	_, err = NewBloomFilterByAccuracy(1000, 1.0)
	assert.Error(t, err)

	// Zero max items
	_, err = NewBloomFilterByAccuracy(0, 0.01)
	assert.Error(t, err)
}

func TestStandardConstructors(t *testing.T) {
	numItems := uint64(5000)
	targetFpp := 0.01

	// Create by accuracy
	bf1, err := NewBloomFilterByAccuracy(numItems, targetFpp)
	assert.NoError(t, err)
	assert.NotNil(t, bf1)

	// Verify capacity is rounded to multiple of 64
	capacity1 := bf1.Capacity()
	assert.Equal(t, uint64(0), capacity1%64)

	// Create by size with same parameters
	numBits := SuggestNumFilterBits(numItems, targetFpp)
	numHashes := SuggestNumHashesFromSize(numItems, numBits)
	bf2, err := NewBloomFilterBySize(numBits, numHashes)
	assert.NoError(t, err)

	// Both should have same capacity and hash count
	assert.Equal(t, bf1.Capacity(), bf2.Capacity())
	assert.Equal(t, bf1.NumHashes(), bf2.NumHashes())

	// Both should be empty
	assert.True(t, bf1.IsEmpty())
	assert.True(t, bf2.IsEmpty())
	assert.Equal(t, uint64(0), bf1.BitsUsed())
	assert.Equal(t, uint64(0), bf2.BitsUsed())
}

func TestBasicOperations(t *testing.T) {
	numItems := uint64(5000)
	targetFpp := 0.01
	seed := uint64(12345)

	bf, err := NewBloomFilterByAccuracy(numItems, targetFpp, WithSeed(seed))
	assert.NoError(t, err)
	assert.Equal(t, seed, bf.Seed())

	// Initially empty
	assert.True(t, bf.IsEmpty())

	// Insert items
	for i := uint64(0); i < numItems; i++ {
		err = bf.UpdateUInt64(i)
		assert.NoError(t, err)
	}

	// No longer empty
	assert.False(t, bf.IsEmpty())

	// Check bits used is reasonable (should be around 50% for optimal parameters)
	bitsUsed := bf.BitsUsed()
	capacity := bf.Capacity()
	utilizationPercent := float64(bitsUsed) * 100.0 / float64(capacity)
	assert.Greater(t, utilizationPercent, 30.0)
	assert.Less(t, utilizationPercent, 70.0)

	// All inserted items should be found
	for i := uint64(0); i < numItems; i++ {
		assert.True(t, bf.QueryUInt64(i), "Item %d should be found", i)
	}

	// Count false positives on non-inserted items
	falsePositives := 0
	testSize := 10000
	for i := numItems; i < numItems+uint64(testSize); i++ {
		if bf.QueryUInt64(i) {
			falsePositives++
		}
	}

	// False positive rate should be within reasonable bounds of target
	actualFpp := float64(falsePositives) / float64(testSize)
	// Allow up to 3x the target FPP (probabilistic structure)
	assert.Less(t, actualFpp, targetFpp*3.0, "Actual FPP: %.4f, Target: %.4f", actualFpp, targetFpp)

	// Test Reset
	err = bf.Reset()
	assert.NoError(t, err)
	assert.True(t, bf.IsEmpty())
	assert.Equal(t, uint64(0), bf.BitsUsed())
}

func TestInversion(t *testing.T) {
	bf, err := NewBloomFilterBySize(256, 5, WithSeed(42))
	assert.NoError(t, err)

	// Insert some items
	for i := uint64(0); i < 100; i++ {
		bf.UpdateUInt64(i)
	}

	bitsUsedBefore := bf.BitsUsed()
	capacity := bf.Capacity()

	// Count items that appear present before inversion
	presentBefore := 0
	for i := uint64(0); i < 100; i++ {
		if bf.QueryUInt64(i) {
			presentBefore++
		}
	}
	assert.Equal(t, 100, presentBefore)

	// Invert
	err = bf.Invert()
	assert.NoError(t, err)

	// Bits used should be inverted
	bitsUsedAfter := bf.BitsUsed()
	assert.Equal(t, capacity-bitsUsedBefore, bitsUsedAfter)

	// Most original items should now not be present
	stillPresent := 0
	for i := uint64(0); i < 100; i++ {
		if bf.QueryUInt64(i) {
			stillPresent++
		}
	}
	assert.Less(t, stillPresent, 10) // Very few should still appear present
}

func TestIncompatibleSetOperations(t *testing.T) {
	bf1, _ := NewBloomFilterBySize(256, 5, WithSeed(42))

	// Different num_bits
	bf2, _ := NewBloomFilterBySize(512, 5, WithSeed(42))
	assert.False(t, bf1.IsCompatible(bf2))
	assert.Error(t, bf1.Union(bf2))
	assert.Error(t, bf1.Intersect(bf2))

	// Different num_hashes
	bf3, _ := NewBloomFilterBySize(256, 7, WithSeed(42))
	assert.False(t, bf1.IsCompatible(bf3))
	assert.Error(t, bf1.Union(bf3))

	// Different seed
	bf4, _ := NewBloomFilterBySize(256, 5, WithSeed(99))
	assert.False(t, bf1.IsCompatible(bf4))
	assert.Error(t, bf1.Intersect(bf4))
}

func TestBasicUnion(t *testing.T) {
	n := uint64(1000)
	bf1, _ := NewBloomFilterBySize(12288, 4, WithSeed(123))
	bf2, _ := NewBloomFilterBySize(12288, 4, WithSeed(123))

	// bf1: items 0 to n-1
	for i := uint64(0); i < n; i++ {
		bf1.UpdateUInt64(i)
	}

	// bf2: items n/2 to 3n/2-1 (overlap in middle)
	for i := n / 2; i < 3*n/2; i++ {
		bf2.UpdateUInt64(i)
	}

	// Union bf2 into bf1
	err := bf1.Union(bf2)
	assert.NoError(t, err)

	// All items from 0 to 3n/2-1 should be present
	for i := uint64(0); i < 3*n/2; i++ {
		assert.True(t, bf1.QueryUInt64(i), "Item %d should be in union", i)
	}

	// Count false positives on items beyond range
	falsePositives := 0
	testRange := 1000
	for i := 3 * n / 2; i < 3*n/2+uint64(testRange); i++ {
		if bf1.QueryUInt64(i) {
			falsePositives++
		}
	}

	fppRate := float64(falsePositives) / float64(testRange)
	assert.Less(t, fppRate, 0.20) // Should be reasonable
}

func TestBasicIntersection(t *testing.T) {
	n := uint64(1000)
	bf1, _ := NewBloomFilterBySize(12288, 4, WithSeed(456))
	bf2, _ := NewBloomFilterBySize(12288, 4, WithSeed(456))

	// bf1: items 0 to n-1
	for i := uint64(0); i < n; i++ {
		bf1.UpdateUInt64(i)
	}

	// bf2: items n/2 to 3n/2-1
	for i := n / 2; i < 3*n/2; i++ {
		bf2.UpdateUInt64(i)
	}

	// Intersect
	err := bf1.Intersect(bf2)
	assert.NoError(t, err)

	// Items in overlap (n/2 to n-1) should be present
	for i := n / 2; i < n; i++ {
		assert.True(t, bf1.QueryUInt64(i), "Item %d should be in intersection", i)
	}

	// Items outside overlap should mostly not be present
	presentOutside := 0
	for i := uint64(0); i < n/2; i++ {
		if bf1.QueryUInt64(i) {
			presentOutside++
		}
	}
	// Allow some false positives
	assert.Less(t, presentOutside, int(n/10))
}

func TestQueryAndUpdate(t *testing.T) {
	bf, _ := NewBloomFilterBySize(256, 5, WithSeed(789))

	// First call should return false (not present)
	wasPresent, err := bf.QueryAndUpdateUInt64(42)
	assert.NoError(t, err)
	assert.False(t, wasPresent)

	// Second call should return true (now present)
	wasPresent, err = bf.QueryAndUpdateUInt64(42)
	assert.NoError(t, err)
	assert.True(t, wasPresent)

	// Regular query should also return true
	assert.True(t, bf.QueryUInt64(42))
}

func TestMultipleDataTypes(t *testing.T) {
	bf, _ := NewBloomFilterBySize(512, 7)

	// Test int64
	bf.UpdateInt64(-123)
	assert.True(t, bf.QueryInt64(-123))
	assert.False(t, bf.QueryInt64(-124))

	// Test string
	bf.UpdateString("hello world")
	assert.True(t, bf.QueryString("hello world"))
	assert.False(t, bf.QueryString("hello"))

	// Test byte slice
	data := []byte{1, 2, 3, 4, 5}
	bf.UpdateSlice(data)
	assert.True(t, bf.QuerySlice(data))
	assert.False(t, bf.QuerySlice([]byte{1, 2, 3}))

	// Test float64
	bf.UpdateFloat64(3.14159)
	assert.True(t, bf.QueryFloat64(3.14159))
	assert.False(t, bf.QueryFloat64(2.71828))

	// Test NaN handling (NaN should be canonicalized)
	bf.UpdateFloat64(math.NaN())
	assert.True(t, bf.QueryFloat64(math.NaN()))

	// Test -0.0 and 0.0 are treated the same
	bf.UpdateFloat64(0.0)
	assert.True(t, bf.QueryFloat64(-0.0))
	assert.True(t, bf.QueryFloat64(0.0))
}

func TestSerializationRoundtrip(t *testing.T) {
	bf, _ := NewBloomFilterBySize(256, 5, WithSeed(999))

	// Insert some items
	for i := uint64(0); i < 50; i++ {
		bf.UpdateUInt64(i)
	}

	// Serialize
	bytes, err := bf.ToCompactSlice()
	assert.NoError(t, err)
	assert.NotNil(t, bytes)

	// Verify we can deserialize and properties match
	bf2, err := NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)
	assert.Equal(t, bf.Seed(), bf2.Seed())
	assert.Equal(t, bf.NumHashes(), bf2.NumHashes())
	assert.Equal(t, bf.Capacity(), bf2.Capacity())
	assert.Equal(t, bf.BitsUsed(), bf2.BitsUsed())
}

func TestEmptySerializationFormat(t *testing.T) {
	bf, _ := NewBloomFilterBySize(256, 5, WithSeed(111))

	// Serialize empty filter
	bytes, err := bf.ToCompactSlice()
	assert.NoError(t, err)

	// Empty filter should have shorter serialization (24 bytes)
	assert.Equal(t, 24, len(bytes))

	// Should be able to deserialize
	bf2, err := NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)
	assert.True(t, bf2.IsEmpty())
	assert.Equal(t, bf.Seed(), bf2.Seed())
	assert.Equal(t, bf.NumHashes(), bf2.NumHashes())
}

func TestSuggestFunctions(t *testing.T) {
	// Test SuggestNumFilterBits
	bits := SuggestNumFilterBits(10000, 0.01)
	assert.Greater(t, bits, uint64(0))

	// For 10000 items at 1% FPP, should be around 95850 bits
	assert.InDelta(t, 95850, bits, 1000)

	// Test SuggestNumHashes
	hashes := SuggestNumHashes(0.01)
	assert.Greater(t, hashes, uint16(0))
	// For 1% FPP, should be around 7 hashes
	assert.InDelta(t, 7, hashes, 1)

	// Test SuggestNumHashesFromSize
	hashes2 := SuggestNumHashesFromSize(10000, bits)
	assert.Greater(t, hashes2, uint16(0))
	// Should also be around 7
	assert.InDelta(t, 7, hashes2, 1)
}

func TestCapacityRounding(t *testing.T) {
	// Test that capacity is always rounded to multiple of 64
	testCases := []struct {
		input    uint64
		expected uint64
	}{
		{1, 64},
		{63, 64},
		{64, 64},
		{65, 128},
		{127, 128},
		{128, 128},
		{129, 192},
		{1000, 1024},
	}

	for _, tc := range testCases {
		result := roundCapacity(tc.input)
		assert.Equal(t, tc.expected, result, "roundCapacity(%d) should be %d", tc.input, tc.expected)
		assert.Equal(t, uint64(0), result%64, "Result should be multiple of 64")
	}
}

func TestDeserializationRoundtrip(t *testing.T) {
	// Create and populate a filter
	bf1, err := NewBloomFilterBySize(512, 7, WithSeed(12345))
	assert.NoError(t, err)

	for i := uint64(0); i < 100; i++ {
		bf1.UpdateUInt64(i)
	}

	// Serialize
	bytes, err := bf1.ToCompactSlice()
	assert.NoError(t, err)

	// Deserialize
	bf2, err := NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)

	// Verify properties match
	assert.Equal(t, bf1.Seed(), bf2.Seed())
	assert.Equal(t, bf1.NumHashes(), bf2.NumHashes())
	assert.Equal(t, bf1.Capacity(), bf2.Capacity())
	assert.Equal(t, bf1.BitsUsed(), bf2.BitsUsed())
	assert.Equal(t, bf1.IsEmpty(), bf2.IsEmpty())

	// Verify all inserted items are found
	for i := uint64(0); i < 100; i++ {
		assert.True(t, bf2.QueryUInt64(i), "Item %d should be found after deserialization", i)
	}

	// Verify queries match
	for i := uint64(100); i < 200; i++ {
		assert.Equal(t, bf1.QueryUInt64(i), bf2.QueryUInt64(i), "Query results should match for item %d", i)
	}
}

func TestDeserializeEmptyFilter(t *testing.T) {
	// Create empty filter
	bf1, err := NewBloomFilterBySize(256, 5, WithSeed(999))
	assert.NoError(t, err)

	// Serialize
	bytes, err := bf1.ToCompactSlice()
	assert.NoError(t, err)
	assert.Equal(t, 24, len(bytes)) // Empty filter is 24 bytes

	// Deserialize
	bf2, err := NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)

	// Verify properties
	assert.True(t, bf2.IsEmpty())
	assert.Equal(t, uint64(0), bf2.BitsUsed())
	assert.Equal(t, bf1.Seed(), bf2.Seed())
	assert.Equal(t, bf1.NumHashes(), bf2.NumHashes())
	assert.Equal(t, bf1.Capacity(), bf2.Capacity())
}

func TestDeserializeInvalidData(t *testing.T) {
	// Too small
	_, err := NewBloomFilterFromSlice([]byte{1, 2, 3})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient data")

	// Wrong serialization version
	bytes := make([]byte, 24)
	insertPreambleLongs(bytes, preambleLongsEmpty)
	insertSerVer(bytes)
	bytes[serVerOffset] = 99 // Invalid version
	insertFamilyID(bytes)
	insertFlags(bytes, setEmptyFlag(0))
	_, err = NewBloomFilterFromSlice(bytes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported serialization version")

	// Wrong family ID
	bytes = make([]byte, 24)
	insertPreambleLongs(bytes, preambleLongsEmpty)
	insertSerVer(bytes)
	bytes[familyIDOffset] = 99 // Invalid family
	insertFlags(bytes, setEmptyFlag(0))
	_, err = NewBloomFilterFromSlice(bytes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid family ID")

	// Zero numHashes
	bytes = make([]byte, 24)
	insertPreambleLongs(bytes, preambleLongsEmpty)
	insertSerVer(bytes)
	insertFamilyID(bytes)
	insertFlags(bytes, setEmptyFlag(0))
	insertNumHashes(bytes, 0)
	_, err = NewBloomFilterFromSlice(bytes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "numHashes must be positive")
}

func TestDeserializeWithDirtyBits(t *testing.T) {
	// Create and populate a filter
	bf1, err := NewBloomFilterBySize(256, 5, WithSeed(777))
	assert.NoError(t, err)

	for i := uint64(0); i < 50; i++ {
		bf1.UpdateUInt64(i)
	}

	// Serialize
	bytes, err := bf1.ToCompactSlice()
	assert.NoError(t, err)

	// Manually set numBitsSet to dirty value (0xFFFFFFFFFFFFFFFF)
	insertNumBitsSet(bytes, dirtyBitsValue)

	// Deserialize - should recount bits
	bf2, err := NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)

	// Should have correct bit count (recalculated)
	assert.Equal(t, bf1.BitsUsed(), bf2.BitsUsed())
	assert.Greater(t, bf2.BitsUsed(), uint64(0))
}

func TestSerializeDeserializeConsistency(t *testing.T) {
	// Test multiple serialize-deserialize cycles
	bf, err := NewBloomFilterBySize(512, 7, WithSeed(42))
	assert.NoError(t, err)

	for i := uint64(0); i < 100; i++ {
		bf.UpdateUInt64(i)
	}

	// First cycle
	bytes1, err := bf.ToCompactSlice()
	assert.NoError(t, err)

	bf2, err := NewBloomFilterFromSlice(bytes1)
	assert.NoError(t, err)

	// Second cycle
	bytes2, err := bf2.ToCompactSlice()
	assert.NoError(t, err)

	// Bytes should be identical
	assert.Equal(t, bytes1, bytes2)

	// Third cycle
	bf3, err := NewBloomFilterFromSlice(bytes2)
	assert.NoError(t, err)

	bytes3, err := bf3.ToCompactSlice()
	assert.NoError(t, err)

	// Still identical
	assert.Equal(t, bytes1, bytes3)
}

func TestHashFunctionConsistency(t *testing.T) {
	// Test that hash function produces consistent results
	seed := uint64(12345)
	bf, err := NewBloomFilterBySize(1024, 5, WithSeed(seed))
	assert.NoError(t, err)

	// Insert an item
	err = bf.UpdateInt64(42)
	assert.NoError(t, err)

	// Query it back immediately
	assert.True(t, bf.QueryInt64(42), "Item should be found immediately after insertion")

	// Serialize and deserialize
	bytes, err := bf.ToCompactSlice()
	assert.NoError(t, err)

	bf2, err := NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)

	// Still found after deserialization
	assert.True(t, bf2.QueryInt64(42), "Item should be found after deserialization")

	// Create a new filter with the same seed
	bf3, err := NewBloomFilterBySize(1024, 5, WithSeed(seed))
	assert.NoError(t, err)
	bf3.UpdateInt64(42)

	// Should produce identical serialization
	bytes3, err := bf3.ToCompactSlice()
	assert.NoError(t, err)
	assert.Equal(t, bytes, bytes3, "Same seed and items should produce identical binary output")
}

func TestArrayMethods(t *testing.T) {
	seed := uint64(99999)

	// Test int64 arrays
	intArray := []int64{1, 2, 3, 4, 5}
	bf1, _ := NewBloomFilterBySize(512, 5, WithSeed(seed))

	// Update with array
	err := bf1.UpdateInt64Array(intArray)
	assert.NoError(t, err)
	assert.False(t, bf1.IsEmpty())

	// Query array should return true
	assert.True(t, bf1.QueryInt64Array(intArray), "Should find array after update")

	// QueryAndUpdate should return true (already present)
	wasPresent, err := bf1.QueryAndUpdateInt64Array(intArray)
	assert.NoError(t, err)
	assert.True(t, wasPresent, "Array should be present")

	// Different array should not be found
	differentArray := []int64{10, 20, 30}
	assert.False(t, bf1.QueryInt64Array(differentArray), "Different array should not be found")

	// Test float64 arrays
	floatArray := []float64{1.1, 2.2, 3.3}
	bf2, _ := NewBloomFilterBySize(512, 5, WithSeed(seed))

	err = bf2.UpdateFloat64Array(floatArray)
	assert.NoError(t, err)

	assert.True(t, bf2.QueryFloat64Array(floatArray), "Should find float array after update")

	wasPresent, err = bf2.QueryAndUpdateFloat64Array(floatArray)
	assert.NoError(t, err)
	assert.True(t, wasPresent)

	// Empty arrays should be no-op
	bf3, _ := NewBloomFilterBySize(512, 5, WithSeed(seed))
	err = bf3.UpdateInt64Array([]int64{})
	assert.NoError(t, err)
	assert.True(t, bf3.IsEmpty(), "Empty array update should not change filter")

	err = bf3.UpdateFloat64Array([]float64{})
	assert.NoError(t, err)
	assert.True(t, bf3.IsEmpty(), "Empty array update should not change filter")

	// Nil arrays should be no-op
	err = bf3.UpdateInt64Array(nil)
	assert.NoError(t, err)
	assert.True(t, bf3.IsEmpty(), "Nil array update should not change filter")
}

// TestBasicUpdateMethods replicates Java's testBasicUpdateMethods
func TestBasicUpdateMethods(t *testing.T) {
	numDistinct := uint64(100)
	fpp := 1e-6
	bf, err := NewBloomFilterByAccuracy(numDistinct, fpp)
	assert.NoError(t, err)

	// Empty string should do nothing (no update)
	err = bf.UpdateString("")
	assert.NoError(t, err)
	// Querying empty string should return false (not inserted)
	wasPresent, err := bf.QueryAndUpdateString("")
	assert.NoError(t, err)
	assert.False(t, wasPresent)
	assert.Equal(t, uint64(0), bf.BitsUsed())

	// Update with non-empty string
	err = bf.UpdateString("abc")
	assert.NoError(t, err)

	// Query different string (should not be present)
	wasPresent, err = bf.QueryAndUpdateString("def")
	assert.NoError(t, err)
	assert.False(t, wasPresent)

	// Update with int
	err = bf.UpdateInt64(932)
	assert.NoError(t, err)

	// Query different int (should not be present)
	wasPresent, err = bf.QueryAndUpdateInt64(543)
	assert.NoError(t, err)
	assert.False(t, wasPresent)

	// Update with NaN
	err = bf.UpdateFloat64(math.NaN())
	assert.NoError(t, err)

	// Query positive infinity (should not be present)
	wasPresent, err = bf.QueryAndUpdateFloat64(math.Inf(1))
	assert.NoError(t, err)
	assert.False(t, wasPresent)

	// Bits used should be reasonable (at most numHashes * 6 for 6 distinct updates)
	assert.LessOrEqual(t, bf.BitsUsed(), uint64(bf.NumHashes())*6)
	assert.False(t, bf.IsEmpty())
}

// TestArrayUpdateMethods replicates Java's testArrayUpdateMethods
func TestArrayUpdateMethods(t *testing.T) {
	// Test data: 3 doubles = 24 bytes
	rawData := []float64{1.414, 2.71, 3.1415926538}

	numDistinct := uint64(100)
	fpp := 1e-6

	// Test UpdateFloat64Array (matches Java's update(double[]))
	bfDoubles, err := NewBloomFilterByAccuracy(numDistinct, fpp)
	assert.NoError(t, err)

	err = bfDoubles.UpdateFloat64Array(rawData)
	assert.NoError(t, err)

	wasPresent, err := bfDoubles.QueryAndUpdateFloat64Array(rawData)
	assert.NoError(t, err)
	assert.True(t, wasPresent, "Should find array after update")

	found := bfDoubles.QueryFloat64Array(rawData)
	assert.True(t, found, "Query should return true")

	numBitsSet := bfDoubles.BitsUsed()
	seed := bfDoubles.Seed()

	// Test with byte slice (matches Java's update(byte[]))
	bytes := make([]byte, len(rawData)*8)
	for i, val := range rawData {
		bits := math.Float64bits(val)
		binary.LittleEndian.PutUint64(bytes[i*8:], bits)
	}

	bfBytes, err := NewBloomFilterByAccuracy(numDistinct, fpp, WithSeed(seed))
	assert.NoError(t, err)

	err = bfBytes.UpdateSlice(bytes)
	assert.NoError(t, err)

	wasPresent, err = bfBytes.QueryAndUpdateSlice(bytes)
	assert.NoError(t, err)
	assert.True(t, wasPresent)

	found = bfBytes.QuerySlice(bytes)
	assert.True(t, found)

	// Both should have same number of bits set (same data, same seed)
	assert.Equal(t, numBitsSet, bfBytes.BitsUsed())

	// Test with int64 array (matches Java's update(long[]))
	intData := []int64{12345, 67890, -11111}
	bfInts, err := NewBloomFilterByAccuracy(numDistinct, fpp, WithSeed(seed))
	assert.NoError(t, err)

	err = bfInts.UpdateInt64Array(intData)
	assert.NoError(t, err)

	wasPresent, err = bfInts.QueryAndUpdateInt64Array(intData)
	assert.NoError(t, err)
	assert.True(t, wasPresent)

	found = bfInts.QueryInt64Array(intData)
	assert.True(t, found)

	// Intersect all filters (each with different data but same seed)
	bf := &bloomFilterImpl{
		seed:         seed,
		numHashes:    bfDoubles.NumHashes(),
		isDirty:      false,
		capacityBits: bfDoubles.Capacity(),
		numBitsSet:   0,
		bitArray:     make([]uint64, bfDoubles.Capacity()/64),
	}

	// Manually create a filter by intersecting
	bf.Union(bfDoubles)
	bf.Intersect(bfBytes)

	// After intersecting with itself (same data), should have same bit count
	assert.Equal(t, numBitsSet, bf.BitsUsed(),
		"Intersection of identical data should preserve bit count")
}

// TestNegativeQueries verifies that items NOT inserted are (usually) not found
func TestNegativeQueries(t *testing.T) {
	bf, err := NewBloomFilterBySize(1024, 5, WithSeed(42))
	assert.NoError(t, err)

	// Insert items 0-99
	for i := int64(0); i < 100; i++ {
		err = bf.UpdateInt64(i)
		assert.NoError(t, err)
	}

	// Test negative cases - items that were NOT inserted
	negativeItems := []int64{-1, -10, -100, 100, 101, 200, 1000, 10000}

	foundCount := 0
	for _, item := range negativeItems {
		if bf.QueryInt64(item) {
			foundCount++
		}
	}

	// Should not find ALL negative items (that would be a bug)
	assert.Less(t, foundCount, len(negativeItems),
		"Should not find all non-inserted items")

	// Specifically, -1 should almost certainly not be found
	// (allowing for small probability of false positive)
	found := bf.QueryInt64(-1)
	t.Logf("Item -1 found: %v (false positive)", found)
}
