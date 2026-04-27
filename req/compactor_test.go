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

package req

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompactorPromoteEvensOrOddsInto(t *testing.T) {
	items := []float32{5, 3, 1, 7, 9, 2}

	tests := []struct {
		name        string
		hra         bool
		odds        bool
		startOffset int
		endOffset   int
		expected    []float32
	}{
		{"LRA evens", false, false, 2, 6, []float32{3, 7}},
		{"LRA odds", false, true, 2, 6, []float32{5, 9}},
		{"HRA evens", true, false, 0, 4, []float32{1, 3}},
		{"HRA odds", true, true, 0, 4, []float32{2, 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newCompactor(0, tt.hra, minK)
			for _, v := range items {
				c.Append(v)
			}
			next := newCompactor(1, tt.hra, minK)

			promoteCount, err := c.promoteEvensOrOddsInto(next, tt.startOffset, tt.endOffset, tt.odds)
			assert.NoError(t, err)
			assert.Equal(t, 2, promoteCount)
			assert.Equal(t, 2, next.Count())
			for i, want := range tt.expected {
				assert.Equal(t, want, next.Item(i))
			}
		})
	}
}

func TestCompactorPromoteEvensOrOddsIntoOddRangeError(t *testing.T) {
	c := newCompactor(0, false, minK)
	c.Append(3)
	c.Append(2)
	c.Append(1)

	c.TrimCount(4)
	assert.Equal(t, 3, c.Count())

	cnt, err := c.countWithCriterion(3.0, true)
	assert.NoError(t, err)
	assert.Equal(t, 3, cnt)

	assert.Equal(t, float32(3), c.Item(2))

	next := newCompactor(1, false, minK)
	_, err = c.promoteEvensOrOddsInto(next, 0, 3, false)
	assert.EqualError(t, err, "input range size must be even")
}

func TestCompactorAppend(t *testing.T) {
	t.Run("LRA mode", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		initialCap := c.Capacity()
		c.Append(10)
		c.Append(20)
		c.Append(30)
		assert.Equal(t, 3, c.Count())
		assert.Equal(t, initialCap, c.Capacity())
		assert.Equal(t, float32(10), c.Item(0))
		assert.Equal(t, float32(20), c.Item(1))
		assert.Equal(t, float32(30), c.Item(2))
	})

	t.Run("HRA mode", func(t *testing.T) {
		c := newCompactor(0, true, minK)
		initialCap := c.Capacity()
		c.Append(10)
		c.Append(20)
		c.Append(30)
		assert.Equal(t, 3, c.Count())
		assert.Equal(t, initialCap, c.Capacity())
		assert.Equal(t, float32(30), c.Item(0))
		assert.Equal(t, float32(20), c.Item(1))
		assert.Equal(t, float32(10), c.Item(2))
	})

	t.Run("LRA mode with reallocation", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		initialCap := c.Capacity()
		for i := 0; i < initialCap; i++ {
			c.Append(float32(i))
		}
		assert.Equal(t, initialCap, c.Capacity())
		c.Append(float32(initialCap))
		assert.Equal(t, initialCap+1, c.Count())
		assert.Greater(t, c.Capacity(), initialCap)
		for i := 0; i <= initialCap; i++ {
			assert.Equal(t, float32(i), c.Item(i))
		}
	})

	t.Run("HRA mode with reallocation", func(t *testing.T) {
		c := newCompactor(0, true, minK)
		initialCap := c.Capacity()
		for i := 0; i < initialCap; i++ {
			c.Append(float32(i))
		}
		assert.Equal(t, initialCap, c.Capacity())
		c.Append(float32(initialCap))
		assert.Equal(t, initialCap+1, c.Count())
		assert.Greater(t, c.Capacity(), initialCap)
		for i := 0; i <= initialCap; i++ {
			assert.Equal(t, float32(initialCap-i), c.Item(i))
		}
	})
}

func TestCompactorCountWithCriterion(t *testing.T) {
	createSortedCompactor := func(hra bool, length int) *compactor {
		c := newCompactor(0, hra, minK)
		for i := 0; i < length; i++ {
			c.Append(float32(i + 1))
		}
		return c
	}

	checkCountWithCriteria := func(t *testing.T, c *compactor, v float32) {
		t.Helper()
		length := c.Count()
		iv := int(v)

		// exclusive (< v)
		var expectedExcl int
		if v > float32(length) {
			expectedExcl = length
		} else if v <= 1 {
			expectedExcl = 0
		} else if float32(iv) == v {
			expectedExcl = iv - 1
		} else {
			expectedExcl = iv
		}
		gotExcl, err := c.countWithCriterion(v, false)
		assert.NoError(t, err)
		assert.Equal(t, expectedExcl, gotExcl, "exclusive count for v=%v", v)

		// inclusive (<= v)
		var expectedIncl int
		if v >= float32(length) {
			expectedIncl = length
		} else if v < 1 {
			expectedIncl = 0
		} else {
			expectedIncl = iv
		}
		gotIncl, err := c.countWithCriterion(v, true)
		assert.NoError(t, err)
		assert.Equal(t, expectedIncl, gotIncl, "inclusive count for v=%v", v)
	}

	for length := 5; length < 10; length++ {
		for _, hra := range []bool{false, true} {
			name := "LRA"
			if hra {
				name = "HRA"
			}
			t.Run(fmt.Sprintf("%s len=%d", name, length), func(t *testing.T) {
				c := createSortedCompactor(hra, length)
				for v := float32(0.5); v <= float32(length)+0.5; v += 0.5 {
					checkCountWithCriteria(t, c, v)
				}
			})
		}
	}

	checkCountLessThan := func(t *testing.T, hra bool) {
		t.Helper()
		sorted := []float32{1, 2, 3, 4, 5, 6, 7}
		src := newCompactor(0, hra, minK)
		for _, v := range sorted {
			src.Append(v)
		}
		src.Sort()

		c := newCompactor(0, hra, minK)

		err := c.mergeSortIn(src)
		assert.NoError(t, err)

		got, err := c.countWithCriterion(4, false)
		assert.NoError(t, err)
		assert.Equal(t, 3, got)

		err = c.mergeSortIn(src)
		assert.NoError(t, err)

		got, err = c.countWithCriterion(4, false)
		assert.NoError(t, err)
		assert.Equal(t, 6, got)
		assert.Equal(t, 14, c.Count())
	}

	t.Run("LRA duplicates", func(t *testing.T) {
		checkCountLessThan(t, false)
	})

	t.Run("HRA duplicates", func(t *testing.T) {
		checkCountLessThan(t, true)
	})

	t.Run("NaN returns error", func(t *testing.T) {
		c := createSortedCompactor(false, 5)
		_, err := c.countWithCriterion(float32(math.NaN()), true)
		assert.ErrorContains(t, err, "float items must not be NaN")
	})
}

func TestCompactorMergeSortIn(t *testing.T) {
	t.Run("LRA", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		c.Append(1)
		c.Append(3)
		c.Append(5)
		c.Sort()

		other := newCompactor(0, false, minK)
		other.Append(2)
		other.Append(4)
		other.Append(6)
		other.Sort()

		err := c.mergeSortIn(other)
		assert.NoError(t, err)
		assert.Equal(t, 6, c.Count())

		expected := []float32{1, 2, 3, 4, 5, 6}
		for i, want := range expected {
			assert.Equal(t, want, c.Item(i))
		}
	})

	t.Run("HRA", func(t *testing.T) {
		c := newCompactor(0, true, minK)
		c.Append(1)
		c.Append(3)
		c.Append(5)
		c.Sort()

		src := newCompactor(0, true, minK)
		src.Append(2)
		src.Append(4)
		src.Append(6)
		src.Sort()

		err := c.mergeSortIn(src)
		assert.NoError(t, err)
		assert.Equal(t, 6, c.Count())

		expected := []float32{1, 2, 3, 4, 5, 6}
		for i, want := range expected {
			assert.Equal(t, want, c.Item(i))
		}
	})
}

func TestCompactorTrimCount(t *testing.T) {
	t.Run("trim to smaller count", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		for i := 0; i < 5; i++ {
			c.Append(float32(i))
		}
		c.TrimCount(3)
		assert.Equal(t, 3, c.Count())
	})

	t.Run("trim to larger count", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		for i := 0; i < 5; i++ {
			c.Append(float32(i))
		}
		c.TrimCount(10)
		assert.Equal(t, 5, c.Count())
	})

	t.Run("trim on empty compactor", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		c.TrimCount(0)
		assert.Equal(t, 0, c.Count())
	})
}

func TestCompactorTrimCapacity(t *testing.T) {
	t.Run("LRA count less than capacity", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		c.Append(1)
		c.Append(2)
		c.Append(3)
		assert.Greater(t, c.Capacity(), c.Count())

		c.TrimCapacity()
		assert.Equal(t, 3, c.Count())
		assert.Equal(t, 3, c.Capacity())
		assert.Equal(t, float32(1), c.Item(0))
		assert.Equal(t, float32(2), c.Item(1))
		assert.Equal(t, float32(3), c.Item(2))
	})

	t.Run("LRA count equals capacity", func(t *testing.T) {
		c := newCompactor(0, false, minK)
		initialCap := c.Capacity()
		for i := 0; i < initialCap; i++ {
			c.Append(float32(i))
		}
		assert.Equal(t, c.Count(), c.Capacity())

		c.TrimCapacity()
		assert.Equal(t, initialCap, c.Count())
		assert.Equal(t, initialCap, c.Capacity())
	})

	t.Run("HRA count less than capacity", func(t *testing.T) {
		c := newCompactor(0, true, minK)
		c.Append(1)
		c.Append(2)
		c.Append(3)
		assert.Greater(t, c.Capacity(), c.Count())

		c.TrimCapacity()
		assert.Equal(t, 3, c.Count())
		assert.Equal(t, 3, c.Capacity())
		assert.Equal(t, float32(3), c.Item(0))
		assert.Equal(t, float32(2), c.Item(1))
		assert.Equal(t, float32(1), c.Item(2))
	})

	t.Run("HRA count equals capacity", func(t *testing.T) {
		c := newCompactor(0, true, minK)
		initialCap := c.Capacity()
		for i := 0; i < initialCap; i++ {
			c.Append(float32(i))
		}
		assert.Equal(t, c.Count(), c.Capacity())

		c.TrimCapacity()
		assert.Equal(t, initialCap, c.Count())
		assert.Equal(t, initialCap, c.Capacity())
	})
}

func TestNearestEven(t *testing.T) {
	tests := []struct {
		input    float32
		expected int
	}{
		{0.0, 0},
		{1.0, 2},
		{2.5, 2},
		{3.5, 4},
		{5.0, 6},
		{-1.0, -2},
		{-2.5, -2},
		{-3.5, -4},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%v", tt.input), func(t *testing.T) {
			assert.Equal(t, tt.expected, nearestEven(tt.input))
		})
	}
}

func TestNumberOfTrailingOnes(t *testing.T) {
	tests := []struct {
		input    int64
		expected int
	}{
		{0b0, 0},
		{0b1, 1},
		{0b111, 3},
		{0b1011, 2},
		{-1, 64},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%d", tt.input), func(t *testing.T) {
			assert.Equal(t, tt.expected, numberOfTrailingOnes(tt.input))
		})
	}
}

func TestCompactorGetters(t *testing.T) {
	c := newCompactor(0, true, 12)
	assert.False(t, c.Coin())
	assert.Greater(t, c.NumSections(), 0)
	assert.Greater(t, c.SectionSize(), 0)
	assert.Greater(t, c.SectionSizeFlt(), float32(0))
	assert.True(t, c.IsHighRankAccuracyMode())
	assert.Equal(t, int64(0), c.State())
}

// TestCompactorItemsSerDe tests marshalItems/decode round-trip with raw array
// position checks, ported from Java ReqFloatBufferTest.checkSerDe.
func TestCompactorItemsSerDe(t *testing.T) {
	t.Run("HRA", func(t *testing.T) {
		runCompactorItemsSerDe(t, true)
	})
	t.Run("LRA", func(t *testing.T) {
		runCompactorItemsSerDe(t, false)
	})
}

func runCompactorItemsSerDe(t *testing.T, hra bool) {
	t.Helper()
	c := newCompactor(0, hra, minK)
	initialCap := c.Capacity()

	// Append more items than initial capacity to trigger growth.
	numItems := initialCap + 1
	for i := 0; i < numItems; i++ {
		c.Append(float32(i))
	}
	assert.Greater(t, c.Capacity(), initialCap)

	capacity := c.Capacity()
	count := c.Count()
	sorted := c.sorted

	// Verify raw item positions before serialization.
	if hra {
		assert.Equal(t, float32(numItems-1), c.items[capacity-count])
		assert.Equal(t, float32(0), c.items[capacity-1])
	} else {
		assert.Equal(t, float32(0), c.items[0])
		assert.Equal(t, float32(numItems-1), c.items[count-1])
	}

	// Verify marshalItems byte output matches Item(i) order.
	itemBytes := c.marshalItems()
	assert.Equal(t, count*4, len(itemBytes))
	for i := 0; i < count; i++ {
		bits := binary.LittleEndian.Uint32(itemBytes[i*4:])
		got := math.Float32frombits(bits)
		assert.Equal(t, c.Item(i), got, "serialized item mismatch at offset %d", i)
	}

	// Full round-trip via MarshalBinary + decodeCompactor.
	fullBytes, err := c.MarshalBinary()
	assert.NoError(t, err)
	result, err := decodeCompactor(fullBytes, 0, sorted, hra)
	assert.NoError(t, err)
	c2 := result.compactor

	assert.Equal(t, count, c2.Count())
	assert.Equal(t, sorted, c2.sorted)
	assert.Equal(t, hra, c2.isHighRankAccuracyMode)

	// Verify raw positions in deserialized compactor.
	if hra {
		cap2 := c2.Capacity()
		assert.Equal(t, float32(numItems-1), c2.items[cap2-c2.count])
		assert.Equal(t, float32(0), c2.items[cap2-1])
	} else {
		assert.Equal(t, float32(0), c2.items[0])
		assert.Equal(t, float32(numItems-1), c2.items[c2.count-1])
	}

	// Verify all items match by logical offset.
	for i := 0; i < count; i++ {
		assert.Equal(t, c.Item(i), c2.Item(i), "item mismatch at offset %d", i)
	}
}

func TestCompactorSerializationDeserialization(t *testing.T) {
	t.Run("LRA", func(t *testing.T) {
		runCompactorSerializationDeserialization(t, 12, false)
	})
	t.Run("HRA", func(t *testing.T) {
		runCompactorSerializationDeserialization(t, 12, true)
	})
}

func runCompactorSerializationDeserialization(t *testing.T, k int, hra bool) {
	t.Helper()
	c1 := newCompactor(0, hra, k)
	nomCap := nomCapMul * initNumberOfSections * k
	expectedCap := 2 * nomCap
	expectedDelta := nomCap

	for i := 1; i <= nomCap; i++ {
		c1.Append(float32(i))
	}

	sectionSizeFlt := c1.SectionSizeFlt()
	sectionSize := c1.SectionSize()
	numSections := c1.NumSections()
	state := c1.State()
	lgWt := c1.lgWeight
	sorted := c1.sorted

	// serialize
	c1ser, err := c1.MarshalBinary()
	assert.NoError(t, err)

	// deserialize via buffer
	result, err := decodeCompactor(c1ser, 0, sorted, hra)
	assert.NoError(t, err)
	c2 := result.compactor

	assert.Equal(t, float32(1), result.minItem)
	assert.Equal(t, float32(nomCap), result.maxItem)
	assert.Equal(t, int64(nomCap), result.n)
	assert.Equal(t, sectionSizeFlt, c2.SectionSizeFlt())
	assert.Equal(t, sectionSize, c2.SectionSize())
	assert.Equal(t, numSections, c2.NumSections())
	assert.Equal(t, state, c2.State())
	assert.Equal(t, lgWt, c2.lgWeight)
	assert.Equal(t, hra, c2.IsHighRankAccuracyMode())
	if hra {
		assert.Equal(t, expectedCap, c2.Capacity())
	} else {
		// LRA decoder keeps items at count-sized capacity (not expanded to nomCap).
		assert.Equal(t, nomCap, c2.Capacity())
	}
	assert.Equal(t, expectedDelta, c2.delta)

	for i := 0; i < nomCap; i++ {
		assert.Equal(t, c1.Item(i), c2.Item(i), "item mismatch at offset %d", i)
	}

	// deserialize via stream
	decoder := newCompactorDecoder(sorted, hra)
	result2, err := decoder.Decode(bytes.NewReader(c1ser))
	assert.NoError(t, err)
	c3 := result2.compactor

	assert.Equal(t, float32(1), result2.minItem)
	assert.Equal(t, float32(nomCap), result2.maxItem)
	assert.Equal(t, int64(nomCap), result2.n)
	assert.Equal(t, sectionSizeFlt, c3.SectionSizeFlt())
	assert.Equal(t, sectionSize, c3.SectionSize())
	for i := 0; i < nomCap; i++ {
		assert.Equal(t, c1.Item(i), c3.Item(i), "stream: item mismatch at offset %d", i)
	}
}

func TestCompactorSerializationDeserializationWithNegativeValues(t *testing.T) {
	t.Run("LRA", func(t *testing.T) {
		runCompactorSerializationDeserializationNegative(t, 12, false)
	})
	t.Run("HRA", func(t *testing.T) {
		runCompactorSerializationDeserializationNegative(t, 12, true)
	})
}

func runCompactorSerializationDeserializationNegative(t *testing.T, k int, hra bool) {
	t.Helper()
	c1 := newCompactor(0, hra, k)
	nomCap := nomCapMul * initNumberOfSections * k

	for i := 1; i <= nomCap; i++ {
		c1.Append(float32(-i))
	}

	c1ser, err := c1.MarshalBinary()
	assert.NoError(t, err)

	result, err := decodeCompactor(c1ser, 0, c1.sorted, hra)
	assert.NoError(t, err)
	assert.Equal(t, float32(-nomCap), result.minItem)
	assert.Equal(t, float32(-1), result.maxItem)
}

func TestCompactorSerializationDeserializationWithMixedValues(t *testing.T) {
	t.Run("LRA", func(t *testing.T) {
		runCompactorSerializationDeserializationMixed(t, 12, false)
	})
	t.Run("HRA", func(t *testing.T) {
		runCompactorSerializationDeserializationMixed(t, 12, true)
	})
}

func runCompactorSerializationDeserializationMixed(t *testing.T, k int, hra bool) {
	t.Helper()
	c1 := newCompactor(0, hra, k)
	nomCap := nomCapMul * initNumberOfSections * k
	half := nomCap / 2

	for i := 0; i < nomCap; i++ {
		c1.Append(float32(i - half))
	}

	c1ser, err := c1.MarshalBinary()
	assert.NoError(t, err)

	result, err := decodeCompactor(c1ser, 0, c1.sorted, hra)
	assert.NoError(t, err)
	assert.Equal(t, float32(-half), result.minItem)
	assert.Equal(t, float32(half-1), result.maxItem)
}
