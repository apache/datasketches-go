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
