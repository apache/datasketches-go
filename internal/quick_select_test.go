package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuickSelect(t *testing.T) {
	testCases := []struct {
		name     string
		arr      []int
		lo       int
		hi       int
		pivot    int
		expected int
	}{
		{
			name:     "find median in odd length array",
			arr:      []int{3, 1, 4, 1, 5, 9, 2, 6},
			lo:       0,
			hi:       7,
			pivot:    4,
			expected: 4,
		},
		{
			name:     "find minimum",
			arr:      []int{3, 1, 4, 1, 5, 9, 2, 6},
			lo:       0,
			hi:       7,
			pivot:    0,
			expected: 1,
		},
		{
			name:     "find maximum",
			arr:      []int{3, 1, 4, 1, 5, 9, 2, 6},
			lo:       0,
			hi:       7,
			pivot:    7,
			expected: 9,
		},
		{
			name:     "single element array",
			arr:      []int{42},
			lo:       0,
			hi:       0,
			pivot:    0,
			expected: 42,
		},
		{
			name:     "two element array - first",
			arr:      []int{5, 3},
			lo:       0,
			hi:       1,
			pivot:    0,
			expected: 3,
		},
		{
			name:     "two element array - second",
			arr:      []int{5, 3},
			lo:       0,
			hi:       1,
			pivot:    1,
			expected: 5,
		},
		{
			name:     "already sorted array",
			arr:      []int{1, 2, 3, 4, 5},
			lo:       0,
			hi:       4,
			pivot:    2,
			expected: 3,
		},
		{
			name:     "reverse sorted array",
			arr:      []int{5, 4, 3, 2, 1},
			lo:       0,
			hi:       4,
			pivot:    2,
			expected: 3,
		},
		{
			name:     "array with duplicates",
			arr:      []int{3, 3, 3, 3, 3},
			lo:       0,
			hi:       4,
			pivot:    2,
			expected: 3,
		},
		{
			name:     "partial range - middle elements",
			arr:      []int{9, 8, 7, 6, 5, 4, 3, 2, 1},
			lo:       2,
			hi:       6,
			pivot:    4,
			expected: 5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Make a copy to avoid modifying the test case
			arrCopy := make([]int, len(tc.arr))
			copy(arrCopy, tc.arr)

			result := QuickSelect(arrCopy, tc.lo, tc.hi, tc.pivot)

			assert.Equal(t, tc.expected, result, "want: %v\ngot: %v", tc.expected, result)
		})
	}
}

func TestQuickSelectFloat64(t *testing.T) {
	arr := []float64{3.14, 1.41, 2.71, 0.57, 1.61}
	result := QuickSelect(arr, 0, 4, 2)
	expected := 1.61

	assert.Equal(t, expected, result, "want: %v\ngot: %v", expected, result)
}

func TestQuickSelectString(t *testing.T) {
	arr := []string{"dog", "cat", "elephant", "ant", "bear"}
	result := QuickSelect(arr, 0, 4, 2)
	expected := "cat"

	assert.Equal(t, expected, result, "want: %v\ngot: %v", expected, result)
}
