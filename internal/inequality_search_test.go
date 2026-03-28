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

package internal

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/datasketches-go/common"
)

func float64Less(a, b float64) bool { return a < b }
func float32Less(a, b float32) bool { return a < b }
func int64Less(a, b int64) bool     { return a < b }

func buildRandFloat64Arr(rng *rand.Rand, length int) []float64 {
	arr := make([]float64, length)
	v := 1.0
	for i := 0; i < length; i++ {
		arr[i] = v
		if rng.Float64() >= 0.4 {
			v += 1.0
		}
	}
	return arr
}

func buildRandFloat32Arr(rng *rand.Rand, length int) []float32 {
	arr := make([]float32, length)
	v := float32(1.0)
	for i := 0; i < length; i++ {
		arr[i] = v
		if rng.Float64() >= 0.4 {
			v += 1.0
		}
	}
	return arr
}

func buildRandInt64Arr(rng *rand.Rand, length int) []int64 {
	arr := make([]int64, length)
	v := int64(1)
	for i := 0; i < length; i++ {
		arr[i] = v
		if rng.Float64() >= 0.4 {
			v += 2
		}
	}
	return arr
}

func TestFindWithInequalityEmptyArray(t *testing.T) {
	crits := []Inequality{InequalityLT, InequalityLE, InequalityGE, InequalityGT}
	for _, crit := range crits {
		_, err := FindWithInequality([]float64{}, 0, 0, 1.0, crit, float64Less)
		assert.EqualError(t, err, "empty array")
	}
}

func TestFindWithInequalityNotFoundWhenLowGreaterThanHigh(t *testing.T) {
	arr := []float64{1, 2, 3, 4, 5}
	crits := []Inequality{InequalityLT, InequalityLE, InequalityGE, InequalityGT}
	for _, crit := range crits {
		res, err := FindWithInequality(arr, 3, 1, 3.0, crit, float64Less)
		require.NoError(t, err)
		assert.Equal(t, -1, res)
	}
}

func TestBinarySearchFloat64Limits(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for length := 10; length <= 13; length++ {
		arr := buildRandFloat64Arr(rng, length)
		low := 2
		high := length - 2

		type testCase struct {
			name string
			v    float64
			crit Inequality
			want int
		}

		lowV := arr[low]
		highV := arr[high]

		testCases := []testCase{
			{"LT v<lowV", lowV - 1, InequalityLT, -1},
			{"LT v==lowV", lowV, InequalityLT, -1},
			{"LT v>highV", highV + 1, InequalityLT, high},
			{"LE v<lowV", lowV - 1, InequalityLE, -1},
			{"LE v==highV", highV, InequalityLE, high},
			{"LE v>highV", highV + 1, InequalityLE, high},
			{"GT v<lowV", lowV - 1, InequalityGT, low},
			{"GT v==highV", highV, InequalityGT, -1},
			{"GT v>highV", highV + 1, InequalityGT, -1},
			{"GE v<lowV", lowV - 1, InequalityGE, low},
			{"GE v==lowV", lowV, InequalityGE, low},
			{"GE v>highV", highV + 1, InequalityGE, -1},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := FindWithInequality(arr, low, high, tc.v, tc.crit, common.CompareFn[float64](float64Less))
				require.NoError(t, err)
				assert.Equal(t, tc.want, res)
			})
		}
	}
}

func TestBinarySearchFloat32Limits(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for length := 10; length <= 13; length++ {
		arr := buildRandFloat32Arr(rng, length)
		low := 2
		high := length - 2

		type testCase struct {
			name string
			v    float32
			crit Inequality
			want int
		}

		lowV := arr[low]
		highV := arr[high]

		testCases := []testCase{
			{"LT v<lowV", lowV - 1, InequalityLT, -1},
			{"LT v==lowV", lowV, InequalityLT, -1},
			{"LT v>highV", highV + 1, InequalityLT, high},
			{"LE v<lowV", lowV - 1, InequalityLE, -1},
			{"LE v==highV", highV, InequalityLE, high},
			{"LE v>highV", highV + 1, InequalityLE, high},
			{"GT v<lowV", lowV - 1, InequalityGT, low},
			{"GT v==highV", highV, InequalityGT, -1},
			{"GT v>highV", highV + 1, InequalityGT, -1},
			{"GE v<lowV", lowV - 1, InequalityGE, low},
			{"GE v==lowV", lowV, InequalityGE, low},
			{"GE v>highV", highV + 1, InequalityGE, -1},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := FindWithInequality(arr, low, high, tc.v, tc.crit, float32Less)
				require.NoError(t, err)
				assert.Equal(t, tc.want, res)
			})
		}
	}
}

func TestBinarySearchInt64Limits(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for length := 10; length <= 13; length++ {
		arr := buildRandInt64Arr(rng, length)
		low := 2
		high := length - 2

		type testCase struct {
			name string
			v    int64
			crit Inequality
			want int
		}

		lowV := arr[low]
		highV := arr[high]

		testCases := []testCase{
			{"LT v<lowV", lowV - 1, InequalityLT, -1},
			{"LT v==lowV", lowV, InequalityLT, -1},
			{"LT v>highV", highV + 1, InequalityLT, high},
			{"LE v<lowV", lowV - 1, InequalityLE, -1},
			{"LE v==highV", highV, InequalityLE, high},
			{"LE v>highV", highV + 1, InequalityLE, high},
			{"GT v<lowV", lowV - 1, InequalityGT, low},
			{"GT v==highV", highV, InequalityGT, -1},
			{"GT v>highV", highV + 1, InequalityGT, -1},
			{"GE v<lowV", lowV - 1, InequalityGE, low},
			{"GE v==lowV", lowV, InequalityGE, low},
			{"GE v>highV", highV + 1, InequalityGE, -1},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				res, err := FindWithInequality(arr, low, high, tc.v, tc.crit, int64Less)
				require.NoError(t, err)
				assert.Equal(t, tc.want, res)
			})
		}
	}
}

func TestFloat64Search(t *testing.T) {
	type testCase struct {
		name string
		arr  []float64
	}

	testCases := []testCase{
		{"single element", []float64{1}},
		{"two equal elements", []float64{1, 1}},
		{"mixed duplicates", []float64{1, 1, 1, 2, 2, 2, 3, 4, 4, 4}},
	}

	crits := []Inequality{InequalityLT, InequalityLE, InequalityGE, InequalityGT}

	for _, tc := range testCases {
		for _, crit := range crits {
			t.Run(tc.name, func(t *testing.T) {
				arr := tc.arr
				low := 0
				high := len(arr) - 1
				for v := 0.5; v <= arr[high]+0.5; v += 0.5 {
					res, err := FindWithInequality(arr, low, high, v, crit, float64Less)
					require.NoError(t, err)
					assertInequalityResult(t, arr, low, high, v, crit, res, float64Less)
				}
			})
		}
	}
}

func TestFloat32Search(t *testing.T) {
	type testCase struct {
		name string
		arr  []float32
	}

	testCases := []testCase{
		{"single element", []float32{1}},
		{"two equal elements", []float32{1, 1}},
		{"mixed duplicates", []float32{1, 1, 1, 2, 2, 2, 3, 4, 4, 4}},
	}

	crits := []Inequality{InequalityLT, InequalityLE, InequalityGE, InequalityGT}

	for _, tc := range testCases {
		for _, crit := range crits {
			t.Run(tc.name, func(t *testing.T) {
				arr := tc.arr
				low := 0
				high := len(arr) - 1
				for v := float32(0.5); v <= arr[high]+0.5; v += 0.5 {
					res, err := FindWithInequality(arr, low, high, v, crit, float32Less)
					require.NoError(t, err)
					assertInequalityResult(t, arr, low, high, v, crit, res, float32Less)
				}
			})
		}
	}
}

func TestInt64Search(t *testing.T) {
	type testCase struct {
		name string
		arr  []int64
	}

	testCases := []testCase{
		{"single element", []int64{1}},
		{"two equal elements", []int64{1, 1}},
		{"mixed duplicates", []int64{1, 1, 1, 2, 2, 2, 3, 4, 4, 4}},
	}

	crits := []Inequality{InequalityLT, InequalityLE, InequalityGE, InequalityGT}

	for _, tc := range testCases {
		for _, crit := range crits {
			t.Run(tc.name, func(t *testing.T) {
				arr := tc.arr
				low := 0
				high := len(arr) - 1
				for v := int64(0); v <= arr[high]+1; v++ {
					res, err := FindWithInequality(arr, low, high, v, crit, int64Less)
					require.NoError(t, err)
					assertInequalityResult(t, arr, low, high, v, crit, res, int64Less)
				}
			})
		}
	}
}

func assertInequalityResult[C comparable](t *testing.T, arr []C, low, high int, v C, crit Inequality, res int, less func(C, C) bool) {
	t.Helper()
	if res == -1 {
		return
	}
	assert.GreaterOrEqual(t, res, low, "result index below low bound")
	assert.LessOrEqual(t, res, high, "result index above high bound")

	switch crit {
	case InequalityLT:
		// arr[res] < v
		assert.True(t, less(arr[res], v), "LT: arr[%d] should be < v(%v), got %v", res, v, arr[res])
	case InequalityLE:
		// arr[res] <= v
		assert.True(t, less(arr[res], v) || arr[res] == v, "LE: arr[%d] should be <= v(%v), got %v", res, v, arr[res])
	case InequalityGE:
		// arr[res] >= v
		assert.True(t, less(v, arr[res]) || arr[res] == v, "GE: arr[%d] should be >= v(%v), got %v", res, v, arr[res])
	case InequalityGT:
		// arr[res] > v
		assert.True(t, less(v, arr[res]), "GT: arr[%d] should be > v(%v), got %v", res, v, arr[res])
	}
}
