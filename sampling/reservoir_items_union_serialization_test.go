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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGoUnionBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	err := os.MkdirAll(internal.GoPath, os.ModePerm)
	assert.NoError(t, err)

	exactNValues := []int{1, 10, 32, 100, 128}
	samplingKValues := []int{32, 64, 128}

	// ========== ReservoirItemsUnion<Long> (9 files) ==========
	t.Run("union_long", func(t *testing.T) {
		// Empty
		t.Run("empty_maxk128", func(t *testing.T) {
			union, _ := NewReservoirItemsUnion[int64](128)
			data, _ := union.ToSlice(Int64SerDe{})
			os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_long_empty_maxk128_go.sk", internal.GoPath), data, 0644)
		})
		// Exact
		for _, n := range exactNValues {
			n := n
			t.Run(fmt.Sprintf("exact_n%d_maxk128", n), func(t *testing.T) {
				union, _ := NewReservoirItemsUnion[int64](128)
				for i := 0; i < n; i++ {
					union.Update(int64(i))
				}
				data, _ := union.ToSlice(Int64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_long_exact_n%d_maxk128_go.sk", internal.GoPath, n), data, 0644)
			})
		}
		// Sampling
		for _, k := range samplingKValues {
			k := k
			t.Run(fmt.Sprintf("sampling_n1000_maxk%d", k), func(t *testing.T) {
				union, _ := NewReservoirItemsUnion[int64](k)
				for i := 0; i < 1000; i++ {
					union.Update(int64(i))
				}
				data, _ := union.ToSlice(Int64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_long_sampling_n1000_maxk%d_go.sk", internal.GoPath, k), data, 0644)
			})
		}
	})

	// ========== ReservoirItemsUnion<Double> (9 files) ==========
	t.Run("union_double", func(t *testing.T) {
		// Empty
		t.Run("empty_maxk128", func(t *testing.T) {
			union, _ := NewReservoirItemsUnion[float64](128)
			data, _ := union.ToSlice(Float64SerDe{})
			os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_double_empty_maxk128_go.sk", internal.GoPath), data, 0644)
		})
		// Exact
		for _, n := range exactNValues {
			n := n
			t.Run(fmt.Sprintf("exact_n%d_maxk128", n), func(t *testing.T) {
				union, _ := NewReservoirItemsUnion[float64](128)
				for i := 0; i < n; i++ {
					union.Update(float64(i))
				}
				data, _ := union.ToSlice(Float64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_double_exact_n%d_maxk128_go.sk", internal.GoPath, n), data, 0644)
			})
		}
		// Sampling
		for _, k := range samplingKValues {
			k := k
			t.Run(fmt.Sprintf("sampling_n1000_maxk%d", k), func(t *testing.T) {
				union, _ := NewReservoirItemsUnion[float64](k)
				for i := 0; i < 1000; i++ {
					union.Update(float64(i))
				}
				data, _ := union.ToSlice(Float64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_double_sampling_n1000_maxk%d_go.sk", internal.GoPath, k), data, 0644)
			})
		}
	})

	// ========== ReservoirItemsUnion<String> (9 files) ==========
	t.Run("union_string", func(t *testing.T) {
		// Empty
		t.Run("empty_maxk128", func(t *testing.T) {
			union, _ := NewReservoirItemsUnion[string](128)
			data, _ := union.ToSlice(StringSerDe{})
			os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_string_empty_maxk128_go.sk", internal.GoPath), data, 0644)
		})
		// Exact
		for _, n := range exactNValues {
			n := n
			t.Run(fmt.Sprintf("exact_n%d_maxk128", n), func(t *testing.T) {
				union, _ := NewReservoirItemsUnion[string](128)
				for i := 0; i < n; i++ {
					union.Update(fmt.Sprintf("item%d", i))
				}
				data, _ := union.ToSlice(StringSerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_string_exact_n%d_maxk128_go.sk", internal.GoPath, n), data, 0644)
			})
		}
		// Sampling
		for _, k := range samplingKValues {
			k := k
			t.Run(fmt.Sprintf("sampling_n1000_maxk%d", k), func(t *testing.T) {
				union, _ := NewReservoirItemsUnion[string](k)
				for i := 0; i < 1000; i++ {
					union.Update(fmt.Sprintf("item%d", i))
				}
				data, _ := union.ToSlice(StringSerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_union_string_sampling_n1000_maxk%d_go.sk", internal.GoPath, k), data, 0644)
			})
		}
	})
}

// TestReservoirItemsUnion_JavaCompat tests deserialization of Java-generated reservoir union files.
// Validates both longs-specialization and generic items unions for long/double/string.
func TestReservoirItemsUnion_JavaCompat(t *testing.T) {
	makeRangeInt64 := func(n int) []int64 {
		out := make([]int64, n)
		for i := 0; i < n; i++ {
			out[i] = int64(i)
		}
		return out
	}
	makeRangeFloat64 := func(n int) []float64 {
		out := make([]float64, n)
		for i := 0; i < n; i++ {
			out[i] = float64(i)
		}
		return out
	}
	makeRangeString := func(n int) []string {
		out := make([]string, n)
		for i := 0; i < n; i++ {
			out[i] = fmt.Sprintf("item%d", i)
		}
		return out
	}
	makeEvenInt64 := func(k int) []int64 {
		out := make([]int64, k)
		for i := 0; i < k; i++ {
			out[i] = int64(i * 2)
		}
		return out
	}
	makeEvenFloat64 := func(k int) []float64 {
		out := make([]float64, k)
		for i := 0; i < k; i++ {
			out[i] = float64(i * 2)
		}
		return out
	}
	makeEvenString := func(k int) []string {
		out := make([]string, k)
		for i := 0; i < k; i++ {
			out[i] = fmt.Sprintf("item%d", i*2)
		}
		return out
	}

	t.Run("items_union_long", func(t *testing.T) {
		cases := []struct {
			name     string
			filename string
			maxK     int
			n        int64
			isEmpty  bool
			expected []int64
		}{
			{"empty_maxk128", "reservoir_items_union_long_empty_maxk128_java.sk", 128, 0, true, nil},
			{"exact_n1_maxk128", "reservoir_items_union_long_exact_n1_maxk128_java.sk", 128, 1, false, makeRangeInt64(1)},
			{"exact_n10_maxk128", "reservoir_items_union_long_exact_n10_maxk128_java.sk", 128, 10, false, makeRangeInt64(10)},
			{"exact_n32_maxk128", "reservoir_items_union_long_exact_n32_maxk128_java.sk", 128, 32, false, makeRangeInt64(32)},
			{"exact_n100_maxk128", "reservoir_items_union_long_exact_n100_maxk128_java.sk", 128, 100, false, makeRangeInt64(100)},
			{"exact_n128_maxk128", "reservoir_items_union_long_exact_n128_maxk128_java.sk", 128, 128, false, makeRangeInt64(128)},
			{"sampling_n1000_maxk32", "reservoir_items_union_long_sampling_n1000_maxk32_java.sk", 32, 1000, false, makeEvenInt64(32)},
			{"sampling_n1000_maxk64", "reservoir_items_union_long_sampling_n1000_maxk64_java.sk", 64, 1000, false, makeEvenInt64(64)},
			{"sampling_n1000_maxk128", "reservoir_items_union_long_sampling_n1000_maxk128_java.sk", 128, 1000, false, makeEvenInt64(128)},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				path := filepath.Join(internal.JavaPath, tc.filename)
				if _, err := os.Stat(path); os.IsNotExist(err) {
					t.Skipf("Java file not found: %s", tc.filename)
					return
				}
				data, err := os.ReadFile(path)
				assert.NoError(t, err)

				union, err := NewReservoirItemsUnionFromSlice[int64](data, Int64SerDe{})
				assert.NoError(t, err)
				result, err := union.Result()
				assert.NoError(t, err)
				assert.Equal(t, tc.maxK, result.K())
				assert.Equal(t, tc.n, result.N())
				assert.Equal(t, tc.isEmpty, result.IsEmpty())

				if tc.expected != nil {
					assert.Equal(t, tc.expected, result.Samples())
				} else {
					assert.Equal(t, 0, result.NumSamples())
				}
			})
		}
	})

	t.Run("items_union_double", func(t *testing.T) {
		cases := []struct {
			name     string
			filename string
			maxK     int
			n        int64
			isEmpty  bool
			expected []float64
		}{
			{"empty_maxk128", "reservoir_items_union_double_empty_maxk128_java.sk", 128, 0, true, nil},
			{"exact_n1_maxk128", "reservoir_items_union_double_exact_n1_maxk128_java.sk", 128, 1, false, makeRangeFloat64(1)},
			{"exact_n10_maxk128", "reservoir_items_union_double_exact_n10_maxk128_java.sk", 128, 10, false, makeRangeFloat64(10)},
			{"exact_n32_maxk128", "reservoir_items_union_double_exact_n32_maxk128_java.sk", 128, 32, false, makeRangeFloat64(32)},
			{"exact_n100_maxk128", "reservoir_items_union_double_exact_n100_maxk128_java.sk", 128, 100, false, makeRangeFloat64(100)},
			{"exact_n128_maxk128", "reservoir_items_union_double_exact_n128_maxk128_java.sk", 128, 128, false, makeRangeFloat64(128)},
			{"sampling_n1000_maxk32", "reservoir_items_union_double_sampling_n1000_maxk32_java.sk", 32, 1000, false, makeEvenFloat64(32)},
			{"sampling_n1000_maxk64", "reservoir_items_union_double_sampling_n1000_maxk64_java.sk", 64, 1000, false, makeEvenFloat64(64)},
			{"sampling_n1000_maxk128", "reservoir_items_union_double_sampling_n1000_maxk128_java.sk", 128, 1000, false, makeEvenFloat64(128)},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				path := filepath.Join(internal.JavaPath, tc.filename)
				if _, err := os.Stat(path); os.IsNotExist(err) {
					t.Skipf("Java file not found: %s", tc.filename)
					return
				}
				data, err := os.ReadFile(path)
				assert.NoError(t, err)

				union, err := NewReservoirItemsUnionFromSlice[float64](data, Float64SerDe{})
				assert.NoError(t, err)
				result, err := union.Result()
				assert.NoError(t, err)
				assert.Equal(t, tc.maxK, result.K())
				assert.Equal(t, tc.n, result.N())
				assert.Equal(t, tc.isEmpty, result.IsEmpty())

				if tc.expected != nil {
					assert.Equal(t, tc.expected, result.Samples())
				} else {
					assert.Equal(t, 0, result.NumSamples())
				}
			})
		}
	})

	t.Run("items_union_string", func(t *testing.T) {
		cases := []struct {
			name     string
			filename string
			maxK     int
			n        int64
			isEmpty  bool
			expected []string
		}{
			{"empty_maxk128", "reservoir_items_union_string_empty_maxk128_java.sk", 128, 0, true, nil},
			{"exact_n1_maxk128", "reservoir_items_union_string_exact_n1_maxk128_java.sk", 128, 1, false, makeRangeString(1)},
			{"exact_n10_maxk128", "reservoir_items_union_string_exact_n10_maxk128_java.sk", 128, 10, false, makeRangeString(10)},
			{"exact_n32_maxk128", "reservoir_items_union_string_exact_n32_maxk128_java.sk", 128, 32, false, makeRangeString(32)},
			{"exact_n100_maxk128", "reservoir_items_union_string_exact_n100_maxk128_java.sk", 128, 100, false, makeRangeString(100)},
			{"exact_n128_maxk128", "reservoir_items_union_string_exact_n128_maxk128_java.sk", 128, 128, false, makeRangeString(128)},
			{"sampling_n1000_maxk32", "reservoir_items_union_string_sampling_n1000_maxk32_java.sk", 32, 1000, false, makeEvenString(32)},
			{"sampling_n1000_maxk64", "reservoir_items_union_string_sampling_n1000_maxk64_java.sk", 64, 1000, false, makeEvenString(64)},
			{"sampling_n1000_maxk128", "reservoir_items_union_string_sampling_n1000_maxk128_java.sk", 128, 1000, false, makeEvenString(128)},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				path := filepath.Join(internal.JavaPath, tc.filename)
				if _, err := os.Stat(path); os.IsNotExist(err) {
					t.Skipf("Java file not found: %s", tc.filename)
					return
				}
				data, err := os.ReadFile(path)
				assert.NoError(t, err)

				union, err := NewReservoirItemsUnionFromSlice[string](data, StringSerDe{})
				assert.NoError(t, err)
				result, err := union.Result()
				assert.NoError(t, err)
				assert.Equal(t, tc.maxK, result.K())
				assert.Equal(t, tc.n, result.N())
				assert.Equal(t, tc.isEmpty, result.IsEmpty())

				if tc.expected != nil {
					assert.Equal(t, tc.expected, result.Samples())
				} else {
					assert.Equal(t, 0, result.NumSamples())
				}
			})
		}
	})
}
