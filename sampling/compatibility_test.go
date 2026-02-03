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

// TestGenerateGoBinariesForCompatibilityTesting generates serialization test data.
// This test is skipped unless DSKETCH_TEST_GENERATE_GO environment variable is set.
// Run with: DSKETCH_TEST_GENERATE_GO=1 go test -v -run TestGenerateGoBinaries
// Generates 27 files for cross-language compatibility testing.
// Note: Go only has generic ReservoirItemsSketch[T], no separate ReservoirLongsSketch.
// See https://github.com/apache/datasketches-go/issues/90 for context.
func TestGenerateGoBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	err := os.MkdirAll(internal.GoPath, os.ModePerm)
	assert.NoError(t, err)

	exactNValues := []int{1, 10, 32, 100, 128}
	samplingKValues := []int{32, 64, 128}

	// ========== ReservoirItemsSketch<Long> (9 files) ==========
	t.Run("items_long", func(t *testing.T) {
		// Empty
		t.Run("empty_k128", func(t *testing.T) {
			sketch, _ := NewReservoirItemsSketch[int64](128)
			data, _ := sketch.ToSlice(Int64SerDe{})
			os.WriteFile(fmt.Sprintf("%s/reservoir_items_long_empty_k128_go.sk", internal.GoPath), data, 0644)
		})
		// Exact
		for _, n := range exactNValues {
			n := n
			t.Run(fmt.Sprintf("exact_n%d_k128", n), func(t *testing.T) {
				sketch, _ := NewReservoirItemsSketch[int64](128)
				for i := int64(0); i < int64(n); i++ {
					sketch.Update(i)
				}
				data, _ := sketch.ToSlice(Int64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_long_exact_n%d_k128_go.sk", internal.GoPath, n), data, 0644)
			})
		}
		// Sampling
		for _, k := range samplingKValues {
			k := k
			t.Run(fmt.Sprintf("sampling_n1000_k%d", k), func(t *testing.T) {
				sketch, _ := NewReservoirItemsSketch[int64](k)
				for i := int64(0); i < 1000; i++ {
					sketch.Update(i)
				}
				data, _ := sketch.ToSlice(Int64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_long_sampling_n1000_k%d_go.sk", internal.GoPath, k), data, 0644)
			})
		}
	})

	// ========== ReservoirItemsSketch<Double> (9 files) ==========
	t.Run("items_double", func(t *testing.T) {
		// Empty
		t.Run("empty_k128", func(t *testing.T) {
			sketch, _ := NewReservoirItemsSketch[float64](128)
			data, _ := sketch.ToSlice(Float64SerDe{})
			os.WriteFile(fmt.Sprintf("%s/reservoir_items_double_empty_k128_go.sk", internal.GoPath), data, 0644)
		})
		// Exact
		for _, n := range exactNValues {
			n := n
			t.Run(fmt.Sprintf("exact_n%d_k128", n), func(t *testing.T) {
				sketch, _ := NewReservoirItemsSketch[float64](128)
				for i := 0; i < n; i++ {
					sketch.Update(float64(i))
				}
				data, _ := sketch.ToSlice(Float64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_double_exact_n%d_k128_go.sk", internal.GoPath, n), data, 0644)
			})
		}
		// Sampling
		for _, k := range samplingKValues {
			k := k
			t.Run(fmt.Sprintf("sampling_n1000_k%d", k), func(t *testing.T) {
				sketch, _ := NewReservoirItemsSketch[float64](k)
				for i := 0; i < 1000; i++ {
					sketch.Update(float64(i))
				}
				data, _ := sketch.ToSlice(Float64SerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_double_sampling_n1000_k%d_go.sk", internal.GoPath, k), data, 0644)
			})
		}
	})

	// ========== ReservoirItemsSketch<String> (9 files) ==========
	t.Run("items_string", func(t *testing.T) {
		// Empty
		t.Run("empty_k128", func(t *testing.T) {
			sketch, _ := NewReservoirItemsSketch[string](128)
			data, _ := sketch.ToSlice(StringSerDe{})
			os.WriteFile(fmt.Sprintf("%s/reservoir_items_string_empty_k128_go.sk", internal.GoPath), data, 0644)
		})
		// Exact
		for _, n := range exactNValues {
			n := n
			t.Run(fmt.Sprintf("exact_n%d_k128", n), func(t *testing.T) {
				sketch, _ := NewReservoirItemsSketch[string](128)
				for i := 0; i < n; i++ {
					sketch.Update(fmt.Sprintf("item%d", i))
				}
				data, _ := sketch.ToSlice(StringSerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_string_exact_n%d_k128_go.sk", internal.GoPath, n), data, 0644)
			})
		}
		// Sampling
		for _, k := range samplingKValues {
			k := k
			t.Run(fmt.Sprintf("sampling_n1000_k%d", k), func(t *testing.T) {
				sketch, _ := NewReservoirItemsSketch[string](k)
				for i := 0; i < 1000; i++ {
					sketch.Update(fmt.Sprintf("item%d", i))
				}
				data, _ := sketch.ToSlice(StringSerDe{})
				os.WriteFile(fmt.Sprintf("%s/reservoir_items_string_sampling_n1000_k%d_go.sk", internal.GoPath, k), data, 0644)
			})
		}
	})

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

// TestSerializationCompatibilityEmpty tests deserialization of an empty sketch.
func TestSerializationCompatibilityEmpty(t *testing.T) {
	filename := filepath.Join(internal.GoPath, "reservoir_items_long_empty_k128_go.sk")
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Skipf("Go file not found: %s", filename)
		return
	}
	data, err := os.ReadFile(filename)
	assert.NoError(t, err)

	sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, 128, sketch.K())
	assert.Equal(t, int64(0), sketch.N())
}

// TestSerializationCompatibilityExact tests deserialization of sketches in exact mode.
func TestSerializationCompatibilityExact(t *testing.T) {
	testCases := []struct {
		filename string
		k        int
		n        int64
	}{
		{"reservoir_items_long_exact_n1_k128_go.sk", 128, 1},
		{"reservoir_items_long_exact_n10_k128_go.sk", 128, 10},
		{"reservoir_items_long_exact_n32_k128_go.sk", 128, 32},
		{"reservoir_items_long_exact_n100_k128_go.sk", 128, 100},
		{"reservoir_items_long_exact_n128_k128_go.sk", 128, 128},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			filename := filepath.Join(internal.GoPath, tc.filename)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Go file not found: %s", filename)
				return
			}
			data, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
			assert.NoError(t, err)
			assert.Equal(t, tc.k, sketch.K())
			assert.Equal(t, tc.n, sketch.N())
			assert.Equal(t, int(tc.n), sketch.NumSamples())
		})
	}
}

// TestSerializationCompatibilityWithSampling tests deserialization of sketches in sampling mode.
func TestSerializationCompatibilityWithSampling(t *testing.T) {
	testCases := []struct {
		filename string
		k        int
		n        int64
	}{
		{"reservoir_items_long_sampling_n1000_k32_go.sk", 32, 1000},
		{"reservoir_items_long_sampling_n1000_k64_go.sk", 64, 1000},
		{"reservoir_items_long_sampling_n1000_k128_go.sk", 128, 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			filename := filepath.Join(internal.GoPath, tc.filename)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Go file not found: %s", filename)
				return
			}
			data, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
			assert.NoError(t, err)
			assert.Equal(t, tc.k, sketch.K())
			assert.Equal(t, tc.n, sketch.N())
			assert.Equal(t, tc.k, sketch.NumSamples()) // Only k items kept after sampling
		})
	}
}

// TestSerializationRoundTrip tests serialization and deserialization round-trip.
func TestSerializationRoundTrip(t *testing.T) {
	// Create sketch and add items
	sketch, _ := NewReservoirItemsSketch[int64](10)
	for i := int64(1); i <= 5; i++ {
		sketch.Update(i)
	}

	// Serialize
	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	// Verify preamble structure (Java-compatible format)
	// Byte 0: 0xC0 (ResizeFactor X8) | 0x02 (preamble_longs) = 0xC2
	assert.Equal(t, byte(0xC2), data[0])                                  // preamble_longs = 2 for non-empty + ResizeFactor bits
	assert.Equal(t, byte(2), data[1])                                     // serVer = 2
	assert.Equal(t, byte(internal.FamilyEnum.ReservoirItems.Id), data[2]) // familyID

	// Deserialize and verify
	restored, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sketch.K(), restored.K())
	assert.Equal(t, sketch.N(), restored.N())
	assert.Equal(t, sketch.Samples(), restored.Samples())
}

// TestReservoirItemsSketch_JavaCompat tests deserialization of Java-generated reservoir sketch files.
// These tests verify cross-language compatibility with files generated by datasketches-java.
func TestReservoirItemsSketch_JavaCompat(t *testing.T) {
	// Test cases based on Java PR #714: ReservoirCrossLanguageTest.java (36 total files)
	testCases := []struct {
		name     string
		filename string
		k        int
		n        int64
		isEmpty  bool
	}{
		// ReservoirItemsSketch<Long> - Empty (1)
		{"items_long_empty_k128", "reservoir_items_long_empty_k128_java.sk", 128, 0, true},

		// ReservoirItemsSketch<Long> - Exact (5)
		{"items_long_exact_n1_k128", "reservoir_items_long_exact_n1_k128_java.sk", 128, 1, false},
		{"items_long_exact_n10_k128", "reservoir_items_long_exact_n10_k128_java.sk", 128, 10, false},
		{"items_long_exact_n32_k128", "reservoir_items_long_exact_n32_k128_java.sk", 128, 32, false},
		{"items_long_exact_n100_k128", "reservoir_items_long_exact_n100_k128_java.sk", 128, 100, false},
		{"items_long_exact_n128_k128", "reservoir_items_long_exact_n128_k128_java.sk", 128, 128, false},

		// ReservoirItemsSketch<Long> - Sampling (3)
		{"items_long_sampling_n1000_k32", "reservoir_items_long_sampling_n1000_k32_java.sk", 32, 1000, false},
		{"items_long_sampling_n1000_k64", "reservoir_items_long_sampling_n1000_k64_java.sk", 64, 1000, false},
		{"items_long_sampling_n1000_k128", "reservoir_items_long_sampling_n1000_k128_java.sk", 128, 1000, false},

		// ReservoirItemsSketch<Double> - Empty (1)
		{"items_double_empty_k128", "reservoir_items_double_empty_k128_java.sk", 128, 0, true},

		// ReservoirItemsSketch<Double> - Exact (5)
		{"items_double_exact_n1_k128", "reservoir_items_double_exact_n1_k128_java.sk", 128, 1, false},
		{"items_double_exact_n10_k128", "reservoir_items_double_exact_n10_k128_java.sk", 128, 10, false},
		{"items_double_exact_n32_k128", "reservoir_items_double_exact_n32_k128_java.sk", 128, 32, false},
		{"items_double_exact_n100_k128", "reservoir_items_double_exact_n100_k128_java.sk", 128, 100, false},
		{"items_double_exact_n128_k128", "reservoir_items_double_exact_n128_k128_java.sk", 128, 128, false},

		// ReservoirItemsSketch<Double> - Sampling (3)
		{"items_double_sampling_n1000_k32", "reservoir_items_double_sampling_n1000_k32_java.sk", 32, 1000, false},
		{"items_double_sampling_n1000_k64", "reservoir_items_double_sampling_n1000_k64_java.sk", 64, 1000, false},
		{"items_double_sampling_n1000_k128", "reservoir_items_double_sampling_n1000_k128_java.sk", 128, 1000, false},

		// ReservoirItemsSketch<String> - Empty (1)
		{"items_string_empty_k128", "reservoir_items_string_empty_k128_java.sk", 128, 0, true},

		// ReservoirItemsSketch<String> - Exact (5)
		{"items_string_exact_n1_k128", "reservoir_items_string_exact_n1_k128_java.sk", 128, 1, false},
		{"items_string_exact_n10_k128", "reservoir_items_string_exact_n10_k128_java.sk", 128, 10, false},
		{"items_string_exact_n32_k128", "reservoir_items_string_exact_n32_k128_java.sk", 128, 32, false},
		{"items_string_exact_n100_k128", "reservoir_items_string_exact_n100_k128_java.sk", 128, 100, false},
		{"items_string_exact_n128_k128", "reservoir_items_string_exact_n128_k128_java.sk", 128, 128, false},

		// ReservoirItemsSketch<String> - Sampling (3)
		{"items_string_sampling_n1000_k32", "reservoir_items_string_sampling_n1000_k32_java.sk", 32, 1000, false},
		{"items_string_sampling_n1000_k64", "reservoir_items_string_sampling_n1000_k64_java.sk", 64, 1000, false},
		{"items_string_sampling_n1000_k128", "reservoir_items_string_sampling_n1000_k128_java.sk", 128, 1000, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filepath := filepath.Join(internal.JavaPath, tc.filename)

			// Skip if Java file not yet available
			if _, err := os.Stat(filepath); os.IsNotExist(err) {
				t.Skipf("Java file not found: %s (waiting for sync from datasketches-java)", tc.filename)
				return
			}

			data, err := os.ReadFile(filepath)
			assert.NoError(t, err)

			sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
			assert.NoError(t, err)

			assert.Equal(t, tc.k, sketch.K(), "k mismatch")
			assert.Equal(t, tc.n, sketch.N(), "n mismatch")
			assert.Equal(t, tc.isEmpty, sketch.IsEmpty(), "isEmpty mismatch")

			if !tc.isEmpty {
				samples := sketch.Samples()
				if tc.n <= int64(tc.k) {
					// Exact mode: should have exactly n samples
					assert.Equal(t, int(tc.n), len(samples), "sample count mismatch in exact mode")
				} else {
					// Sampling mode: should have exactly k samples
					assert.Equal(t, tc.k, len(samples), "sample count mismatch in sampling mode")
				}
			}
		})
	}
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
