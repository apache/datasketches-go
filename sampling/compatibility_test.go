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
func TestGenerateGoBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	err := os.MkdirAll(internal.GoPath, os.ModePerm)
	assert.NoError(t, err)

	t.Run("reservoir empty", func(t *testing.T) {
		k := 10
		sketch, err := NewReservoirItemsSketch[int64](k)
		assert.NoError(t, err)

		data, err := sketch.ToSlice(Int64SerDe{})
		assert.NoError(t, err)

		filename := fmt.Sprintf("%s/reservoir_long_n0_k%d_go.sk", internal.GoPath, k)
		err = os.WriteFile(filename, data, 0644)
		assert.NoError(t, err)
		t.Logf("Generated: %s (%d bytes)", filename, len(data))
	})

	t.Run("reservoir below k", func(t *testing.T) {
		k, n := 100, 10
		sketch, err := NewReservoirItemsSketch[int64](k)
		assert.NoError(t, err)

		for i := int64(1); i <= int64(n); i++ {
			sketch.Update(i)
		}

		data, err := sketch.ToSlice(Int64SerDe{})
		assert.NoError(t, err)

		filename := fmt.Sprintf("%s/reservoir_long_n%d_k%d_go.sk", internal.GoPath, n, k)
		err = os.WriteFile(filename, data, 0644)
		assert.NoError(t, err)
		t.Logf("Generated: %s (%d bytes)", filename, len(data))
	})

	t.Run("reservoir at k", func(t *testing.T) {
		k, n := 10, 10
		sketch, err := NewReservoirItemsSketch[int64](k)
		assert.NoError(t, err)

		for i := int64(1); i <= int64(n); i++ {
			sketch.Update(i)
		}

		data, err := sketch.ToSlice(Int64SerDe{})
		assert.NoError(t, err)

		filename := fmt.Sprintf("%s/reservoir_long_n%d_k%d_go.sk", internal.GoPath, n, k)
		err = os.WriteFile(filename, data, 0644)
		assert.NoError(t, err)
		t.Logf("Generated: %s (%d bytes)", filename, len(data))
	})

	t.Run("reservoir with sampling", func(t *testing.T) {
		k, n := 10, 100
		sketch, err := NewReservoirItemsSketch[int64](k)
		assert.NoError(t, err)

		for i := int64(1); i <= int64(n); i++ {
			sketch.Update(i)
		}

		data, err := sketch.ToSlice(Int64SerDe{})
		assert.NoError(t, err)

		filename := fmt.Sprintf("%s/reservoir_long_n%d_k%d_go.sk", internal.GoPath, n, k)
		err = os.WriteFile(filename, data, 0644)
		assert.NoError(t, err)
		t.Logf("Generated: %s (%d bytes)", filename, len(data))
	})
}

// TestSerializationCompatibilityEmpty tests deserialization of an empty sketch.
func TestSerializationCompatibilityEmpty(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(internal.GoPath, "reservoir_long_n0_k10_go.sk"))
	assert.NoError(t, err)

	sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, 10, sketch.K())
	assert.Equal(t, int64(0), sketch.N())
}

// TestSerializationCompatibilityBelowK tests deserialization of a sketch with items below k.
func TestSerializationCompatibilityBelowK(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(internal.GoPath, "reservoir_long_n10_k100_go.sk"))
	assert.NoError(t, err)

	sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, 100, sketch.K())
	assert.Equal(t, int64(10), sketch.N())
	assert.Equal(t, 10, sketch.NumSamples())
}

// TestSerializationCompatibilityAtK tests deserialization of a sketch at capacity.
func TestSerializationCompatibilityAtK(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(internal.GoPath, "reservoir_long_n10_k10_go.sk"))
	assert.NoError(t, err)

	sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, 10, sketch.K())
	assert.Equal(t, int64(10), sketch.N())
	assert.Equal(t, 10, sketch.NumSamples())
}

// TestSerializationCompatibilityWithSampling tests deserialization of a sketch with sampling.
func TestSerializationCompatibilityWithSampling(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(internal.GoPath, "reservoir_long_n100_k10_go.sk"))
	assert.NoError(t, err)

	sketch, err := NewReservoirItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, 10, sketch.K())
	assert.Equal(t, int64(100), sketch.N())
	assert.Equal(t, 10, sketch.NumSamples()) // Only k items kept after sampling
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

	// Verify preamble structure
	assert.Equal(t, byte(3), data[0])                                     // preamble_longs = 3 for non-empty
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
		// ReservoirLongsSketch - Empty (1)
		{"longs_empty_k128", "reservoir_longs_empty_k128_java.sk", 128, 0, true},

		// ReservoirLongsSketch - Exact (5)
		{"longs_exact_n1_k128", "reservoir_longs_exact_n1_k128_java.sk", 128, 1, false},
		{"longs_exact_n10_k128", "reservoir_longs_exact_n10_k128_java.sk", 128, 10, false},
		{"longs_exact_n32_k128", "reservoir_longs_exact_n32_k128_java.sk", 128, 32, false},
		{"longs_exact_n100_k128", "reservoir_longs_exact_n100_k128_java.sk", 128, 100, false},
		{"longs_exact_n128_k128", "reservoir_longs_exact_n128_k128_java.sk", 128, 128, false},

		// ReservoirLongsSketch - Sampling (3)
		{"longs_sampling_n1000_k32", "reservoir_longs_sampling_n1000_k32_java.sk", 32, 1000, false},
		{"longs_sampling_n1000_k64", "reservoir_longs_sampling_n1000_k64_java.sk", 64, 1000, false},
		{"longs_sampling_n1000_k128", "reservoir_longs_sampling_n1000_k128_java.sk", 128, 1000, false},

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
