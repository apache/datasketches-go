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

		data, err := sketch.ToByteArray(Int64SerDe{})
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

		data, err := sketch.ToByteArray(Int64SerDe{})
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

		data, err := sketch.ToByteArray(Int64SerDe{})
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

		data, err := sketch.ToByteArray(Int64SerDe{})
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
	data, err := sketch.ToByteArray(Int64SerDe{})
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
