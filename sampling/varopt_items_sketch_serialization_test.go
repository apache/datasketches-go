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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const varOptItemsSerializationEpsilon = 1e-13

func TestGenerateGoBinariesForCompatibilityTestingVarOptItemsSketch(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	err := os.MkdirAll(internal.GoPath, os.ModePerm)
	require.NoError(t, err)

	t.Run("long generate", func(t *testing.T) {
		for _, n := range []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000} {
			n := n
			t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
				sketch, err := NewVarOptItemsSketch[int64](32)
				require.NoError(t, err)

				for i := 1; i <= n; i++ {
					require.NoError(t, sketch.Update(int64(i), 1.0))
				}

				data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
				filename := filepath.Join(internal.GoPath, fmt.Sprintf("varopt_sketch_long_n%d_go.sk", n))
				require.NoError(t, os.WriteFile(filename, data, 0644))
			})
		}
	})

	t.Run("string exact", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[string](1024)
		require.NoError(t, err)

		for i := 1; i <= 200; i++ {
			require.NoError(t, sketch.Update(strconv.Itoa(i), 1000.0/float64(i)))
		}

		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchStringSerDe{})
		filename := filepath.Join(internal.GoPath, "varopt_sketch_string_exact_go.sk")
		require.NoError(t, os.WriteFile(filename, data, 0644))
	})

	t.Run("long sampling", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[int64](1024)
		require.NoError(t, err)

		for i := 0; i < 2000; i++ {
			require.NoError(t, sketch.Update(int64(i), 1.0))
		}
		require.NoError(t, sketch.Update(-1, 100000.0))
		require.NoError(t, sketch.Update(-2, 110000.0))
		require.NoError(t, sketch.Update(-3, 120000.0))

		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
		filename := filepath.Join(internal.GoPath, "varopt_sketch_long_sampling_go.sk")
		require.NoError(t, os.WriteFile(filename, data, 0644))
	})
}

func TestVarOptItemsSketchJavaCompat(t *testing.T) {
	t.Run("long", func(t *testing.T) {
		for _, n := range []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000} {
			t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
				filename := filepath.Join(internal.JavaPath, fmt.Sprintf("varopt_sketch_long_n%d_java.sk", n))
				data, err := os.ReadFile(filename)
				if os.IsNotExist(err) {
					t.Skipf("Java file not found: %s", filename)
				}
				require.NoError(t, err)

				sketch, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
				require.NoError(t, err)
				assert.Equal(t, n == 0, sketch.IsEmpty())
				assert.Equal(t, 32, sketch.K())
				assert.Equal(t, int64(n), sketch.N())
				if n > 10 {
					assert.Equal(t, 32, sketch.NumSamples())
				} else {
					assert.Equal(t, n, sketch.NumSamples())
				}

				summary, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
				require.NoError(t, err)
				assert.InDelta(t, float64(n), summary.Estimate, varOptItemsSerializationEpsilon)
				assert.InDelta(t, float64(n), summary.TotalSketchWeight, varOptItemsSerializationEpsilon)
			})
		}
	})

	t.Run("string exact", func(t *testing.T) {
		filename := filepath.Join(internal.JavaPath, "varopt_sketch_string_exact_java.sk")
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
		}
		require.NoError(t, err)

		sketch, err := DecodeVarOptItemsSketch[string](data, common.ItemSketchStringSerDe{})
		require.NoError(t, err)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, 1024, sketch.K())
		assert.Equal(t, int64(200), sketch.N())
		assert.Equal(t, 200, sketch.NumSamples())

		expectedWeight := 0.0
		for i := 1; i <= 200; i++ {
			expectedWeight += 1000.0 / float64(i)
		}

		summary, err := sketch.EstimateSubsetSum(func(string) bool { return true })
		require.NoError(t, err)
		assert.InDelta(t, expectedWeight, summary.Estimate, varOptItemsSerializationEpsilon)
		assert.InDelta(t, expectedWeight, summary.TotalSketchWeight, varOptItemsSerializationEpsilon)
	})

	t.Run("long sampling", func(t *testing.T) {
		filename := filepath.Join(internal.JavaPath, "varopt_sketch_long_sampling_java.sk")
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
		}
		require.NoError(t, err)

		sketch, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
		require.NoError(t, err)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, 1024, sketch.K())
		assert.Equal(t, int64(2003), sketch.N())
		assert.Equal(t, sketch.K(), sketch.NumSamples())

		summary, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
		require.NoError(t, err)
		assert.InDelta(t, 332000.0, summary.Estimate, varOptItemsSerializationEpsilon)
		assert.InDelta(t, 332000.0, summary.TotalSketchWeight, varOptItemsSerializationEpsilon)

		summary, err = sketch.EstimateSubsetSum(func(x int64) bool { return x < 0 })
		require.NoError(t, err)
		assert.InDelta(t, 330000.0, summary.Estimate, varOptItemsSerializationEpsilon)

		summary, err = sketch.EstimateSubsetSum(func(x int64) bool { return x >= 0 })
		require.NoError(t, err)
		assert.InDelta(t, 2000.0, summary.Estimate, varOptItemsSerializationEpsilon)
	})
}

func TestVarOptItemsSketchCppCompat(t *testing.T) {
	t.Run("long", func(t *testing.T) {
		for _, n := range []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000} {
			n := n
			t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
				filename := filepath.Join(internal.CppPath, fmt.Sprintf("varopt_sketch_long_n%d_cpp.sk", n))
				data, err := os.ReadFile(filename)
				if os.IsNotExist(err) {
					t.Skipf("C++ file not found: %s", filename)
				}
				require.NoError(t, err)

				sketch, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
				require.NoError(t, err)
				assert.Equal(t, n == 0, sketch.IsEmpty())
				assert.Equal(t, 32, sketch.K())
				assert.Equal(t, int64(n), sketch.N())
				if n > 10 {
					assert.Equal(t, 32, sketch.NumSamples())
				} else {
					assert.Equal(t, n, sketch.NumSamples())
				}

				summary, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
				require.NoError(t, err)
				assert.InDelta(t, float64(n), summary.Estimate, varOptItemsSerializationEpsilon)
				assert.InDelta(t, float64(n), summary.TotalSketchWeight, varOptItemsSerializationEpsilon)
			})
		}
	})

	t.Run("string exact", func(t *testing.T) {
		filename := filepath.Join(internal.CppPath, "varopt_sketch_string_exact_cpp.sk")
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			t.Skipf("C++ file not found: %s", filename)
		}
		require.NoError(t, err)

		sketch, err := DecodeVarOptItemsSketch[string](data, common.ItemSketchStringSerDe{})
		require.NoError(t, err)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, 1024, sketch.K())
		assert.Equal(t, int64(200), sketch.N())
		assert.Equal(t, 200, sketch.NumSamples())

		expectedWeight := 0.0
		for i := 1; i <= 200; i++ {
			expectedWeight += 1000.0 / float64(i)
		}

		summary, err := sketch.EstimateSubsetSum(func(string) bool { return true })
		require.NoError(t, err)
		assert.InDelta(t, expectedWeight, summary.Estimate, varOptItemsSerializationEpsilon)
		assert.InDelta(t, expectedWeight, summary.TotalSketchWeight, varOptItemsSerializationEpsilon)
	})

	t.Run("long sampling", func(t *testing.T) {
		filename := filepath.Join(internal.CppPath, "varopt_sketch_long_sampling_cpp.sk")
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			t.Skipf("C++ file not found: %s", filename)
		}
		require.NoError(t, err)

		sketch, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
		require.NoError(t, err)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, 1024, sketch.K())
		assert.Equal(t, int64(2003), sketch.N())
		assert.Equal(t, sketch.K(), sketch.NumSamples())

		summary, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
		require.NoError(t, err)
		assert.InDelta(t, 332000.0, summary.Estimate, varOptItemsSerializationEpsilon)
		assert.InDelta(t, 332000.0, summary.TotalSketchWeight, varOptItemsSerializationEpsilon)

		summary, err = sketch.EstimateSubsetSum(func(x int64) bool { return x < 0 })
		require.NoError(t, err)
		assert.InDelta(t, 330000.0, summary.Estimate, varOptItemsSerializationEpsilon)

		summary, err = sketch.EstimateSubsetSum(func(x int64) bool { return x >= 0 })
		require.NoError(t, err)
		assert.InDelta(t, 2000.0, summary.Estimate, varOptItemsSerializationEpsilon)
	})
}

func TestVarOptItemsSketchSerialization(t *testing.T) {
	t.Run("nil sketch encode", func(t *testing.T) {
		var buf bytes.Buffer
		encoder := NewVarOptItemsSketchEncoder[int64](&buf, common.ItemSketchLongSerDe{})

		err := encoder.Encode(nil)
		require.ErrorContains(t, err, "cannot encode nil VarOptItemsSketch")
	})

	t.Run("bad serialization version", func(t *testing.T) {
		sketch := createUnweightedVarOptItemsSketch(t, 16, 16)
		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
		data[1] = 0

		_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
		require.ErrorContains(t, err, "invalid serialization version: expected 2, got 0")
	})

	t.Run("bad family", func(t *testing.T) {
		sketch := createUnweightedVarOptItemsSketch(t, 16, 16)
		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
		data[2] = 0

		_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
		require.ErrorContains(t, err, "invalid family ID: expected 13, got 0")
	})

	t.Run("bad prelongs", func(t *testing.T) {
		for _, preLongs := range []byte{0, 2, 5} {
			preLongs := preLongs
			t.Run(string(rune('0'+preLongs)), func(t *testing.T) {
				sketch := createUnweightedVarOptItemsSketch(t, 32, 33)
				data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
				data[0] = preLongs

				_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
				require.ErrorContains(t, err, fmt.Sprintf("invalid preamble longs: expected warmup or full, got %d", preLongs))
			})
		}
	})

	t.Run("malformed preamble", func(t *testing.T) {
		source := encodeVarOptItemsSketch(t, createUnweightedVarOptItemsSketch(t, 50, 50), common.ItemSketchLongSerDe{})

		t.Run("full preamble without R", func(t *testing.T) {
			data := cloneBytes(source)
			data[0] = preambleLongsFull

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "invalid preamble longs: expected warmup because n<=k, got 4")
		})

		t.Run("zero k", func(t *testing.T) {
			data := cloneBytes(source)
			binary.LittleEndian.PutUint32(data[4:], 0)

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "k must be at least 1 and less than 2^31 - 1")
		})

		t.Run("negative H count", func(t *testing.T) {
			data := cloneBytes(source)
			binary.LittleEndian.PutUint32(data[16:], math.MaxUint32)

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "invalid state in warmup mode: expected n==h, got n=50, h=4294967295")
		})

		t.Run("negative R count", func(t *testing.T) {
			data := cloneBytes(source)
			binary.LittleEndian.PutUint32(data[20:], uint32(0xffffff80))

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "invalid state in warmup mode: expected r==0, got r=4294967168")
		})

		t.Run("warmup preamble in full mode", func(t *testing.T) {
			data := encodeVarOptItemsSketch(t, createUnweightedVarOptItemsSketch(t, 32, 33), common.ItemSketchLongSerDe{})
			data[0] = (data[0] & 0xc0) | preambleLongsWarmup

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "invalid preamble longs: expected full because n>k, got 3")
		})
	})

	t.Run("empty sketch", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[string](5)
		require.NoError(t, err)

		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchStringSerDe{})
		require.Len(t, data, int(preambleLongsEmpty<<3))

		loaded, err := DecodeVarOptItemsSketch[string](data, common.ItemSketchStringSerDe{})
		require.NoError(t, err)
		assert.Equal(t, int64(0), loaded.N())
		assert.Equal(t, 0, loaded.NumSamples())
		assert.True(t, loaded.IsEmpty())
	})

	t.Run("non-empty degenerate sketch", func(t *testing.T) {
		sketch, err := NewVarOptItemsSketch[string](12, WithResizeFactor(ResizeX2))
		require.NoError(t, err)

		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchStringSerDe{})
		for len(data) < int(preambleLongsWarmup<<3) {
			data = append(data, 0)
		}
		data[3] = 0

		_, err = DecodeVarOptItemsSketch[string](data, common.ItemSketchStringSerDe{})
		require.ErrorContains(t, err, "invalid preamble longs: expected warmup or full, got 1")
	})

	t.Run("invalid full mode H plus R count", func(t *testing.T) {
		data := encodeVarOptItemsSketch(t, createUnweightedVarOptItemsSketch(t, 32, 33), common.ItemSketchLongSerDe{})
		binary.LittleEndian.PutUint32(data[20:], 0)

		_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
		require.ErrorContains(t, err, "invalid state in full mode: expected h+r==k")
	})

	t.Run("corrupt serialized R weight", func(t *testing.T) {
		t.Run("zero", func(t *testing.T) {
			data := encodeVarOptItemsSketch(t, createUnweightedVarOptItemsSketch(t, 32, 33), common.ItemSketchLongSerDe{})
			binary.LittleEndian.PutUint64(data[24:], math.Float64bits(0))

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "data is corrupt in full mode: invalid R region weight")
		})

		t.Run("negative", func(t *testing.T) {
			data := encodeVarOptItemsSketch(t, createUnweightedVarOptItemsSketch(t, 32, 33), common.ItemSketchLongSerDe{})
			binary.LittleEndian.PutUint64(data[24:], math.Float64bits(-1.5))

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "data is corrupt in full mode: invalid R region weight")
		})

		t.Run("nan", func(t *testing.T) {
			data := encodeVarOptItemsSketch(t, createUnweightedVarOptItemsSketch(t, 32, 33), common.ItemSketchLongSerDe{})
			binary.LittleEndian.PutUint64(data[24:], math.Float64bits(math.NaN()))

			_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "data is corrupt in full mode: invalid R region weight")
		})
	})

	t.Run("corrupt serialized H weight", func(t *testing.T) {
		sketch := createUnweightedVarOptItemsSketch(t, 100, 20)
		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
		preambleBytes := int(data[0]&0x3f) << 3
		binary.LittleEndian.PutUint64(data[preambleBytes:], math.Float64bits(-1.5))

		_, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
		require.ErrorContains(t, err, "non-positive weight: -1.500000")
	})

	t.Run("round trip", func(t *testing.T) {
		t.Run("under-full sketch", func(t *testing.T) {
			sketch := createUnweightedVarOptItemsSketch(t, 100, 10)
			data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})

			loaded, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.NoError(t, err)
			assertVarOptItemsSketchEqual(t, sketch, loaded)

			_, err = DecodeVarOptItemsSketch[int64](data[:len(data)-1], common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "unexpected EOF")
		})

		t.Run("end-of-warmup sketch", func(t *testing.T) {
			sketch := createUnweightedVarOptItemsSketch(t, 2843, 2843)
			data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
			require.Equal(t, preambleLongsWarmup, data[0]&0x3f)

			loaded, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.NoError(t, err)
			assertVarOptItemsSketchEqual(t, sketch, loaded)

			_, err = DecodeVarOptItemsSketch[int64](data[:len(data)-1000], common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "unexpected EOF")
		})

		t.Run("full sketch", func(t *testing.T) {
			sketch := createUnweightedVarOptItemsSketch(t, 32, 32)
			require.NoError(t, sketch.Update(100, 100.0))
			require.NoError(t, sketch.Update(101, 101.0))

			totalWeight, err := sketch.EstimateSubsetSum(func(int64) bool { return true })
			require.NoError(t, err)
			cumulativeWeight := 0.0
			for sample := range sketch.All() {
				cumulativeWeight += sample.Weight
			}
			require.InDelta(t, 1.0, cumulativeWeight/totalWeight.TotalSketchWeight, varOptItemsSerializationEpsilon)

			samples := collectVarOptItemsSamples(sketch)
			require.GreaterOrEqual(t, len(samples), 2)
			require.InDelta(t, 100.0, samples[0].Weight, varOptItemsSerializationEpsilon)
			require.InDelta(t, 101.0, samples[1].Weight, varOptItemsSerializationEpsilon)
			require.Equal(t, int64(100), samples[0].Item)
			require.Equal(t, int64(101), samples[1].Item)

			data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchLongSerDe{})
			require.Equal(t, preambleLongsFull, data[0]&0x3f)

			loaded, err := DecodeVarOptItemsSketch[int64](data, common.ItemSketchLongSerDe{})
			require.NoError(t, err)
			assertVarOptItemsSketchEqual(t, sketch, loaded)

			_, err = DecodeVarOptItemsSketch[int64](data[:len(data)-100], common.ItemSketchLongSerDe{})
			require.ErrorContains(t, err, "unexpected EOF")
		})

		t.Run("string sketch", func(t *testing.T) {
			sketch, err := NewVarOptItemsSketch[string](5)
			require.NoError(t, err)
			for _, item := range []string{"a", "bc", "def", "ghij", "klmno"} {
				require.NoError(t, sketch.Update(item, 1.0))
			}
			require.NoError(t, sketch.Update("heavy item", 100.0))

			data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchStringSerDe{})
			loaded, err := DecodeVarOptItemsSketch[string](data, common.ItemSketchStringSerDe{})
			require.NoError(t, err)
			assertVarOptItemsSketchEqual(t, sketch, loaded)

			_, err = DecodeVarOptItemsSketch[string](data[:len(data)-12], common.ItemSketchStringSerDe{})
			require.ErrorContains(t, err, "offset out of bounds")
		})
	})
}

func createUnweightedVarOptItemsSketch(t *testing.T, k int, n int) *VarOptItemsSketch[int64] {
	t.Helper()

	sketch, err := NewVarOptItemsSketch[int64](uint(k))
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, sketch.Update(int64(i), 1.0))
	}
	return sketch
}

func encodeVarOptItemsSketch[T any](t *testing.T, sketch *VarOptItemsSketch[T], serde common.ItemSketchSerde[T]) []byte {
	t.Helper()

	var buf bytes.Buffer
	encoder := NewVarOptItemsSketchEncoder(&buf, serde)
	require.NoError(t, encoder.Encode(sketch))
	return buf.Bytes()
}

func assertVarOptItemsSketchEqual[T comparable](t *testing.T, expected *VarOptItemsSketch[T], actual *VarOptItemsSketch[T]) {
	t.Helper()

	require.Equal(t, expected.K(), actual.K())
	require.Equal(t, expected.N(), actual.N())
	require.Equal(t, expected.NumSamples(), actual.NumSamples())
	require.Equal(t, expected.H(), actual.H())
	require.Equal(t, expected.R(), actual.R())

	expectedSamples := collectVarOptItemsSamples(expected)
	actualSamples := collectVarOptItemsSamples(actual)
	require.Len(t, actualSamples, len(expectedSamples))
	for i := range expectedSamples {
		require.Equal(t, expectedSamples[i].Item, actualSamples[i].Item)
		require.InDelta(t, expectedSamples[i].Weight, actualSamples[i].Weight, varOptItemsSerializationEpsilon)
	}
}

func collectVarOptItemsSamples[T any](sketch *VarOptItemsSketch[T]) []Sample[T] {
	samples := make([]Sample[T], 0, sketch.NumSamples())
	for sample := range sketch.All() {
		samples = append(samples, sample)
	}
	return samples
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
