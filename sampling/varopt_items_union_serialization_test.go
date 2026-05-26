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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

func TestGenerateGoBinariesForCompatibilityTestingVarOptItemsUnion(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	err := os.MkdirAll(internal.GoPath, os.ModePerm)
	require.NoError(t, err)

	t.Run("double sampling", func(t *testing.T) {
		const (
			kSmall = 16
			n1     = 32
			n2     = 64
			kMax   = 128
		)

		sketch, err := NewVarOptItemsSketch[float64](kSmall)
		require.NoError(t, err)

		// small k sketch, but sampling.
		for i := 0; i < n1; i++ {
			require.NoError(t, sketch.Update(float64(i), 1.0))
		}
		require.NoError(t, sketch.Update(-1, n1*n1)) // negative heavy item to allow a simple predicate to filter.

		union, err := NewVarOptItemsUnion[float64](kMax)
		require.NoError(t, err)

		// another one, but different n to get a different per-item weight.
		sketch, err = NewVarOptItemsSketch[float64](kSmall)
		require.NoError(t, err)
		for i := 0; i < n2; i++ {
			require.NoError(t, sketch.Update(float64(i), 1.0))
		}
		require.NoError(t, union.Update(sketch))

		data := encodeVarOptItemsSketch(t, sketch, common.ItemSketchDoubleSerDe{})
		filename := filepath.Join(internal.GoPath, "varopt_union_double_sampling_go.sk")
		require.NoError(t, os.WriteFile(filename, data, 0644))
	})
}

func TestVarOptItemsUnionJavaCompat(t *testing.T) {
	t.Run("double sampling", func(t *testing.T) {
		filename := filepath.Join(internal.JavaPath, "varopt_union_double_sampling_java.sk")
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
		}
		require.NoError(t, err)

		union, err := DecodeVarOptItemsUnion[float64](data, common.ItemSketchDoubleSerDe{})
		require.NoError(t, err)

		// must reduce k in the process.
		sketch, err := union.Result()
		require.NoError(t, err)
		assert.Less(t, sketch.K(), 128)
		assert.Equal(t, int64(97), sketch.N())

		// light items, ignoring the heavy one.
		summary, err := sketch.EstimateSubsetSum(func(item float64) bool {
			return item >= 0
		})
		require.NoError(t, err)
		assert.InDelta(t, 96.0, summary.Estimate, varOptItemsSerializationEpsilon)
		assert.InDelta(t, 96.0+1024.0, summary.TotalSketchWeight, varOptItemsSerializationEpsilon)
	})
}

func TestVarOptItemsUnionCppCompat(t *testing.T) {
	t.Run("double sampling", func(t *testing.T) {
		filename := filepath.Join(internal.CppPath, "varopt_union_double_sampling_cpp.sk")
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			t.Skipf("C++ file not found: %s", filename)
		}
		require.NoError(t, err)

		union, err := DecodeVarOptItemsUnion[float64](data, common.ItemSketchDoubleSerDe{})
		require.NoError(t, err)

		// must reduce k in the process.
		sketch, err := union.Result()
		require.NoError(t, err)
		assert.Less(t, sketch.K(), 128)
		assert.Equal(t, int64(97), sketch.N())

		// light items, ignoring the heavy one.
		summary, err := sketch.EstimateSubsetSum(func(item float64) bool {
			return item >= 0
		})
		require.NoError(t, err)
		assert.InDelta(t, 96.0, summary.Estimate, varOptItemsSerializationEpsilon)
		assert.InDelta(t, 96.0+1024.0, summary.TotalSketchWeight, varOptItemsSerializationEpsilon)
	})
}

func TestVarOptItemsUnionSerialization(t *testing.T) {
	t.Run("empty union", func(t *testing.T) {
		const k = 100

		union, err := NewVarOptItemsUnion[string](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(nil))

		serde := common.ItemSketchStringSerDe{}
		data := encodeVarOptItemsUnion(t, union, serde)
		require.Len(t, data, 8)

		rebuilt, err := DecodeVarOptItemsUnion[string](data, serde)
		require.NoError(t, err)

		sketch, err := rebuilt.Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), sketch.N())

		assert.Equal(t, union.String(), rebuilt.String())
	})

	t.Run("union empty sketch", func(t *testing.T) {
		const k = 2048

		serde := common.ItemSketchStringSerDe{}
		sketch, err := NewVarOptItemsSketch[string](k)
		require.NoError(t, err)

		data := encodeVarOptItemsSketch(t, sketch, serde)
		decoded, err := DecodeVarOptItemsSketch[string](data, serde)
		require.NoError(t, err)

		union, err := NewVarOptItemsUnion[string](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(decoded))

		result, err := union.Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), result.N())
		assert.Equal(t, 0, result.H())
		assert.Equal(t, 0, result.R())
	})

	t.Run("exact union", func(t *testing.T) {
		const (
			k  = 128
			n1 = 32
			n2 = 64
		)

		sketch1 := newUnweightedLongsVarOptItemsSketch(t, k, n1)
		sketch2 := newUnweightedLongsVarOptItemsSketch(t, k, n2)

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch1))
		require.NoError(t, union.Update(sketch2))

		serde := common.ItemSketchLongSerDe{}
		data := encodeVarOptItemsUnion(t, union, serde)

		rebuilt, err := DecodeVarOptItemsUnion[int64](data, serde)
		require.NoError(t, err)

		compareVarOptItemsUnionsExact(t, rebuilt, union)
	})

	t.Run("sampling union", func(t *testing.T) {
		const (
			k = 128
			n = 256
		)

		sketch := newUnweightedLongsVarOptItemsSketch(t, k, n)
		for i := 1; i <= 8; i++ {
			require.NoError(t, sketch.Update(int64(n+i), 1000.0+float64(i-1)))
		}

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(sketch))

		serde := common.ItemSketchLongSerDe{}
		data := encodeVarOptItemsUnion(t, union, serde)

		rebuilt, err := DecodeVarOptItemsUnion[int64](data, serde)
		require.NoError(t, err)

		compareVarOptItemsUnionsExact(t, rebuilt, union)
	})

	t.Run("bad ser ver", func(t *testing.T) {
		const (
			k = 25
			n = 30
		)

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(newUnweightedLongsVarOptItemsSketch(t, k, n)))

		serde := common.ItemSketchLongSerDe{}
		data := encodeVarOptItemsUnion(t, union, serde)

		data[1] = 0 // corrupt the serialization version byte

		_, err = DecodeVarOptItemsUnion[int64](data, serde)
		require.ErrorContains(t, err, "invalid serial version")
	})

	t.Run("bad pre longs", func(t *testing.T) {
		const (
			k = 25
			n = 30
		)

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(newUnweightedLongsVarOptItemsSketch(t, k, n)))

		serde := common.ItemSketchLongSerDe{}
		data := encodeVarOptItemsUnion(t, union, serde)

		data[0] = varOptItemsUnionEmptyPreLongs - 1 // corrupt the preLongs byte

		_, err = DecodeVarOptItemsUnion[int64](data, serde)
		require.ErrorContains(t, err, "invalid preLongs")
	})

	t.Run("bad family", func(t *testing.T) {
		const (
			k = 25
			n = 30
		)

		union, err := NewVarOptItemsUnion[int64](k)
		require.NoError(t, err)
		require.NoError(t, union.Update(newUnweightedLongsVarOptItemsSketch(t, k, n)))

		serde := common.ItemSketchLongSerDe{}
		data := encodeVarOptItemsUnion(t, union, serde)

		data[2] = 0 // corrupt the family ID byte

		_, err = DecodeVarOptItemsUnion[int64](data, serde)
		require.ErrorContains(t, err, "invalid family ID")
	})
}

func encodeVarOptItemsUnion[T any](t *testing.T, union *VarOptItemsUnion[T], serde common.ItemSketchSerde[T]) []byte {
	t.Helper()

	var buf bytes.Buffer
	enc := NewVarOptItemsUnionEncoder[T](&buf, serde)
	require.NoError(t, enc.Encode(union))
	return buf.Bytes()
}
