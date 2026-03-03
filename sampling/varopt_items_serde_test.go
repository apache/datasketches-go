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
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

type emptyCorruptingInt64SerDe struct{}

func (emptyCorruptingInt64SerDe) SerializeToBytes(items []int64) ([]byte, error) {
	if len(items) == 0 {
		return []byte{0xCA, 0xFE, 0xBA, 0xBE}, nil
	}
	return Int64SerDe{}.SerializeToBytes(items)
}

func (emptyCorruptingInt64SerDe) DeserializeFromBytes(data []byte, numItems int) ([]int64, error) {
	return Int64SerDe{}.DeserializeFromBytes(data, numItems)
}

func (emptyCorruptingInt64SerDe) SizeOfItem() int {
	return Int64SerDe{}.SizeOfItem()
}

func TestVarOptItemsSketchSerde_EmptyRoundTrip(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	restored, err := NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.True(t, restored.IsEmpty())
	assert.Equal(t, 16, restored.K())
}

func TestVarOptItemsSketchSerde_EmptySketchIgnoresCustomEmptyItemsBytes(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)

	data, err := sketch.ToSlice(emptyCorruptingInt64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, 8, len(data))

	restored, err := NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.True(t, restored.IsEmpty())
	assert.Equal(t, 16, restored.K())
	assert.Equal(t, int64(0), restored.N())
}

func TestVarOptItemsSketchSerde_WarmupRoundTrip(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 10; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	restored, err := NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sketch.K(), restored.K())
	assert.Equal(t, sketch.N(), restored.N())
	assert.Equal(t, sketch.H(), restored.H())
	assert.Equal(t, sketch.R(), restored.R())
	assert.Greater(t, cap(restored.data), restored.H())
	assert.Equal(t, cap(restored.data), cap(restored.weights))
}

func TestVarOptItemsSketchSerde_SamplingRoundTrip(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 80; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.Greater(t, sketch.R(), 0)

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	restored, err := NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sketch.K(), restored.K())
	assert.Equal(t, sketch.N(), restored.N())
	assert.Equal(t, sketch.H(), restored.H())
	assert.Equal(t, sketch.R(), restored.R())
	assert.InDelta(t, sketch.totalWeightR, restored.totalWeightR, 1e-9)
}

func TestVarOptItemsUnionSerde_EmptyRoundTrip(t *testing.T) {
	union, err := NewVarOptItemsUnion[int64](16)
	assert.NoError(t, err)

	data, err := union.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	restored, err := NewVarOptItemsUnionFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, union.maxK, restored.maxK)

	result, err := restored.Result()
	assert.NoError(t, err)
	assert.True(t, result.IsEmpty())
}

func TestVarOptItemsUnionSerde_NonEmptyRoundTrip(t *testing.T) {
	union, err := NewVarOptItemsUnion[int64](16)
	assert.NoError(t, err)

	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 10; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.NoError(t, union.UpdateSketch(sketch))

	data, err := union.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	restored, err := NewVarOptItemsUnionFromSlice[int64](data, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, union.maxK, restored.maxK)

	result, err := restored.Result()
	assert.NoError(t, err)
	assert.Equal(t, sketch.N(), result.N())
	assert.Equal(t, sketch.NumSamples(), result.NumSamples())
}

func TestVarOptItemsSketchSerde_HeaderConsistency(t *testing.T) {
	// preLongs says empty, but empty flag is not set.
	data := make([]byte, 8)
	data[0] = byte(varOptPreambleLongsEmpty)
	data[1] = varOptSerVer
	data[2] = byte(internal.FamilyEnum.VarOptItems.Id)
	data[3] = 0
	binary.LittleEndian.PutUint32(data[4:], 8)

	_, err := NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "empty preLongs without empty flag")
}

func TestVarOptItemsSketchSerde_WarmupDataWithFullPreLongsIsInvalid(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 10; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.Equal(t, 0, sketch.R())

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	data[0] = (data[0] & 0xC0) | byte(varOptPreambleLongsFull)

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "n <= k but not in warmup mode")
}

func TestVarOptItemsSketchSerde_WarmupModeRequiresNEqualsH(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 10; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	binary.LittleEndian.PutUint64(data[8:], uint64(9))

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "warmup mode but n != h")
}

func TestVarOptItemsSketchSerde_WarmupModeRequiresRZero(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 10; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	binary.LittleEndian.PutUint32(data[20:], uint32(1))

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "warmup mode but r > 0")
}

func TestVarOptItemsSketchSerde_FullModeRequiresHSumREqualsK(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 80; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.Greater(t, sketch.R(), 0)

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	h := binary.LittleEndian.Uint32(data[16:])
	binary.LittleEndian.PutUint32(data[16:], h-1)

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "full mode but h + r != k")
}

func TestVarOptItemsSketchSerde_NGreaterThanKRequiresFullMode(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 80; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.Greater(t, sketch.N(), int64(sketch.K()))

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	data[0] = (data[0] & 0xC0) | byte(varOptPreambleLongsWarmup)

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "n > k but not in full mode")
}

func TestVarOptItemsSketchSerde_FullModeRequiresRPositive(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 80; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.Greater(t, sketch.R(), 0)

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	binary.LittleEndian.PutUint32(data[16:], uint32(sketch.K()))
	binary.LittleEndian.PutUint32(data[20:], uint32(0))

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "full mode but r == 0")
}

func TestVarOptItemsSketchSerde_NaNTotalWeightRIsInvalid(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int64](16)
	assert.NoError(t, err)
	for i := int64(1); i <= 80; i++ {
		assert.NoError(t, sketch.Update(i, float64(i)))
	}
	assert.Greater(t, sketch.R(), 0)

	data, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)

	binary.LittleEndian.PutUint64(data[24:], math.Float64bits(math.NaN()))

	_, err = NewVarOptItemsSketchFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "invalid totalWeightR")
}

func TestVarOptItemsUnionSerde_HeaderConsistency(t *testing.T) {
	// preLongs says empty, but empty flag is not set.
	data := make([]byte, 8)
	data[0] = byte(varOptUnionPreambleLongsEmpty)
	data[1] = varOptUnionSerVer
	data[2] = byte(internal.FamilyEnum.VarOptUnion.Id)
	data[3] = 0
	binary.LittleEndian.PutUint32(data[4:], 8)

	_, err := NewVarOptItemsUnionFromSlice[int64](data, Int64SerDe{})
	assert.ErrorContains(t, err, "empty preLongs without empty flag")
}
