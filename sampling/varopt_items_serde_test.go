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
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

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
	assert.Equal(t, union.MaxK(), restored.MaxK())

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
	assert.Equal(t, union.MaxK(), restored.MaxK())

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
