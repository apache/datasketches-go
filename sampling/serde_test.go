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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInt64SerDe(t *testing.T) {
	serde := Int64SerDe{}
	items := []int64{1, 2, 3, 42, -100, 1000000}

	bytes, err := serde.SerializeToBytes(items)
	assert.NoError(t, err)
	assert.Equal(t, len(items)*8, len(bytes))

	restored, err := serde.DeserializeFromBytes(bytes, len(items))
	assert.NoError(t, err)
	assert.Equal(t, items, restored)
}

func TestInt32SerDe(t *testing.T) {
	serde := Int32SerDe{}
	items := []int32{1, 2, 3, 42, -100, 1000000}

	bytes, err := serde.SerializeToBytes(items)
	assert.NoError(t, err)
	assert.Equal(t, len(items)*4, len(bytes))

	restored, err := serde.DeserializeFromBytes(bytes, len(items))
	assert.NoError(t, err)
	assert.Equal(t, items, restored)
}

func TestFloat64SerDe(t *testing.T) {
	serde := Float64SerDe{}
	items := []float64{1.5, 2.5, 3.14159, -100.5}

	bytes, err := serde.SerializeToBytes(items)
	assert.NoError(t, err)
	assert.Equal(t, len(items)*8, len(bytes))

	restored, err := serde.DeserializeFromBytes(bytes, len(items))
	assert.NoError(t, err)
	assert.Len(t, restored, len(items))
}

func TestStringSerDe(t *testing.T) {
	serde := StringSerDe{}
	items := []string{"hello", "world", "", "testing 123", "日本語"}

	bytes, err := serde.SerializeToBytes(items)
	assert.NoError(t, err)

	restored, err := serde.DeserializeFromBytes(bytes, len(items))
	assert.NoError(t, err)
	assert.Equal(t, items, restored)
}

func TestSketchSerializationInt64(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 5; i++ {
		sketch.Update(i)
	}

	bytes, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)
	assert.NotNil(t, bytes)

	restored, err := NewReservoirItemsSketchFromSlice[int64](bytes, Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sketch.K(), restored.K())
	assert.Equal(t, sketch.N(), restored.N())
	assert.Equal(t, sketch.Samples(), restored.Samples())
}

func TestSketchSerializationString(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[string](5)

	sketch.Update("apple")
	sketch.Update("banana")
	sketch.Update("cherry")

	bytes, err := sketch.ToSlice(StringSerDe{})
	assert.NoError(t, err)

	restored, err := NewReservoirItemsSketchFromSlice[string](bytes, StringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sketch.K(), restored.K())
	assert.Equal(t, sketch.N(), restored.N())
	assert.Equal(t, sketch.Samples(), restored.Samples())
}

func TestSketchSerializationEmpty(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)

	bytes, err := sketch.ToSlice(Int64SerDe{})
	assert.NoError(t, err)
	assert.Equal(t, 8, len(bytes)) // Minimal preamble

	restored, err := NewReservoirItemsSketchFromSlice[int64](bytes, Int64SerDe{})
	assert.NoError(t, err)
	assert.True(t, restored.IsEmpty())
	assert.Equal(t, 10, restored.K())
}
