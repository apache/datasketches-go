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

package frequencies

import (
	"testing"
	"unsafe"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/murmur3"
)

type StringHasher struct {
}

func (h StringHasher) Hash(item string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(item), len(item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, datum[:])
}

func TestEmpty(t *testing.T) {
	h := StringHasher{}
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, h)
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 0)
	assert.Equal(t, sketch.GetStreamLength(), int64(0))
	lb, err := sketch.GetLowerBound("a")
	assert.NoError(t, err)
	assert.Equal(t, lb, int64(0))
	ub, err := sketch.GetUpperBound("a")
	assert.NoError(t, err)
	assert.Equal(t, ub, int64(0))
}

type StringPointerHasher struct {
}

func (h StringPointerHasher) Hash(item *string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(*item), len(*item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, datum[:])
}

func TestNilInput(t *testing.T) {
	h := StringPointerHasher{}
	sketch, err := NewItemsSketchWithMaxMapSize[*string](1<<_LG_MIN_MAP_SIZE, h)
	assert.NoError(t, err)
	err = sketch.Update(nil)
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 0)
	assert.Equal(t, sketch.GetStreamLength(), int64(0))
	lb, err := sketch.GetLowerBound(nil)
	assert.NoError(t, err)
	assert.Equal(t, lb, int64(0))
	ub, err := sketch.GetUpperBound(nil)
	assert.NoError(t, err)
	assert.Equal(t, ub, int64(0))

}

func TestOneItem(t *testing.T) {
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringHasher{})
	assert.NoError(t, err)
	err = sketch.Update("a")
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 1)
	assert.Equal(t, sketch.GetStreamLength(), int64(1))
	est, err := sketch.GetEstimate("a")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	lb, err := sketch.GetLowerBound("a")
	assert.NoError(t, err)
	assert.Equal(t, lb, int64(1))
}

func TestSeveralItem(t *testing.T) {
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringHasher{})
	assert.NoError(t, err)
	err = sketch.Update("a")
	assert.NoError(t, err)
	err = sketch.Update("b")
	assert.NoError(t, err)
	err = sketch.Update("c")
	assert.NoError(t, err)
	err = sketch.Update("d")
	assert.NoError(t, err)
	err = sketch.Update("b")
	assert.NoError(t, err)
	err = sketch.Update("c")
	assert.NoError(t, err)
	err = sketch.Update("b")
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 4)
	assert.Equal(t, sketch.GetStreamLength(), int64(7))
	est, err := sketch.GetEstimate("a")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	est, err = sketch.GetEstimate("b")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(3))
	est, err = sketch.GetEstimate("c")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(2))
	est, err = sketch.GetEstimate("d")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))

	items, err := sketch.GetFrequentItems(ErrorTypeEnum.NoFalsePositives)
	assert.NoError(t, err)
	assert.Equal(t, len(items), 4)

	items, err = sketch.GetFrequentItemsWithThreshold(3, ErrorTypeEnum.NoFalsePositives)
	assert.NoError(t, err)
	assert.Equal(t, len(items), 1)
	assert.Equal(t, items[0].item, "b")

	sketch.Reset()
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 0)
	assert.Equal(t, sketch.GetStreamLength(), int64(0))

}
