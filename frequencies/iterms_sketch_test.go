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
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/murmur3"
)

type StringItemsSketchOp struct {
}

func (h StringItemsSketchOp) Hash(item string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(item), len(item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, datum[:])
}

func (h StringItemsSketchOp) SerializeOneToSlice(item string) []byte {
	// TODO fix me
	return []byte(item)
}

func (h StringItemsSketchOp) SerializeManyToSlice(item []string) []byte {
	// TODO fix me
	out := make([]byte, 0, len(item)*8)
	for _, s := range item {
		out = append(out, h.SerializeOneToSlice(s)...)
	}
	return out
}

type StringPointerHasher struct {
}

func (h StringPointerHasher) Hash(item *string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(*item), len(*item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, datum[:])
}

func (h StringPointerHasher) SerializeOneToSlice(item *string) []byte {
	// TODO fix me
	return []byte(*item)
}

func (h StringPointerHasher) SerializeManyToSlice(item []*string) []byte {
	// TODO fix me
	out := make([]byte, 0, len(item)*8)
	for _, s := range item {
		out = append(out, h.SerializeOneToSlice(s)...)
	}
	return out
}

type IntItemsSketchOp struct {
	scratch [8]byte
}

func (h IntItemsSketchOp) Hash(item int) uint64 {
	binary.LittleEndian.PutUint64(h.scratch[:], uint64(item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, h.scratch[:])
}

func (h IntItemsSketchOp) SerializeOneToSlice(item int) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(item))
	return out
}

func (h IntItemsSketchOp) SerializeManyToSlice(item []int) []byte {
	// TODO fix me
	out := make([]byte, 0, len(item)*8)
	for _, s := range item {
		out = append(out, h.SerializeOneToSlice(s)...)
	}
	return out
}

func TestEmpty(t *testing.T) {
	h := StringItemsSketchOp{}
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
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
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
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
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

	err = sketch.Reset()
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 0)
	assert.Equal(t, sketch.GetStreamLength(), int64(0))
}

func TestEstimationMode(t *testing.T) {
	sketch, err := NewItemsSketchWithMaxMapSize[int](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	err = sketch.UpdateMany(1, 10)
	assert.NoError(t, err)
	err = sketch.Update(2)
	assert.NoError(t, err)
	err = sketch.Update(3)
	assert.NoError(t, err)
	err = sketch.Update(4)
	assert.NoError(t, err)
	err = sketch.Update(5)
	assert.NoError(t, err)
	err = sketch.Update(6)
	assert.NoError(t, err)
	err = sketch.UpdateMany(7, 15)
	assert.NoError(t, err)
	err = sketch.Update(8)
	assert.NoError(t, err)
	err = sketch.Update(9)
	assert.NoError(t, err)
	err = sketch.Update(10)
	assert.NoError(t, err)
	err = sketch.Update(11)
	assert.NoError(t, err)
	err = sketch.Update(12)
	assert.NoError(t, err)

	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetStreamLength(), int64(35))

	{
		items, err := sketch.GetFrequentItems(ErrorTypeEnum.NoFalsePositives)
		assert.NoError(t, err)
		assert.Equal(t, len(items), 2)
		// only 2 items (1 and 7) should have counts more than 1
		count := 0
		for _, item := range items {
			if item.GetLowerBound() > 1 {
				count++
			}
		}
		assert.Equal(t, count, 2)
	}

	{
		items, err := sketch.GetFrequentItems(ErrorTypeEnum.NoFalseNegatives)
		assert.NoError(t, err)
		assert.True(t, len(items) >= 2)
		// only 2 items (1 and 7) should have counts more than 5
		count := 0
		for _, item := range items {
			if item.GetLowerBound() > 5 {
				count++
			}
		}
		assert.Equal(t, count, 2)
	}
}

func TestSerializeStringDeserializeEmpty(t *testing.T) {
	_, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	assert.NoError(t, err)
	//bytes, err := sketch1.ToSlice()
}

/*
  @Test
  public void serializeStringDeserializeEmpty() {
    ItemsSketch<String> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe());
    ItemsSketch<String> sketch2 =
        ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 0);
    Assert.assertEquals(sketch2.getStreamLength(), 0);
  }
*/
