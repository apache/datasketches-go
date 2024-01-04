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
	"strconv"
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
	panic("not implemented")
}

func (h StringItemsSketchOp) SerializeManyToSlice(item []string) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	totalBytes := 0
	numItems := len(item)
	serialized2DArray := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		serialized2DArray[i] = []byte(item[i])
		totalBytes += len(serialized2DArray[i]) + 4
	}
	bytesOut := make([]byte, totalBytes)
	offset := 0
	for i := 0; i < numItems; i++ {
		utf8len := len(serialized2DArray[i])
		binary.LittleEndian.PutUint32(bytesOut[offset:], uint32(utf8len))
		offset += 4
		copy(bytesOut[offset:], serialized2DArray[i])
		offset += utf8len
	}
	return bytesOut
}

func (h StringItemsSketchOp) DeserializeManyFromSlice(slc []byte, offset int, length int) []string {
	if length == 0 {
		return []string{}
	}
	array := make([]string, length)
	offsetBytes := offset
	for i := 0; i < length; i++ {
		strLength := int(binary.LittleEndian.Uint32(slc[offsetBytes:]))
		offsetBytes += 4
		utf8Bytes := make([]byte, strLength)
		copy(utf8Bytes, slc[offsetBytes:])
		offsetBytes += strLength
		array[i] = string(utf8Bytes)
	}
	return array
}

type StringPointerSketchOp struct {
}

func (h StringPointerSketchOp) Hash(item *string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(*item), len(*item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, datum[:])
}

func (h StringPointerSketchOp) SerializeOneToSlice(item *string) []byte {
	panic("not implemented")
}

func (h StringPointerSketchOp) SerializeManyToSlice(item []*string) []byte {
	panic("not implemented")
}

func (h StringPointerSketchOp) DeserializeManyFromSlice(slc []byte, offset int, length int) []*string {
	panic("not implemented")
}

type IntItemsSketchOp struct {
	scratch [8]byte
}

func (h IntItemsSketchOp) Hash(item int64) uint64 {
	binary.LittleEndian.PutUint64(h.scratch[:], uint64(item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, h.scratch[:])
}

func (h IntItemsSketchOp) SerializeOneToSlice(item int64) []byte {
	panic("not implemented")
}

func (h IntItemsSketchOp) SerializeManyToSlice(item []int64) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	bytes := make([]byte, 8*len(item))
	offset := 0
	for i := 0; i < len(item); i++ {
		binary.LittleEndian.PutUint64(bytes[offset:], uint64(item[i]))
		offset += 8
	}
	return bytes
}

func (h IntItemsSketchOp) DeserializeManyFromSlice(slc []byte, offset int, length int) []int64 {
	if length == 0 {
		return []int64{}
	}
	array := make([]int64, 0, length)
	offsetBytes := offset
	for i := 0; i < length; i++ {
		array = append(array, int64(binary.LittleEndian.Uint64(slc[offsetBytes:])))
		offsetBytes += 8
	}
	return array
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
	h := StringPointerSketchOp{}
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
	sketch, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
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
	sketch1, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	assert.NoError(t, err)
	bytes := sketch1.ToSlice()
	sketch2, err := NewItemsSketchFromSlice[string](bytes, StringItemsSketchOp{})
	assert.NoError(t, err)
	assert.True(t, sketch2.IsEmpty())
	assert.Equal(t, sketch2.GetNumActiveItems(), 0)
	assert.Equal(t, sketch2.GetStreamLength(), int64(0))
}

func TestSerializeDeserializeUtf8Strings(t *testing.T) {
	sketch1, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	assert.NoError(t, err)
	err = sketch1.Update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	assert.NoError(t, err)
	err = sketch1.Update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	assert.NoError(t, err)
	err = sketch1.Update("ccccccccccccccccccccccccccccc")
	assert.NoError(t, err)
	err = sketch1.Update("ddddddddddddddddddddddddddddd")
	assert.NoError(t, err)

	bytes := sketch1.ToSlice()
	sketch2, err := NewItemsSketchFromSlice[string](bytes, StringItemsSketchOp{})
	assert.NoError(t, err)
	err = sketch2.Update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	assert.NoError(t, err)
	err = sketch2.Update("ccccccccccccccccccccccccccccc")
	assert.NoError(t, err)
	err = sketch2.Update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	assert.NoError(t, err)

	assert.False(t, sketch2.IsEmpty())
	assert.Equal(t, sketch2.GetNumActiveItems(), 4)
	assert.Equal(t, sketch2.GetStreamLength(), int64(7))
	est, err := sketch2.GetEstimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	est, err = sketch2.GetEstimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(3))
	est, err = sketch2.GetEstimate("ccccccccccccccccccccccccccccc")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(2))
	est, err = sketch2.GetEstimate("ddddddddddddddddddddddddddddd")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
}

func TestSerializeDeserializeLong(t *testing.T) {
	sketch1, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	sketch1.Update(1)
	sketch1.Update(2)
	sketch1.Update(3)
	sketch1.Update(4)

	bytes := sketch1.ToSlice()
	sketch2, err := NewItemsSketchFromSlice[int64](bytes, IntItemsSketchOp{})
	sketch2.Update(2)
	sketch2.Update(3)
	sketch2.Update(2)

	assert.False(t, sketch2.IsEmpty())
	assert.Equal(t, sketch2.GetNumActiveItems(), 4)
	assert.Equal(t, sketch2.GetStreamLength(), int64(7))
	est, err := sketch2.GetEstimate(1)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	est, err = sketch2.GetEstimate(2)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(3))
	est, err = sketch2.GetEstimate(3)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(2))
	est, err = sketch2.GetEstimate(4)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
}

func TestResize(t *testing.T) {
	sketch1, err := NewItemsSketchWithMaxMapSize[string](2<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	for i := 0; i < 32; i++ {
		err = sketch1.UpdateMany(strconv.Itoa(i), int64(i*i))
		assert.NoError(t, err)
	}
}

func TestMergeExact(t *testing.T) {
	sketch1, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	assert.NoError(t, err)
	err = sketch1.Update("a")
	assert.NoError(t, err)
	err = sketch1.Update("b")
	assert.NoError(t, err)
	err = sketch1.Update("c")
	assert.NoError(t, err)
	err = sketch1.Update("d")
	assert.NoError(t, err)

	sketch2, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	assert.NoError(t, err)
	err = sketch2.Update("b")
	assert.NoError(t, err)
	err = sketch2.Update("c")
	assert.NoError(t, err)
	err = sketch2.Update("b")
	assert.NoError(t, err)

	_, err = sketch1.Merge(sketch2)
	assert.NoError(t, err)
	assert.False(t, sketch1.IsEmpty())
	assert.Equal(t, sketch1.GetNumActiveItems(), 4)
	assert.Equal(t, sketch1.GetStreamLength(), int64(7))
	est, err := sketch1.GetEstimate("a")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	est, err = sketch1.GetEstimate("b")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(3))
	est, err = sketch1.GetEstimate("c")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(2))
	est, err = sketch1.GetEstimate("d")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
}

func TestNullMapReturns(t *testing.T) {
	map1, err := newReversePurgeItemHashMap[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	assert.Nil(t, map1.getActiveKeys())
	assert.Nil(t, map1.getActiveValues())
}

func TestMisc(t *testing.T) {
	sk1, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	assert.Equal(t, sk1.GetCurrentMapCapacity(), 6)
	est, err := sk1.GetEstimate(1)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(0))
	sk2, err := NewItemsSketchWithMaxMapSize[int64](8, IntItemsSketchOp{})
	assert.NoError(t, err)
	_, err = sk1.Merge(sk2)
	assert.NoError(t, err)
	_, err = sk1.Merge(nil)
	assert.NoError(t, err)
	err = sk1.Update(1)
	assert.NoError(t, err)
	rows, err := sk1.GetFrequentItems(ErrorTypeEnum.NoFalseNegatives)
	assert.NoError(t, err)
	row := rows[0]
	assert.Equal(t, row.GetItem(), int64(1))
	assert.Equal(t, row.GetEstimate(), int64(1))
	assert.Equal(t, row.GetUpperBound(), int64(1))
	s := row.String()
	t.Log(s)
	var nullRow *RowItem[int64]
	assert.NotEqual(t, row, nullRow)
}

func TestToString(t *testing.T) {
	sk, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	err = sk.Update(1)
	t.Log(sk.ToString())
}

func TestFrequentItems1(t *testing.T) {
	fis, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	fis.Update(1)
	rows, err := fis.GetFrequentItems(ErrorTypeEnum.NoFalsePositives)
	assert.NoError(t, err)
	row := rows[0]
	assert.NotNil(t, row)
	assert.Equal(t, row.GetItem(), int64(1))
	assert.Equal(t, row.GetEstimate(), int64(1))
	assert.Equal(t, row.GetUpperBound(), int64(1))
	newRow := newRowItem[int64](row.GetItem(), row.GetEstimate()+1, row.GetUpperBound(), row.GetLowerBound())
	assert.NotEqual(t, row, newRow)
	newRow = newRowItem[int64](row.GetItem(), row.GetEstimate(), row.GetUpperBound(), row.GetLowerBound())
	assert.Equal(t, row, newRow)
}

func TestUpdateExceptions(t *testing.T) {
	sk1, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	err = sk1.UpdateMany(1, -1)
	assert.Error(t, err)
}

func TestMemExceptions(t *testing.T) {
	sk1, err := NewItemsSketchWithMaxMapSize[int64](1<<_LG_MIN_MAP_SIZE, IntItemsSketchOp{})
	assert.NoError(t, err)
	sk1.Update(1)
	bytes := sk1.ToSlice()
	pre0 := binary.LittleEndian.Uint64(bytes)
	//Now start corrupting
	tryBadMem(t, bytes, _PREAMBLE_LONGS_BYTE, 2) //Corrupt
	binary.LittleEndian.PutUint64(bytes, pre0)   //restore

	tryBadMem(t, bytes, _SER_VER_BYTE, 2)      //Corrupt
	binary.LittleEndian.PutUint64(bytes, pre0) //restore

	tryBadMem(t, bytes, _FAMILY_BYTE, 2)       //Corrupt
	binary.LittleEndian.PutUint64(bytes, pre0) //restore

	tryBadMem(t, bytes, _FLAGS_BYTE, 4)        //Corrupt to true
	binary.LittleEndian.PutUint64(bytes, pre0) //restore
}

func TestOneItemUtf8(t *testing.T) {
	sketch1, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, StringItemsSketchOp{})
	assert.NoError(t, err)
	err = sketch1.Update("\u5fb5")
	assert.NoError(t, err)
	assert.False(t, sketch1.IsEmpty())
	assert.Equal(t, sketch1.GetNumActiveItems(), 1)
	assert.Equal(t, sketch1.GetStreamLength(), int64(1))
	est, err := sketch1.GetEstimate("\u5fb5")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))

	bytes := sketch1.ToSlice()
	sketch2, err := NewItemsSketchFromSlice[string](bytes, StringItemsSketchOp{})
	assert.NoError(t, err)
	assert.False(t, sketch2.IsEmpty())
	assert.Equal(t, sketch2.GetNumActiveItems(), 1)
	assert.Equal(t, sketch2.GetStreamLength(), int64(1))
	est, err = sketch2.GetEstimate("\u5fb5")
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
}

func TestItemGetEpsilon(t *testing.T) {
	esp, err := GetEpsilonItemsSketch(1024)
	assert.NoError(t, err)
	assert.Equal(t, esp, 3.5/1024)

	_, err = GetEpsilonItemsSketch(1000)
	assert.Error(t, err)
}

func TestItemGetAprioriError(t *testing.T) {
	eps := 3.5 / 1024
	apr, err := GetAprioriErrorItemsSketch(1024, 10_000)
	assert.NoError(t, err)
	assert.Equal(t, apr, eps*10_000)
}
