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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFrequentItemsStringSerial(t *testing.T) {
	sketch, err := NewLongSketchWithMaxMapSize(8)
	assert.NoError(t, err)
	sketch2, err := NewLongSketchWithMaxMapSize(128)
	assert.NoError(t, err)
	sketch.Update(10, 100)
	sketch.Update(10, 100)
	sketch.Update(15, 3443)
	sketch.Update(1000001, 1010230)
	sketch.Update(1000002, 1010230)

	ser, err := sketch.serializeToString()
	assert.NoError(t, err)
	newSk0, err := NewLongSketchFromString(ser)
	assert.NoError(t, err)
	newSer0, err := newSk0.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, ser, newSer0)
	assert.Equal(t, sketch.getMaximumMapCapacity(), newSk0.getMaximumMapCapacity())
	assert.Equal(t, sketch.getCurrentMapCapacity(), newSk0.getCurrentMapCapacity())

	sketch2.Update(190, 12902390)
	sketch2.Update(191, 12902390)
	sketch2.Update(192, 12902390)
	sketch2.Update(193, 12902390)
	sketch2.Update(194, 12902390)
	sketch2.Update(195, 12902390)
	sketch2.Update(196, 12902390)
	sketch2.Update(197, 12902390)
	sketch2.Update(198, 12902390)
	sketch2.Update(199, 12902390)
	sketch2.Update(200, 12902390)
	sketch2.Update(201, 12902390)
	sketch2.Update(202, 12902390)
	sketch2.Update(203, 12902390)
	sketch2.Update(204, 12902390)
	sketch2.Update(205, 12902390)
	sketch2.Update(206, 12902390)
	sketch2.Update(207, 12902390)
	sketch2.Update(208, 12902390)

	s2, err := sketch2.serializeToString()
	assert.NoError(t, err)
	newSk2, err := NewLongSketchFromString(s2)
	assert.NoError(t, err)
	newS2, err := newSk2.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, s2, newS2)
	assert.Equal(t, sketch2.getMaximumMapCapacity(), newSk2.getMaximumMapCapacity())
	assert.Equal(t, sketch2.getCurrentMapCapacity(), newSk2.getCurrentMapCapacity())
	assert.Equal(t, sketch2.getStreamLength(), newSk2.getStreamLength())

	mergedSketch, err := sketch.merge(sketch2)
	assert.NoError(t, err)
	ms, err := mergedSketch.serializeToString()
	assert.NoError(t, err)
	newMs, err := NewLongSketchFromString(ms)
	assert.NoError(t, err)
	newSMs, err := newMs.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, ms, newSMs)
	assert.Equal(t, mergedSketch.getMaximumMapCapacity(), newMs.getMaximumMapCapacity())
	assert.Equal(t, mergedSketch.getCurrentMapCapacity(), newMs.getCurrentMapCapacity())
	assert.Equal(t, mergedSketch.getStreamLength(), newMs.getStreamLength())
}

func TestFrequentItemsByteSerial(t *testing.T) {
	sketch, err := NewLongSketchWithMaxMapSize(16)
	assert.NoError(t, err)
	byteArray0, err := sketch.toSlice()
	newSk0, err := NewLongSketchFromSlice(byteArray0)
	assert.NoError(t, err)
	str0, err := sketch.serializeToString()
	assert.NoError(t, err)
	newStr0, err := newSk0.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, str0, newStr0)

	sketch2, err := NewLongSketchWithMaxMapSize(128)
	assert.NoError(t, err)
	sketch.Update(10, 100)
	sketch.Update(10, 100)
	sketch.Update(15, 3443)
	sketch.Update(1000001, 1010230)
	sketch.Update(1000002, 1010230)

	byteArray1, err := sketch.toSlice()
	assert.NoError(t, err)
	newSk1, err := NewLongSketchFromSlice(byteArray1)
	assert.NoError(t, err)
	str1, err := sketch.serializeToString()
	newStr1, err := newSk1.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, str1, newStr1)
	assert.Equal(t, sketch.getMaximumMapCapacity(), newSk1.getMaximumMapCapacity())
	assert.Equal(t, sketch.getCurrentMapCapacity(), newSk1.getCurrentMapCapacity())

	sketch2.Update(190, 12902390)
	sketch2.Update(191, 12902390)
	sketch2.Update(192, 12902390)
	sketch2.Update(193, 12902390)
	sketch2.Update(194, 12902390)
	sketch2.Update(195, 12902390)
	sketch2.Update(196, 12902390)
	sketch2.Update(197, 12902390)
	sketch2.Update(198, 12902390)
	sketch2.Update(199, 12902390)
	sketch2.Update(200, 12902390)
	sketch2.Update(201, 12902390)
	sketch2.Update(202, 12902390)
	sketch2.Update(203, 12902390)
	sketch2.Update(204, 12902390)
	sketch2.Update(205, 12902390)
	sketch2.Update(206, 12902390)
	sketch2.Update(207, 12902390)
	sketch2.Update(208, 12902390)

	byteArray2, err := sketch2.toSlice()
	assert.NoError(t, err)
	newSk2, err := NewLongSketchFromSlice(byteArray2)
	assert.NoError(t, err)
	str2, err := sketch2.serializeToString()
	assert.NoError(t, err)
	newStr2, err := newSk2.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, str2, newStr2)
	assert.Equal(t, sketch2.getMaximumMapCapacity(), newSk2.getMaximumMapCapacity())
	assert.Equal(t, sketch2.getCurrentMapCapacity(), newSk2.getCurrentMapCapacity())
	assert.Equal(t, sketch2.getStreamLength(), newSk2.getStreamLength())

	mergedSketch, err := sketch.merge(sketch2)
	assert.NoError(t, err)
	byteArray3, err := mergedSketch.toSlice()
	assert.NoError(t, err)
	newSk3, err := NewLongSketchFromSlice(byteArray3)
	assert.NoError(t, err)
	str3, err := mergedSketch.serializeToString()
	assert.NoError(t, err)
	newStr3, err := newSk3.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, str3, newStr3)
	assert.Equal(t, mergedSketch.getMaximumMapCapacity(), newSk3.getMaximumMapCapacity())
	assert.Equal(t, mergedSketch.getCurrentMapCapacity(), newSk3.getCurrentMapCapacity())
	assert.Equal(t, mergedSketch.getStreamLength(), newSk3.getStreamLength())
}

func TestFrequentItemsByteResetAndEmptySerial(t *testing.T) {
	sketch, err := NewLongSketchWithMaxMapSize(16)
	assert.NoError(t, err)
	sketch.Update(10, 100)
	sketch.Update(10, 100)
	sketch.Update(15, 3443)
	sketch.Update(1000001, 1010230)
	sketch.Update(1000002, 1010230)
	sketch.Reset()

	byteArray0, err := sketch.toSlice()
	assert.NoError(t, err)
	newSk0, err := NewLongSketchFromSlice(byteArray0)
	assert.NoError(t, err)
	str0, err := sketch.serializeToString()
	assert.NoError(t, err)
	newStr0, err := newSk0.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, str0, newStr0)
	assert.Equal(t, sketch.getMaximumMapCapacity(), newSk0.getMaximumMapCapacity())
	assert.Equal(t, sketch.getCurrentMapCapacity(), newSk0.getCurrentMapCapacity())
}

func TestFreqLongMeSerDe(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	sk1, err := NewLongSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	sk1.Update(10, 100)
	sk1.Update(10, 100)
	sk1.Update(15, 3443)
	sk1.Update(1000001, 1010230)
	sk1.Update(1000002, 1010230)

	byteArray0, err := sk1.toSlice()
	assert.NoError(t, err)
	sk2, err := NewLongSketchFromSlice(byteArray0)
	assert.NoError(t, err)

	checkEquality(t, sk1, sk2)
}

/*
  @Test
  public void checkFreqLongsMemSerDe() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443); println(sk1.toString());
    sk1.update(1000001, 1010230); println(sk1.toString());
    sk1.update(1000002, 1010230); println(sk1.toString());

    byte[] bytearray0 = sk1.toByteArray();
    Memory mem0 = Memory.wrap(bytearray0);
    LongsSketch sk2 = LongsSketch.getInstance(mem0);

    checkEquality(sk1, sk2);
  }
*/

func checkEquality(t *testing.T, sk1, sk2 *LongSketch) {
	assert.Equal(t, sk1.getNumActiveItems(), sk2.getNumActiveItems())
	assert.Equal(t, sk1.getCurrentMapCapacity(), sk2.getCurrentMapCapacity())
	assert.Equal(t, sk1.getMaximumError(), sk2.getMaximumError())
	assert.Equal(t, sk1.getMaximumMapCapacity(), sk2.getMaximumMapCapacity())
	assert.Equal(t, sk1.getStorageBytes(), sk2.getStorageBytes())
	assert.Equal(t, sk1.getStreamLength(), sk2.getStreamLength())
	assert.Equal(t, sk1.isEmpty(), sk2.isEmpty())

	NFN := NO_FALSE_NEGATIVES
	NFP := NO_FALSE_POSITIVES

	rowArr1, err := sk1.getFrequentItems(NFN)
	assert.NoError(t, err)
	rowArr2, err := sk2.getFrequentItems(NFN)
	assert.NoError(t, err)
	assert.Equal(t, len(rowArr1), len(rowArr2))
	for i := 0; i < len(rowArr1); i++ {
		s1 := rowArr1[i].String()
		s2 := rowArr2[i].String()
		assert.Equal(t, s1, s2)
	}

	rowArr1, err = sk1.getFrequentItems(NFP)
	assert.NoError(t, err)
	rowArr2, err = sk2.getFrequentItems(NFP)
	assert.NoError(t, err)
	assert.Equal(t, len(rowArr1), len(rowArr2))
	for i := 0; i < len(rowArr1); i++ {
		s1 := rowArr1[i].String()
		s2 := rowArr2[i].String()
		assert.Equal(t, s1, s2)
	}
}
