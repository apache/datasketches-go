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

package hll

import (
	"fmt"

	"github.com/apache/datasketches-go/common"
)

// hll6ArrayImpl Uses 6 bits per slot in a packed byte array
type hll6ArrayImpl struct {
	hllArrayImpl
}

type hll6Iterator struct {
	hllPairIterator
	hll       *hll6ArrayImpl
	bitOffset int
}

func (h *hll6ArrayImpl) iterator() pairIterator {
	a := newHll6Iterator(1<<h.lgConfigK, h)
	return &a
}

func (h *hll6ArrayImpl) copyAs(tgtHllType TgtHllType) hllSketchBase {
	if tgtHllType == h.tgtHllType {
		return h.copy()
	}
	if tgtHllType == TgtHllType_HLL_4 {
		return convertToHll4(h)
	}
	if tgtHllType == TgtHllType_HLL_8 {
		return convertToHll8(h)
	}
	panic(fmt.Sprintf("Cannot convert to TgtHllType id: %d", int(tgtHllType)))
}

func (h *hll6ArrayImpl) copy() hllSketchBase {
	return &hll6ArrayImpl{
		hllArrayImpl: h.copyCommon(),
	}
}

func (h *hll6ArrayImpl) ToCompactSlice() ([]byte, error) {
	return h.ToUpdatableSlice()
}

func (h *hll6ArrayImpl) ToUpdatableSlice() ([]byte, error) {
	return toHllByteArr(h, false)
}

// newHll6Array returns a new Hll4Array.
func newHll6Array(lgConfigK int) hllArray {
	return &hll6ArrayImpl{
		hllArrayImpl: hllArrayImpl{
			hllSketchConfig: hllSketchConfig{
				lgConfigK:  lgConfigK,
				tgtHllType: TgtHllType_HLL_6,
				curMode:    curMode_HLL,
			},
			curMin:      0,
			numAtCurMin: 1 << lgConfigK,
			hipAccum:    0,
			kxq0:        float64(uint64(1 << lgConfigK)),
			kxq1:        0,
			hllByteArr:  make([]byte, (((1<<lgConfigK)*3)>>2)+1),
			auxStart:    hllByteArrStart + 1<<(lgConfigK-1),
		},
	}
}

// deserializeHll6 returns a new Hll6Array from the given byte array.
func deserializeHll6(byteArray []byte) hllArray {
	lgConfigK := extractLgK(byteArray)
	hll6 := newHll6Array(lgConfigK)
	hll6.extractCommonHll(byteArray)
	return hll6
}

func (h *hll6ArrayImpl) couponUpdate(coupon int) hllSketchBase {
	newValue := coupon >> keyBits26
	configKmask := (1 << h.lgConfigK) - 1
	slotNo := coupon & configKmask
	h.updateSlotWithKxQ(slotNo, newValue)
	return h
}

func (h *hll6ArrayImpl) updateSlotWithKxQ(slotNo int, newValue int) {
	oldValue := h.getSlotValue(slotNo)
	if newValue > oldValue {
		put6Bit(h.hllByteArr, 0, slotNo, newValue)
		h.hipAndKxQIncrementalUpdate(oldValue, newValue)
		if oldValue == 0 {
			h.numAtCurMin-- //interpret numAtCurMin as num Zeros
			if h.numAtCurMin < 0 {
				panic("numAtCurMin < 0")
			}
		}
	}
}

func (h *hll6ArrayImpl) getSlotValue(slotNo int) int {
	return get6Bit(h.hllByteArr, 0, slotNo)
}

func get6Bit(arr []byte, offsetBytes int, slotNo int) int {
	startBit := slotNo * 6
	shift := startBit & 0x7
	byteIdx := (startBit >> 3) + offsetBytes
	return (common.GetShortLE(arr, byteIdx) >> shift) & 0x3F
}

func put6Bit(arr []byte, offsetBytes int, slotNo int, newValue int) {
	startBit := slotNo * 6
	shift := startBit & 0x7
	byteIdx := (startBit >> 3) + offsetBytes
	valShifted := (newValue & 0x3F) << shift
	curMasked := common.GetShortLE(arr, byteIdx) & (^(valMask6 << shift))
	insert := curMasked | valShifted
	common.PutShortLE(arr, byteIdx, insert)
}

func convertToHll6(srcAbsHllArr hllArray) hllSketchBase {
	lgConfigK := srcAbsHllArr.GetLgConfigK()
	hll6Array := newHll6Array(lgConfigK)
	hll6Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder())
	numZeros := 1 << lgConfigK
	srcItr := srcAbsHllArr.iterator()
	for srcItr.nextAll() {
		v := srcItr.getValue()
		if v != empty {
			numZeros--
			p := srcItr.getPair()
			hll6Array.couponUpdate(p) //couponUpdate creates KxQ registers
		}
	}
	hll6Array.putNumAtCurMin(numZeros)
	hll6Array.putHipAccum(srcAbsHllArr.getHipAccum()) //intentional overwrite
	hll6Array.putRebuildCurMinNumKxQFlag(false)
	return hll6Array
}

func newHll6Iterator(lengthPairs int, hll *hll6ArrayImpl) hll6Iterator {
	return hll6Iterator{
		hllPairIterator: newHllPairIterator(lengthPairs),
		hll:             hll,
		bitOffset:       -6,
	}
}

func (h *hll6Iterator) nextAll() bool {
	h.index++
	if h.index >= h.lengthPairs {
		return false
	}
	h.bitOffset += 6
	return true
}

func (h *hll6Iterator) nextValid() bool {
	for h.index+1 < h.lengthPairs {
		h.index++
		h.bitOffset += 6
		tmp := common.GetShortLE(h.hll.hllByteArr, h.bitOffset/8)
		h.value = (tmp >> ((h.bitOffset % 8) & 0x7)) & valMask6
		if h.value != empty {
			return true
		}
	}
	return false
}

func (h *hll6Iterator) getValue() int {
	tmp := common.GetShortLE(h.hll.hllByteArr, h.bitOffset/8)
	shift := (h.bitOffset % 8) & 0x7
	return (tmp >> shift) & valMask6
}

func (h *hll6Iterator) getPair() int {
	v := h.getValue()
	return pair(h.index, v)
}
