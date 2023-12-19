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
)

// hll8ArrayImpl Uses 6 bits per slot in a packed byte array
type hll8ArrayImpl struct {
	hllArrayImpl
}

type hll8Iterator struct {
	hllPairIterator
	hll *hll8ArrayImpl
}

func (h *hll8ArrayImpl) iterator() pairIterator {
	a := newHll8Iterator(1<<h.lgConfigK, h)
	return &a
}

func (h *hll8ArrayImpl) copyAs(tgtHllType TgtHllType) hllSketchBase {
	if tgtHllType == h.tgtHllType {
		return h.copy()
	}
	if tgtHllType == TgtHllType_HLL_4 {
		return convertToHll4(h)
	}
	if tgtHllType == TgtHllType_HLL_6 {
		return convertToHll6(h)
	}
	panic(fmt.Sprintf("Cannot convert to TgtHllType id: %d", int(tgtHllType)))
}

func (h *hll8ArrayImpl) copy() hllSketchBase {
	return &hll8ArrayImpl{
		hllArrayImpl: h.copyCommon(),
	}
}

func (h *hll8ArrayImpl) ToCompactSlice() ([]byte, error) {
	return h.ToUpdatableSlice()
}

func (h *hll8ArrayImpl) ToUpdatableSlice() ([]byte, error) {
	return toHllByteArr(h, false)
}

// newHll8Array returns a new Hll8Array.
func newHll8Array(lgConfigK int) hllArray {
	return &hll8ArrayImpl{
		hllArrayImpl: hllArrayImpl{
			hllSketchConfig: hllSketchConfig{
				lgConfigK:  lgConfigK,
				tgtHllType: TgtHllType_HLL_8,
				curMode:    curMode_HLL,
			},
			curMin:      0,
			numAtCurMin: 1 << lgConfigK,
			hipAccum:    0,
			kxq0:        float64(uint64(1 << lgConfigK)),
			kxq1:        0,
			hllByteArr:  make([]byte, 1<<lgConfigK),
			auxStart:    hllByteArrStart + 1<<(lgConfigK-1),
		},
	}
}

// deserializeHll8 returns a new Hll8Array from the given byte array.
func deserializeHll8(byteArray []byte) hllArray {
	lgConfigK := extractLgK(byteArray)
	hll8 := newHll8Array(lgConfigK)
	hll8.extractCommonHll(byteArray)
	return hll8
}

func convertToHll8(srcAbsHllArr hllArray) hllSketchBase {
	lgConfigK := srcAbsHllArr.GetLgConfigK()
	hll8Array := newHll8Array(lgConfigK)
	hll8Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder())
	numZeros := 1 << lgConfigK
	itr := srcAbsHllArr.iterator()
	for itr.nextAll() {
		v := itr.getValue()
		if v != empty {
			numZeros--
			p := itr.getPair()
			hll8Array.couponUpdate(p) //creates KxQ registers
		}
	}
	hll8Array.putNumAtCurMin(numZeros)
	hll8Array.putHipAccum(srcAbsHllArr.getHipAccum()) //intentional overwrite
	hll8Array.putRebuildCurMinNumKxQFlag(false)
	return hll8Array
}

func (h *hll8ArrayImpl) couponUpdate(coupon int) hllSketchBase {
	newValue := coupon >> keyBits26
	configKmask := (1 << h.lgConfigK) - 1
	slotNo := coupon & configKmask
	h.updateSlotWithKxQ(slotNo, newValue)
	return h
}

func (h *hll8ArrayImpl) updateSlotWithKxQ(slotNo int, newValue int) {
	oldValue := h.getSlotValue(slotNo)
	if newValue > oldValue {
		h.hllByteArr[slotNo] = byte(newValue & valMask6)
		h.hipAndKxQIncrementalUpdate(oldValue, newValue)
		if oldValue == 0 {
			h.numAtCurMin-- //interpret numAtCurMin as num Zeros
			if h.numAtCurMin < 0 {
				panic("numAtCurMin < 0")
			}
		}
	}
}

func (h *hll8ArrayImpl) updateSlotNoKxQ(slotNo int, newValue int) {
	oldValue := h.getSlotValue(slotNo)
	h.hllByteArr[slotNo] = byte(max(newValue, oldValue))
}

func (h *hll8ArrayImpl) getSlotValue(slotNo int) int {
	return int(h.hllByteArr[slotNo] & valMask6)
}

func newHll8Iterator(lengthPairs int, hll *hll8ArrayImpl) hll8Iterator {
	return hll8Iterator{
		hllPairIterator: newHllPairIterator(lengthPairs),
		hll:             hll,
	}
}

func (h *hll8Iterator) nextValid() bool {
	for h.index+1 < h.lengthPairs {
		h.index++
		h.value = int(h.hll.hllByteArr[h.index]) & valMask6
		if h.value != empty {
			return true
		}
	}
	return false
}

func (h *hll8Iterator) getValue() int {
	return int(h.hll.hllByteArr[h.index]) & valMask6
}

func (h *hll8Iterator) getPair() int {
	return pair(h.index, h.getValue())
}
