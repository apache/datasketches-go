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
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"math/bits"
	"unsafe"

	"github.com/twmb/murmur3"
)

type HllSketch interface {
	publiclyUpdatable
	estimableSketch
	configuredSketch
	toSliceSketch
	privatelyUpdatable
	iterableSketch
	CopyAs(tgtHllType TgtHllType) (HllSketch, error)
	Copy() (HllSketch, error)
	IsEstimationMode() bool
	GetSerializationVersion() int
}

type publiclyUpdatable interface {
	UpdateUInt64(datum uint64) error
	UpdateInt64(datum int64) error
	UpdateSlice(datum []byte) error
	UpdateString(datum string) error
	Reset() error
}

type estimableSketch interface {
	GetCompositeEstimate() (float64, error)
	GetEstimate() (float64, error)
	GetHipEstimate() (float64, error)
	GetLowerBound(numStdDev int) (float64, error)
	GetUpperBound(numStdDev int) (float64, error)
	IsEmpty() bool
}

type configuredSketch interface {
	GetLgConfigK() int
	GetTgtHllType() TgtHllType
	GetCurMode() curMode
}

type toSliceSketch interface {
	GetUpdatableSerializationBytes() int
	ToCompactSlice() ([]byte, error)
	ToUpdatableSlice() ([]byte, error)
}

type privatelyUpdatable interface {
	couponUpdate(coupon int) (hllSketchBase, error)
}

type iterableSketch interface {
	iterator() pairIterator
}

type hllSketchBase interface {
	estimableSketch
	configuredSketch
	toSliceSketch
	privatelyUpdatable
	iterableSketch

	getMemDataStart() int
	getPreInts() int
	isOutOfOrder() bool
	isRebuildCurMinNumKxQFlag() bool

	putOutOfOrder(oooFlag bool)
	putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQFlag bool)
	copyAs(tgtHllType TgtHllType) (hllSketchBase, error)
	copy() (hllSketchBase, error)
	mergeTo(dest HllSketch) error
}

type hllSketchImpl struct { // extends BaseHllSketch
	sketch  hllSketchBase
	scratch [8]byte
}

func NewHllSketch(lgConfigK int, tgtHllType TgtHllType) (HllSketch, error) {
	lgK := lgConfigK
	lgK, err := checkLgK(lgK)
	if err != nil {
		return nil, err
	}
	couponList, err := newCouponList(lgK, tgtHllType, curModeList)
	if err != nil {
		return nil, err
	}
	return newHllSketchImpl(&couponList), nil
}

func NewHllSketchDefault(lgConfigK int) (HllSketch, error) {
	lgK, err := checkLgK(lgConfigK)
	if err != nil {
		return nil, err
	}
	couponList, err := newCouponList(lgK, TgtHllTypeDefault, curModeList)
	if err != nil {
		return nil, err
	}
	return newHllSketchImpl(&couponList), nil
}

func DeserializeHllSketch(byteArray []byte, checkRebuild bool) (HllSketch, error) {
	if len(byteArray) < 8 {
		return nil, fmt.Errorf("input array too small: %d", len(byteArray))
	}
	curMode, err := checkPreamble(byteArray)
	if err != nil {
		return nil, err
	}
	if curMode == curModeHll {
		tgtHllType := extractTgtHllType(byteArray)
		if tgtHllType == TgtHllTypeHll4 {
			sk, err := deserializeHll4(byteArray)
			if err != nil {
				return nil, err
			}
			return newHllSketchImpl(sk), nil
		} else if tgtHllType == TgtHllTypeHll6 {
			return newHllSketchImpl(deserializeHll6(byteArray)), nil
		} else {
			a := newHllSketchImpl(deserializeHll8(byteArray))
			if checkRebuild {
				err := checkRebuildCurMinNumKxQ(a)
				if err != nil {
					return nil, err
				}
			}
			return a, nil
		}
	} else if curMode == curModeList {
		cp, err := deserializeCouponList(byteArray)
		if err != nil {
			return nil, err
		}
		return newHllSketchImpl(cp), nil
	} else {
		chs, err := deserializeCouponHashSet(byteArray)
		if err != nil {
			return nil, err
		}
		return newHllSketchImpl(chs), nil
	}
}

func newHllSketchImpl(coupon hllSketchBase) HllSketch {
	return &hllSketchImpl{
		sketch:  coupon,
		scratch: [8]byte{},
	}
}

func (h *hllSketchImpl) GetEstimate() (float64, error) {
	return h.sketch.GetEstimate()
}

func (h *hllSketchImpl) GetCompositeEstimate() (float64, error) {
	return h.sketch.GetCompositeEstimate()
}

func (h *hllSketchImpl) GetHipEstimate() (float64, error) {
	return h.sketch.GetHipEstimate()
}

func (h *hllSketchImpl) GetUpperBound(numStdDev int) (float64, error) {
	return h.sketch.GetUpperBound(numStdDev)
}

func (h *hllSketchImpl) GetLowerBound(numStdDev int) (float64, error) {
	return h.sketch.GetLowerBound(numStdDev)
}

func (h *hllSketchImpl) GetUpdatableSerializationBytes() int {
	return h.sketch.GetUpdatableSerializationBytes()
}

func (h *hllSketchImpl) UpdateUInt64(datum uint64) error {
	binary.LittleEndian.PutUint64(h.scratch[:], datum)
	_, err := h.couponUpdate(coupon(h.hash(h.scratch[:])))
	return err
}

func (h *hllSketchImpl) UpdateInt64(datum int64) error {
	return h.UpdateUInt64(uint64(datum))
}

func (h *hllSketchImpl) UpdateSlice(datum []byte) error {
	if len(datum) == 0 {
		return nil
	}
	_, err := h.couponUpdate(coupon(h.hash(datum)))
	return err
}

func (h *hllSketchImpl) UpdateString(datum string) error {
	// get a slice to the string data (avoiding a copy to heap)
	return h.UpdateSlice(unsafe.Slice(unsafe.StringData(datum), len(datum)))
}

func (h *hllSketchImpl) IsEmpty() bool {
	return h.sketch.IsEmpty()
}

func (h *hllSketchImpl) ToCompactSlice() ([]byte, error) {
	return h.sketch.ToCompactSlice()
}

func (h *hllSketchImpl) ToUpdatableSlice() ([]byte, error) {
	return h.sketch.ToUpdatableSlice()
}

func (h *hllSketchImpl) GetLgConfigK() int {
	return h.sketch.GetLgConfigK()
}

func (h *hllSketchImpl) GetTgtHllType() TgtHllType {
	return h.sketch.GetTgtHllType()
}

func (h *hllSketchImpl) GetCurMode() curMode {
	return h.sketch.GetCurMode()
}

func (h *hllSketchImpl) Reset() error {
	lgK, err := checkLgK(h.sketch.GetLgConfigK())
	if err != nil {
		return err
	}
	couponList, err := newCouponList(lgK, h.sketch.GetTgtHllType(), curModeList)
	if err != nil {
		return err
	}
	h.sketch = &couponList
	return nil
}

func (h *hllSketchImpl) iterator() pairIterator {
	return h.sketch.iterator()
}

func coupon(hashLo uint64, hashHi uint64) int {
	addr26 := hashLo & keyMask26
	lz := uint64(bits.LeadingZeros64(hashHi))
	value := min(lz, 62) + 1
	return int((value << keyBits26) | addr26)
}

func (h *hllSketchImpl) couponUpdate(coupon int) (hllSketchBase, error) {
	if (coupon >> keyBits26) == empty {
		return h.sketch, nil
	}
	sk, err := h.sketch.couponUpdate(coupon)
	h.sketch = sk
	return h.sketch, err
}

func (h *hllSketchImpl) putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQFlag bool) {
	h.sketch.putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQFlag)
}

func (h *hllSketchImpl) mergeTo(dest HllSketch) error {
	return h.sketch.mergeTo(dest)
}

func (h *hllSketchImpl) CopyAs(tgtHllType TgtHllType) (HllSketch, error) {
	sketch, err := h.sketch.copyAs(tgtHllType)
	if err != nil {
		return nil, err
	}
	return newHllSketchImpl(sketch), nil
}

func (h *hllSketchImpl) Copy() (HllSketch, error) {
	sketch, err := h.sketch.copy()
	if err != nil {
		return nil, err
	}
	return newHllSketchImpl(sketch), nil
}

// IsEstimationMode returns true for all sketches in this package.
// Hll family of sketches and operators is always estimating, even for very small values.
func (h *hllSketchImpl) IsEstimationMode() bool {
	return true
}

// GetSerializationVersion returns the serialization version used by this sketch.
func (h *hllSketchImpl) GetSerializationVersion() int {
	return serVer
}

func (h *hllSketchImpl) hash(bs []byte) (uint64, uint64) {
	return murmur3.SeedSum128(internal.DEFAULT_UPDATE_SEED, internal.DEFAULT_UPDATE_SEED, bs)
}
