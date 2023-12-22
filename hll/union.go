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
	"github.com/apache/datasketches-go/internal"
)

type Union interface {
	publiclyUpdatable
	estimableSketch
	configuredSketch
	toSliceSketch
	privatelyUpdatable
	UpdateSketch(sketch HllSketch) error
	GetResult(tgtHllType TgtHllType) (HllSketch, error)
}

type unionImpl struct {
	lgMaxK int
	gadget HllSketch
}

func (u *unionImpl) GetHipEstimate() (float64, error) {
	return u.gadget.GetHipEstimate()
}

func (u *unionImpl) GetUpperBound(numStdDev int) (float64, error) {
	return u.gadget.GetUpperBound(numStdDev)
}

func (u *unionImpl) GetLowerBound(numStdDev int) (float64, error) {
	return u.gadget.GetLowerBound(numStdDev)
}

func (u *unionImpl) couponUpdate(coupon int) (hllSketchBase, error) {
	if coupon == empty {
		return u.gadget.(*hllSketchImpl).sketch, nil
	}
	sk, err := u.gadget.couponUpdate(coupon)
	u.gadget.(*hllSketchImpl).sketch = sk
	return sk, err
}

func (u *unionImpl) GetResult(tgtHllType TgtHllType) (HllSketch, error) {
	err := checkRebuildCurMinNumKxQ(u.gadget)
	if err != nil {
		return nil, err
	}
	return u.gadget.CopyAs(tgtHllType)
}

func NewUnionWithDefault() (Union, error) {
	return NewUnion(defaultLgK)
}

func NewUnion(lgMaxK int) (Union, error) {
	sk, err := NewHllSketch(lgMaxK, TgtHllTypeHll8)
	if err != nil {
		return nil, err
	}
	return &unionImpl{
		lgMaxK: lgMaxK,
		gadget: sk,
	}, nil
}

func DeserializeUnion(byteArray []byte) (Union, error) {
	lgK, err := checkLgK(extractLgK(byteArray))
	if err != nil {
		return nil, err
	}
	sk, e := DeserializeHllSketch(byteArray, false)
	if e != nil {
		return nil, e
	}
	union, err := NewUnion(lgK)
	if err != nil {
		return nil, err
	}
	err = union.UpdateSketch(sk)
	return union, err
}

func (u *unionImpl) GetCompositeEstimate() (float64, error) {
	return u.gadget.GetCompositeEstimate()
}

func (u *unionImpl) GetEstimate() (float64, error) {
	return u.gadget.GetEstimate()
}

func (u *unionImpl) UpdateUInt64(datum uint64) error {
	return u.gadget.UpdateUInt64(datum)
}

func (u *unionImpl) UpdateInt64(datum int64) error {
	return u.gadget.UpdateInt64(datum)
}

func (u *unionImpl) UpdateSlice(datum []byte) error {
	return u.gadget.UpdateSlice(datum)
}

func (u *unionImpl) UpdateString(datum string) error {
	return u.gadget.UpdateString(datum)
}

func (u *unionImpl) UpdateSketch(sketch HllSketch) error {
	un, err := u.unionImpl(sketch)
	if err != nil {
		return err
	}
	u.gadget.(*hllSketchImpl).sketch = un
	return nil
}

func (u *unionImpl) GetLgConfigK() int {
	return u.gadget.GetLgConfigK()
}

func (u *unionImpl) GetTgtHllType() TgtHllType {
	return u.gadget.GetTgtHllType()
}

func (u *unionImpl) GetCurMode() curMode {
	return u.gadget.GetCurMode()
}

func (u *unionImpl) IsEmpty() bool {
	return u.gadget.IsEmpty()
}

func (u *unionImpl) ToCompactSlice() ([]byte, error) {
	err := checkRebuildCurMinNumKxQ(u.gadget)
	if err != nil {
		return nil, err
	}
	return u.gadget.ToCompactSlice()
}

func (u *unionImpl) ToUpdatableSlice() ([]byte, error) {
	err := checkRebuildCurMinNumKxQ(u.gadget)
	if err != nil {
		return nil, err
	}
	return u.gadget.ToUpdatableSlice()
}

func (u *unionImpl) GetUpdatableSerializationBytes() int {
	return u.gadget.GetUpdatableSerializationBytes()
}

func (u *unionImpl) Reset() error {
	return u.gadget.Reset()
}

func (u *unionImpl) unionImpl(source HllSketch) (hllSketchBase, error) {
	if u.gadget.GetTgtHllType() != TgtHllTypeHll8 {
		return nil, fmt.Errorf("gadget must be HLL_8")
	}
	if source == nil || source.IsEmpty() {
		return u.gadget.(*hllSketchImpl).sketch, nil
	}

	gadgetC := u.gadget.(*hllSketchImpl)
	sourceC := source.(*hllSketchImpl)

	srcMode := sourceC.sketch.GetCurMode()
	if srcMode == curModeList {
		err := sourceC.mergeTo(u.gadget)
		return u.gadget.(*hllSketchImpl).sketch, err
	}

	srcLgK := source.GetLgConfigK()
	gdgtLgK := u.gadget.GetLgConfigK()
	gdgtEmpty := u.gadget.IsEmpty()

	if srcMode == curModeSet {
		if gdgtEmpty && srcLgK == gdgtLgK {
			un, err := sourceC.CopyAs(TgtHllTypeHll8)
			gadgetC.sketch = un.(*hllSketchImpl).sketch
			return gadgetC.sketch, err
		}
		err := sourceC.mergeTo(u.gadget)
		return gadgetC.sketch, err
	}

	// Hereafter, the source is in HLL mode.
	var (
		bits12 int
		bit3   int
		bit4   int
	)

	if !gdgtEmpty {
		bits12 = int(gadgetC.GetCurMode()) << 1
	} else {
		bits12 = 3 << 1
	}

	if srcLgK < gdgtLgK {
		bit3 = 0
	}

	if srcLgK > u.lgMaxK {
		bit4 = 16
	}

	sw := bit4 | bit3 | bits12

	switch sw {
	case 0, 8, 2, 10:
		// case 0: src <= max, src >= gdt, gdtLIST, gdtHeap
		// case 8: src <= max, src <  gdt, gdtLIST, gdtHeap
		// case 2: src <= max, src >= gdt, gdtSET,  gdtHeap
		// case 10: src <= max, src <  gdt, gdtSET,  gdtHeap
		{
			// Action: copy src, reverse merge w/autofold, ooof=src
			srcHll8, err := sourceC.CopyAs(TgtHllTypeHll8)
			if err != nil {
				return nil, err
			}
			err = gadgetC.mergeTo(srcHll8.(*hllSketchImpl))
			return srcHll8.(*hllSketchImpl).sketch, err
		}
	case 16, 18:
		// case 16: src >  max, src >= gdt, gdtList, gdtHeap
		// case 18: src >  max, src >= gdt, gdtSet,  gdtHeap
		{ //Action: downsample src to MaxLgK, reverse merge w/autofold, ooof=src
			return nil, fmt.Errorf("not implemented cas 16,18")
		}
	case 4, 20:
		// case 4: src <= max, src >= gdt, gdtHLL, gdtHeap
		// case 20: src >  max, src >= gdt, gdtHLL, gdtHeap
		{ //Action: forward HLL merge w/autofold, ooof=True
			//merge src(Hll4,6,8,heap/mem,Mode=HLL) -> gdt(Hll8,heap,Mode=HLL)
			err := mergeHlltoHLLmode(source, u.gadget, srcLgK, gdgtLgK)
			if err != nil {
				return nil, err
			}
			u.gadget.(*hllSketchImpl).sketch.putOutOfOrder(true)
			return u.gadget.(*hllSketchImpl).sketch, nil
		}
	case 12: //src <= max, src <  gdt, gdtHLL, gdtHeap
		{ //Action: downsample gdt to srcLgK, forward HLL merge w/autofold, ooof=True
			return nil, fmt.Errorf("not implemented case 12")
		}
	case 6, 14:
		// case 6: src <= max, src >= gdt, gdtEmpty, gdtHeap
		// case 14: src <= max, src <  gdt, gdtEmpty, gdtHeap
		{ //Action: copy src, replace gdt, ooof=src
			srcHll8, err := sourceC.CopyAs(TgtHllTypeHll8)
			if err != nil {
				return nil, err
			}
			return srcHll8.(*hllSketchImpl).sketch, nil
		}
	case 22: //src >  max, src >= gdt, gdtEmpty, gdtHeap
		{ //Action: downsample src to lgMaxK, replace gdt, ooof=src
			return nil, fmt.Errorf("not implemented")
		}
	default:
		return nil, fmt.Errorf("impossible")
	}
}

func checkRebuildCurMinNumKxQ(sketch HllSketch) error {
	sketchImpl := sketch.(*hllSketchImpl).sketch
	curMode := sketch.GetCurMode()
	tgtHllType := sketch.GetTgtHllType()
	rebuild := sketchImpl.isRebuildCurMinNumKxQFlag()
	if !rebuild || curMode != curModeHll || tgtHllType != TgtHllTypeHll8 {
		return nil
	}

	sketchArrImpl := sketchImpl.(*hll8ArrayImpl)
	curMin := 64
	numAtCurMin := 0
	kxq0 := float64(uint64(1 << sketch.GetLgConfigK()))
	kxq1 := 0.0
	itr := sketchArrImpl.iterator()
	for itr.nextAll() {
		v, err := itr.getValue()
		if err != nil {
			return err
		}
		if v > 0 {
			if v < 32 {
				inv, err := internal.InvPow2(v)
				if err != nil {
					return err
				}
				kxq0 += inv - 1.0
			} else {
				inv, err := internal.InvPow2(v)
				if err != nil {
					return err
				}
				kxq1 += inv - 1.0
			}
		}
		if v > curMin {
			continue
		}
		if v < curMin {
			curMin = v
			numAtCurMin = 1
		} else {
			numAtCurMin++
		}
	}

	sketchArrImpl.putKxQ0(kxq0)
	sketchArrImpl.putKxQ1(kxq1)
	sketchArrImpl.putCurMin(curMin)
	sketchArrImpl.putNumAtCurMin(numAtCurMin)
	sketchArrImpl.putRebuildCurMinNumKxQFlag(false)
	//HipAccum is not affected
	return nil
}

func mergeHlltoHLLmode(src HllSketch, tgt HllSketch, srcLgK int, tgtLgK int) error {
	sw := 0
	if srcLgK > tgtLgK {
		sw |= 4
	}
	if src.GetTgtHllType() != TgtHllTypeHll8 {
		sw |= 8
	}
	srcK := 1 << srcLgK

	switch sw {
	case 0: //HLL_8, srcLgK=tgtLgK, src=heap, tgt=heap
		{
			srcArr := src.(*hllSketchImpl).sketch.(*hll8ArrayImpl).hllByteArr
			tgtArr := tgt.(*hllSketchImpl).sketch.(*hll8ArrayImpl).hllByteArr
			for i := 0; i < srcK; i++ {
				srcV := srcArr[i]
				tgtV := tgtArr[i]
				tgtArr[i] = max(srcV, tgtV)
			}
		}
	case 8, 9: //!HLL_8, srcLgK=tgtLgK, src=heap, tgt=heap/mem
		{
			tgtAbsHllArr := tgt.(*hllSketchImpl).sketch.(*hll8ArrayImpl)
			if src.GetTgtHllType() == TgtHllTypeHll4 {
				src4 := src.(*hllSketchImpl).sketch.(*hll4ArrayImpl)
				auxHashMap := src4.auxHashMap
				curMin := src4.curMin
				i := 0
				j := 0
				for j < srcK {
					b := src4.hllByteArr[i]
					i++
					value := uint(b) & loNibbleMask
					if value == auxToken {
						v, err := auxHashMap.mustFindValueFor(j)
						if err != nil {
							return err
						}
						tgtAbsHllArr.updateSlotNoKxQ(j, v)
					} else {
						tgtAbsHllArr.updateSlotNoKxQ(j, int(value)+curMin)
					}
					j++
					value = uint(b) >> 4
					if value == auxToken {
						v, err := auxHashMap.mustFindValueFor(j)
						if err != nil {
							return err
						}
						tgtAbsHllArr.updateSlotNoKxQ(j, v)
					} else {
						tgtAbsHllArr.updateSlotNoKxQ(j, int(value)+curMin)
					}
					j++
				}
			} else {
				src6 := src.(*hllSketchImpl).sketch.(*hll6ArrayImpl)
				i := 0
				j := 0
				for j < srcK {
					b1 := src6.hllByteArr[i]
					b2 := src6.hllByteArr[i+1]
					b3 := src6.hllByteArr[i+2]
					i += 3
					value := uint(b1) & 0x3f
					tgtAbsHllArr.updateSlotNoKxQ(j, int(value))
					j++
					value = uint(b1) >> 6
					value |= (uint(b2) & 0x0f) << 2
					tgtAbsHllArr.updateSlotNoKxQ(j, int(value))
					j++
					value = uint(b2) >> 4
					value |= (uint(b3) & 3) << 4
					tgtAbsHllArr.updateSlotNoKxQ(j, int(value))
					j++
					value = uint(b3) >> 2
					tgtAbsHllArr.updateSlotNoKxQ(j, int(value))
					j++
				}
			}
		}
		// TODO continue implementing
	default:
		return fmt.Errorf("not implemented")
	}
	tgt.(*hllSketchImpl).sketch.putRebuildCurMinNumKxQFlag(true)
	return nil
}
