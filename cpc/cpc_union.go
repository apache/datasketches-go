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

package cpc

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"math/bits"
)

type CpcUnion struct {
	seed uint64
	lgK  int

	// Note: at most one of bitMatrix and accumulator will be non-null at any given moment.
	// accumulator is a sketch object that is employed until it graduates out of Sparse mode.
	// At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
	// but doesn't maintain any of the "extra" fields of our sketch objects, so some additional work
	// is required when getResult is called at the end.
	bitMatrix   []uint64
	accumulator *CpcSketch
}

func NewCpcUnionSketch(lgK int, seed uint64) (CpcUnion, error) {
	acc, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		return CpcUnion{}, err
	}
	return CpcUnion{
		seed: seed,
		lgK:  lgK,
		// We begin with the accumulator holding an EMPTY_MERGED sketch object.
		// As an optimization the accumulator could start as NULL, but that would require changes elsewhere.
		accumulator: acc,
	}, nil
}

func NewCpcUnionSketchWithDefault(lgK int) (CpcUnion, error) {
	return NewCpcUnionSketch(lgK, internal.DEFAULT_UPDATE_SEED)
}

func (u *CpcUnion) GetFamilyId() int {
	return internal.FamilyEnum.CPC.Id
}

func (u *CpcUnion) Update(source *CpcSketch) error {
	if err := checkSeeds(u.seed, source.seed); err != nil {
		return err
	}

	sourceFlavorOrd := source.GetFlavor()
	if sourceFlavorOrd == CpcFlavorEmpty {
		return nil
	}

	// Accumulator and bitMatrix must be mutually exclusive,
	// so bitMatrix != nil => accumulator == nil and visa versa
	// if (Accumulator != nil) union must be EMPTY or SPARSE,
	if err := u.checkUnionState(); err != nil {
		return err
	}

	if source.lgK < u.lgK {
		if err := u.reduceUnionK(source.lgK); err != nil {
			return err
		}
	}

	// if source is past SPARSE mode, make sure that union is a bitMatrix.
	if sourceFlavorOrd > CpcFlavorSparse && u.accumulator.lgK != 0 {
		u.bitMatrix = u.accumulator.bitMatrixOfSketch()
		u.accumulator = nil
	}

	state := (sourceFlavorOrd - 1) << 1
	if u.bitMatrix != nil {
		state |= 1
	}

	switch state {
	case 0: //A: Sparse, bitMatrix == nil, accumulator valid
		if u.accumulator.lgK == 0 {
			return fmt.Errorf("union accumulator cannot be nil")
		}
		if u.accumulator.GetFlavor() == CpcFlavorEmpty && u.lgK == source.lgK {
			u.accumulator = source
			break
		}
		if err := walkTableUpdatingSketch(u.accumulator, source.pairTable); err != nil {
			return err
		}
		// if the accumulator has graduated beyond sparse, switch union to a bitMatrix
		if u.accumulator.GetFlavor() > CpcFlavorSparse {
			bitMatrix := u.accumulator.bitMatrixOfSketch()
			u.bitMatrix = bitMatrix
			u.accumulator = nil
		}
	case 1: //B: Sparse, bitMatrix valid, accumulator == nil
		u.orTableIntoMatrix(source.pairTable)
	case 3, 5:
		//C: Hybrid, bitMatrix valid, accumulator == nil
		//C: Pinned, bitMatrix valid, accumulator == nil
		u.orWindowIntoMatrix(source.slidingWindow, source.windowOffset, source.lgK)
		u.orTableIntoMatrix(source.pairTable)
	case 7: //D: Sliding, bitMatrix valid, accumulator == null
		// SLIDING mode involves inverted logic, so we can't just walk the source sketch.
		// Instead, we convert it to a bitMatrix that can be OR'ed into the destination.
		sourceMatrix := source.bitMatrixOfSketch()
		u.orMatrixIntoMatrix(sourceMatrix, source.lgK)
	default:
		return fmt.Errorf("illegal Union state: %d", state)
	}
	return nil
}

func (u *CpcUnion) GetResult() (*CpcSketch, error) {
	if err := u.checkUnionState(); err != nil {
		return nil, err
	}

	if u.lgK != 0 { // start of case where union contains a sketch
		if u.accumulator.numCoupons == 0 {
			result, err := NewCpcSketch(u.lgK, u.accumulator.seed)
			if err != nil {
				return nil, err
			}
			result.mergeFlag = true
			return result, nil
		}
		if u.accumulator.GetFlavor() != CpcFlavorSparse {
			return nil, fmt.Errorf("accumulator must be SPARSE")
		}
		result := u.accumulator // effectively a copy
		result.mergeFlag = true
		return result, nil
	} // end of case where union contains a sketch

	// start of case where union contains a bitMatrix
	matrix := u.bitMatrix
	lgK := u.lgK
	result, err := NewCpcSketch(u.lgK, u.seed)
	if err != nil {
		return nil, err
	}

	numCoupons := countBitsSetInMatrix(matrix)
	result.numCoupons = numCoupons

	flavor := determineFlavor(lgK, numCoupons)
	if flavor <= CpcFlavorSparse {
		return nil, fmt.Errorf("flavor must be greater than SPARSE")
	}

	offset := determineCorrectOffset(lgK, numCoupons)
	result.windowOffset = offset

	//Build the window and pair table
	k := 1 << lgK
	window := make([]byte, k)
	result.slidingWindow = window

	// LgSize = K/16; in some cases this will end up being oversized
	newTableLgSize := max(lgK-4, 2)
	table, err := NewPairTable(newTableLgSize, 6+lgK)
	if err != nil {
		return nil, err
	}
	result.pairTable = table

	// The following works even when the offset is zero.
	maskForClearingWindow := (0xFF << offset) ^ -1
	maskForFlippingEarlyZone := (1 << offset) - 1
	allSurprisesORed := uint64(0)

	// Using a sufficiently large hash table avoids the Snow Plow Effect
	for i := 0; i < k; i++ {
		pattern := matrix[i]
		window[i] = byte((pattern >> offset) & 0xFF)
		pattern &= uint64(maskForClearingWindow)
		pattern ^= uint64(maskForFlippingEarlyZone) // This flipping converts surprising 0's to 1's.
		allSurprisesORed |= pattern
		for pattern != 0 {
			col := bits.TrailingZeros64(pattern)
			pattern ^= 1 << col // erase the 1.
			rowCol := (i << 6) | col
			isNovel, err := table.maybeInsert(rowCol)
			if err != nil {
				return nil, err
			}
			if !isNovel {
				return nil, fmt.Errorf("isNovel must be true")
			}
		}
	}

	// At this point we could shrink an oversize hash table, but the relative waste isn't very big.
	result.fiCol = bits.TrailingZeros64(allSurprisesORed)
	if result.fiCol > offset {
		result.fiCol = offset
	} // corner case

	// NB: the HIP-related fields will contain bogus values, but that is okay.

	result.mergeFlag = true
	return result, nil
	// end of case where union contains a bitMatrix
}

func (u *CpcUnion) checkUnionState() error {
	if u == nil {
		return fmt.Errorf("union cannot be nil")
	}

	if u.accumulator.lgK != 0 && u.bitMatrix != nil {
		return fmt.Errorf("accumulator and bitMatrix cannot be both valid or both nil")
	}
	if u.accumulator.lgK != 0 { // not nil
		if u.accumulator.numCoupons > 0 {
			if u.accumulator.slidingWindow != nil || u.accumulator.pairTable == nil {
				return fmt.Errorf("non-empty union accumulator must be SPARSE")
			}
		}
		if u.lgK != u.accumulator.lgK {
			return fmt.Errorf("union LgK must equal accumulator LgK")
		}
	}
	return nil
}

func (u *CpcUnion) reduceUnionK(newLgK int) error {
	if newLgK < u.lgK {
		if u.bitMatrix != nil {
			// downsample the union's bit matrix
			newK := 1 << newLgK
			newMatrix := make([]uint64, newK)
			orMatrixIntoMatrix(newMatrix, newLgK, u.bitMatrix, u.lgK)
			u.bitMatrix = newMatrix
			u.lgK = newLgK
		} else {
			// downsample the union's accumulator
			oldSketch := u.accumulator
			if oldSketch.numCoupons == 0 {
				acc, err := NewCpcSketch(newLgK, oldSketch.seed)
				if err != nil {
					return err
				}
				u.accumulator = acc
				u.lgK = newLgK
				return nil
			}
			sk, err := NewCpcSketch(newLgK, oldSketch.seed)
			if err != nil {
				return err
			}
			newSketch := sk
			if err := walkTableUpdatingSketch(newSketch, oldSketch.pairTable); err != nil {
				return err
			}
			finalNewFlavor := newSketch.GetFlavor()
			if finalNewFlavor == CpcFlavorSparse {
				u.accumulator = newSketch
				u.lgK = newLgK
				return nil
			}
			// the new sketch has graduated beyond sparse, so convert to bitMatrix
			//u.accumulator = nil
			u.bitMatrix = newSketch.bitMatrixOfSketch()
			u.lgK = newLgK
		}
	}
	return nil
}

func (u *CpcUnion) orWindowIntoMatrix(srcWindow []byte, srcOffset int, srcLgK int) {
	//assert(destLgK <= srcLgK)
	if u.lgK > srcLgK {
		panic("destLgK <= srcLgK")
	}
	destMask := (1 << u.lgK) - 1 // downsamples when destlgK < srcLgK
	srcK := 1 << srcLgK
	for srcRow := 0; srcRow < srcK; srcRow++ {
		u.bitMatrix[srcRow&destMask] |= (uint64(srcWindow[srcRow]) << srcOffset)
	}
}

func (u *CpcUnion) orTableIntoMatrix(srcTable *pairTable) {
	slots := srcTable.slotsArr
	numSlots := 1 << srcTable.lgSizeInts
	destMask := (1 << u.lgK) - 1 // downsamples when destlgK < srcLgK
	for i := 0; i < numSlots; i++ {
		rowCol := slots[i]
		if rowCol != -1 {
			col := rowCol & 63
			row := rowCol >> 6
			u.bitMatrix[row&destMask] |= (1 << col) // Set the bit.
		}

	}
}

func (u *CpcUnion) orMatrixIntoMatrix(srcMatrix []uint64, srcLgK int) {
	if u.lgK > srcLgK {
		panic("destLgK <= srcLgK")
	}
	destMask := (1 << u.lgK) - 1 // downsamples when destlgK < srcLgK
	srcK := 1 << srcLgK
	for srcRow := 0; srcRow < srcK; srcRow++ {
		u.bitMatrix[srcRow&destMask] |= srcMatrix[srcRow]
	}

}

func (u *CpcUnion) getNumCoupons() uint64 {
	if u.bitMatrix != nil {
		return countBitsSetInMatrix(u.bitMatrix)
	}
	return u.accumulator.numCoupons
}
