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

type CpcFormat int
type CpcFlavor int

const (
	CpcformatEmptyMerged             CpcFormat = 0
	CpcFormatEmptyHip                CpcFormat = 1
	CpcFormatSparseHybridMerged      CpcFormat = 2
	CpcFormatSparceHybridHip         CpcFormat = 3
	CpcFormatPinnedSlidingMergedNosv CpcFormat = 4
	CpcFormatPinnedSlidingHipNosv    CpcFormat = 5
	CpcFormatPinnedSlidingMerged     CpcFormat = 6
	CpcFormatPinnedSlidingHip        CpcFormat = 7
)

const (
	CpcFlavorEmpty   CpcFlavor = 0 //    0  == C <    1
	CpcFlavorSparse  CpcFlavor = 1 //    1  <= C <   3K/32
	CpcFlavorHybrid  CpcFlavor = 2 // 3K/32 <= C <   K/2
	CpcFlavorPinned  CpcFlavor = 3 //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
	CpcFlavorSliding CpcFlavor = 4 // 27K/8 <= C
)

const (
	defaultLgK = 11
)

var (
	kxpByteLookup = makeKxpByteLookup()
)

func checkLgK(lgK int) error {
	if lgK < minLgK || lgK > maxLgK {
		return fmt.Errorf("LgK must be >= %d and <= %d: %d", minLgK, maxLgK, lgK)
	}
	return nil
}

func checkLgSizeInts(lgSizeInts int) error {
	if lgSizeInts < 2 || lgSizeInts > 26 {
		return fmt.Errorf("Illegal LgSizeInts: %d", lgSizeInts)
	}
	return nil
}

func checkSeeds(seedA uint64, seedB uint64) error {
	if seedA != seedB {
		return fmt.Errorf("Incompatible seeds: %d %d", seedA, seedB)
	}
	return nil
}

func determineFlavor(lgK int, numCoupons uint64) CpcFlavor {
	c := numCoupons
	k := uint64(1) << lgK
	c2 := c << 1
	c8 := c << 3
	c32 := c << 5
	if c == 0 {
		return CpcFlavorEmpty //    0  == C <    1
	}
	if c32 < (3 * k) {
		return CpcFlavorSparse //    1  <= C <   3K/32
	}
	if c2 < k {
		return CpcFlavorHybrid // 3K/32 <= C <   K/2
	}
	if c8 < (27 * k) {
		return CpcFlavorPinned //   K/2 <= C < 27K/8
	}
	return CpcFlavorSliding // 27K/8 <= C
}

func orMatrixIntoMatrix(destMatrix []uint64, destLgK int, srcMatrix []uint64, srcLgK int) {
	//assert(destLgK <= srcLgK)
	destMask := (1 << destLgK) - 1
	srcK := 1 << srcLgK
	for srcRow := 0; srcRow < srcK; srcRow++ {
		destMatrix[srcRow&destMask] |= srcMatrix[srcRow]
	}
}

func bitMatrixOfSketch(sketch CpcSketch) []uint64 {
	k := uint64(1) << sketch.lgK
	offset := sketch.windowOffset
	if offset < 0 || offset > 56 {
		panic("offset < 0 || offset > 56")
	}
	matrix := make([]uint64, k)
	if sketch.numCoupons == 0 {
		return matrix // Returning a matrix of zeros rather than NULL.
	}
	//Fill the matrix with default rows in which the "early zone" is filled with ones.
	//This is essential for the routine's O(k) time cost (as opposed to O(C)).
	defaultRow := (1 << offset) - 1
	for i := range matrix {
		matrix[i] = uint64(defaultRow)
	}
	if sketch.slidingWindow != nil { // In other words, we are in window mode, not sparse mode.
		for i, v := range sketch.slidingWindow { // set the window bits, trusting the sketch's current offset.
			matrix[i] |= (uint64(v) << offset)
		}
	}
	table := sketch.pairTable
	if table == nil {
		panic("table == nil")
	}
	slots := table.slotsArr
	numSlots := 1 << table.lgSizeInts
	for i := 0; i < numSlots; i++ {
		rowCol := slots[i]
		if rowCol != -1 {
			col := rowCol & 63
			row := rowCol >> 6
			// Flip the specified matrix bit from its default value.
			// In the "early" zone the bit changes from 1 to 0.
			// In the "late" zone the bit changes from 0 to 1.
			matrix[row] ^= (1 << col)
		}
	}
	return matrix
}

func countBitsSetInMatrix(matrix []uint64) uint64 {
	count := uint64(0)
	for _, v := range matrix {
		count += uint64(bits.OnesCount64(v))
	}
	return count
}

func determineCorrectOffset(lgK int, numCoupons uint64) int {
	c := numCoupons
	k := uint64(1) << lgK
	tmp := (c << 3) - (19 * k) // 8C - 19K
	if tmp < 0 {
		return 0
	}
	return int(tmp >> (lgK + 3)) // tmp / 8K

}

func walkTableUpdatingSketch(dest *CpcSketch, table *pairTable) error {
	slots := table.slotsArr
	numSlots := 1 << table.lgSizeInts
	destMask := ((1<<dest.lgK)-1)<<6 | 63 // downsamples when dest.lgK < srcLgK

	stride := int(internal.InverseGolden * float64(numSlots))
	if stride == (stride >> 1 << 1) {
		stride++
	}

	for i, j := 0, 0; i < numSlots; i, j = i+1, j+stride {
		j &= numSlots - 1
		rowCol := slots[j]
		if rowCol != -1 {
			if err := dest.rowColUpdate(rowCol & destMask); err != nil {
				return err
			}
		}

	}

	return nil
}

func makeKxpByteLookup() []float64 {
	lookup := make([]float64, 256)
	for b := 0; b < 256; b++ {
		sum := 0.0
		for col := 0; col < 8; col++ {
			bit := (b >> col) & 1
			if bit == 0 {
				sumI, _ := internal.InvPow2(col + 1)
				sum += sumI
			}
		}
		lookup[b] = sum
	}
	return lookup
}
