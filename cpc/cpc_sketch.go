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
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/twmb/murmur3"
	"math"
	"math/bits"
	"unsafe"
)

const (
	minLgK = 4
	maxLgK = 26
)

type CpcSketch struct {
	seed uint64

	//common variables
	lgK        int
	numCoupons uint64 // The number of coupons collected so far.
	mergeFlag  bool   // Is the sketch the result of merging?
	fiCol      int    // First Interesting Column. This is part of a speed optimization.

	windowOffset  int
	slidingWindow []byte     //either null or size K bytes
	pairTable     *pairTable //for sparse and surprising values, either null or variable size

	//The following variables are only valid in HIP varients
	kxp         float64 //used with HIP
	hipEstAccum float64 //used with HIP

	scratch [8]byte
}

func NewCpcSketchWithDefault(lgK int) (CpcSketch, error) {
	return NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
}

func NewCpcSketch(lgK int, seed uint64) (CpcSketch, error) {
	if err := checkLgK(lgK); err != nil {
		return CpcSketch{}, err
	}

	return CpcSketch{
		lgK:  lgK,
		seed: seed,
		kxp:  float64(int64(1) << lgK),
	}, nil
}

func (c *CpcSketch) GetEstimate() float64 {
	if c.mergeFlag {
		return iconEstimate(c.lgK, c.numCoupons)
	}
	return c.hipEstAccum
}

func (c *CpcSketch) GetLowerBound(kappa int) float64 {
	if c.mergeFlag {
		return iconConfidenceLB(c.lgK, c.numCoupons, kappa)
	}
	return hipConfidenceLB(c.lgK, c.numCoupons, c.hipEstAccum, kappa)
}

func (c *CpcSketch) GetUpperBound(kappa int) float64 {
	if c.mergeFlag {
		return iconConfidenceUB(c.lgK, c.numCoupons, kappa)
	}
	return hipConfidenceUB(c.lgK, c.numCoupons, c.hipEstAccum, kappa)
}

func (c *CpcSketch) UpdateUint64(datum uint64) error {
	binary.LittleEndian.PutUint64(c.scratch[:], datum)
	hashLo, hashHi := hash(c.scratch[:], c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64(datum int64) error {
	return c.UpdateUint64(uint64(datum))
}

func (c *CpcSketch) UpdateFloat64(datum float64) error {
	binary.LittleEndian.PutUint64(c.scratch[:], math.Float64bits(datum))
	hashLo, hashHi := hash(c.scratch[:], c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64Slice(datum []int64) error {
	if len(datum) == 0 {
		return nil
	}
	hashLo, hashHi := internal.HashInt64SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt32Slice(datum []int32) error {
	if len(datum) == 0 {
		return nil
	}
	hashLo, hashHi := internal.HashInt32SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateByteSlice(datum []byte) error {
	if len(datum) == 0 {
		return nil
	}
	hashLo, hashHi := murmur3.SeedSum128(c.seed, c.seed, datum)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateString(datum string) error {
	if len(datum) == 0 {
		return nil
	}
	// get a slice to the string data (avoiding a copy to heap)
	return c.UpdateByteSlice(unsafe.Slice(unsafe.StringData(datum), len(datum)))
}

func (c *CpcSketch) hashUpdate(hash0, hash1 uint64) error {
	col := bits.LeadingZeros64(hash1)
	if col < c.fiCol {
		return nil // important speed optimization
	}
	if col > 63 {
		col = 63 // clip so that 0 <= col <= 63
	}
	if c.numCoupons == 0 {
		err := c.promoteEmptyToSparse()
		if err != nil {
			return err
		}
	}
	k := uint64(1) << c.lgK
	row := int(hash0 & (k - 1))
	rowCol := (row << 6) | col

	// Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
	// to the pair (2^26 - 2, 63), which effectively merges the two cells.
	// This case is *extremely* unlikely, but we might as well handle it.
	// It can't happen at all if lgK (or maxLgK) < 26.
	if rowCol == -1 {
		rowCol ^= 1 << 6 //set the LSB of row to 0
	}

	if (c.numCoupons << 5) < (uint64(3) * k) {
		return c.updateSparse(rowCol)
	}
	return c.updateWindowed(rowCol)
}

func (c *CpcSketch) promoteEmptyToSparse() error {
	pairTable, err := NewPairTable(2, 6+c.lgK)
	if err != nil {
		return err
	}
	c.pairTable = pairTable
	return nil
}

func (c *CpcSketch) updateSparse(rowCol int) error {
	k := uint64(1) << c.lgK
	c32pre := c.numCoupons << 5
	if c32pre >= (3 * k) {
		// C >= 3K/32, in other words, flavor == SPARSE
		return fmt.Errorf("C >= 3K/32")
	}
	if c.pairTable == nil {
		return fmt.Errorf("pairTable is nil")
	}
	isNovel, err := c.pairTable.maybeInsert(rowCol)
	if err != nil {
		return err
	}
	if isNovel {
		c.numCoupons++
		c.updateHIP(rowCol)
		c32post := c.numCoupons << 5
		if c32post >= (3 * k) {
			c.promoteSparseToWindowed() // C >= 3K/32
		}
	}
	return nil
}

func (c *CpcSketch) updateWindowed(rowCol int) error {
	if c.windowOffset < 0 || c.windowOffset > 56 {
		return fmt.Errorf("windowOffset < 0 || windowOffset > 56")
	}
	k := uint64(1) << c.lgK
	c32pre := c.numCoupons << 5
	if c32pre < (3 * k) {
		return fmt.Errorf("C < 3K/32")
	}
	c8pre := uint64(c.numCoupons << 3)
	w8pre := uint64(c.windowOffset << 3)
	if c8pre >= ((uint64(27) + w8pre) * k) {
		return fmt.Errorf("C >= (K * 27/8) + (K * windowOffset)")
	}

	isNovel := false //novel if new coupon
	err := error(nil)
	col := rowCol & 63

	if col < c.windowOffset { // track the surprising 0's "before" the window
		isNovel, err = c.pairTable.maybeDelete(rowCol)
		if err != nil {
			return err
		}
	} else if col < (c.windowOffset + 8) { // track the 8 bits inside the window
		row := rowCol >> 6
		oldBits := c.slidingWindow[row]
		newBits := oldBits | (1 << (col - c.windowOffset))
		if newBits != oldBits {
			c.slidingWindow[row] = newBits
			isNovel = true
		}
	} else { // track the surprising 1's "after" the window
		isNovel, err = c.pairTable.maybeInsert(rowCol)
		if err != nil {
			return err
		}
	}

	if isNovel {
		c.numCoupons++
		c.updateHIP(rowCol)
		c8post := c.numCoupons << 3
		if c8post >= ((27 + w8pre) * k) {
			if err := c.modifyOffset(c.windowOffset + 1); err != nil {
				return err
			}
			if c.windowOffset < 1 || c.windowOffset > 56 {
				return fmt.Errorf("windowOffset < 1 || windowOffset > 56")
			}
			w8post := uint64(c.windowOffset << 3)
			if c8post >= ((uint64(27) + w8post) * k) {
				return fmt.Errorf("C < (K * 27/8) + (K * windowOffset)")
			}
		}

	}
	return nil
}

func hash(bs []byte, seed uint64) (uint64, uint64) {
	return murmur3.SeedSum128(seed, seed, bs)
}

func (c *CpcSketch) getFormat() CpcFormat {
	ordinal := 0
	f := c.GetFlavor()
	if f == CpcFlavorHybrid || f == CpcFlavorSparse {
		ordinal = 2
		if !c.mergeFlag {
			ordinal |= 1
		}
	} else {
		ordinal = 0
		if c.slidingWindow != nil {
			ordinal |= 4
		}
		if c.pairTable != nil && c.pairTable.numPairs > 0 {
			ordinal |= 2
		}
		if !c.mergeFlag {
			ordinal |= 1
		}
	}
	return CpcFormat(ordinal)
}

func (c *CpcSketch) GetFlavor() CpcFlavor {
	return determineFlavor(c.lgK, c.numCoupons)
}

func (c *CpcSketch) updateHIP(rowCol int) {
	k := 1 << c.lgK
	col := rowCol & 63
	oneOverP := float64(k) / c.kxp
	c.hipEstAccum += oneOverP
	kxp, _ := internal.InvPow2(col + 1)
	c.kxp -= kxp
}

func (c *CpcSketch) promoteSparseToWindowed() {
	window := make([]byte, 1<<c.lgK)
	newTable, _ := NewPairTable(2, 6+c.lgK)
	oldTable := c.pairTable

	oldSlots := oldTable.slotsArr
	oldNumSlots := 1 << oldTable.lgSizeInts

	for i := 0; i < oldNumSlots; i++ {
		rowCol := oldSlots[i]
		if rowCol != -1 {
			col := rowCol & 63
			if col < 8 {
				row := rowCol >> 6
				window[row] |= 1 << col
			} else {
				newTable.mustInsert(rowCol)
			}
		}
	}

	c.slidingWindow = window
	c.pairTable = newTable
}

func (c *CpcSketch) reset() {
	c.numCoupons = 0
	c.mergeFlag = false
	c.fiCol = 0
	c.windowOffset = 0
	c.slidingWindow = nil
	c.pairTable = nil
	c.kxp = float64(int64(1) << c.lgK)
	c.hipEstAccum = 0
}

func (c *CpcSketch) rowColUpdate(rowCol int) error {
	col := rowCol & 63
	if col < c.fiCol {
		return nil
	}
	if c.numCoupons == 0 {
		err := c.promoteEmptyToSparse()
		if err != nil {
			return err
		}
	}
	k := uint64(1) << c.lgK
	if (c.numCoupons << 5) < (3 * k) {
		return c.updateSparse(rowCol)
	} else {
		// TODO(pierre)
		// return c.updateWindowed(rowCol)
	}
	return nil
}

func (c *CpcSketch) modifyOffset(newOffset int) error {
	if newOffset < 0 || newOffset > 56 {
		return fmt.Errorf("newOffset < 0 || newOffset > 56")
	}
	if newOffset != (c.windowOffset + 1) {
		return fmt.Errorf("newOffset != (c.windowOffset + 1)")
	}
	if c.slidingWindow == nil || c.pairTable == nil {
		return fmt.Errorf("slidingWindow == nil || pairTable == nil")
	}
	k := 1 << c.lgK
	bitMatrix := bitMatrixOfSketch(*c)
	if (newOffset & 0x7) == 0 {
		c.refreshKXP(bitMatrix)
	}
	c.pairTable.clear()
	maskForClearingWindow := (0xFF << newOffset) ^ -1
	maskForFlippingEarlyZone := (1 << newOffset) - 1
	allSurprisesORed := uint64(0)
	for i := 0; i < k; i++ {
		pattern := bitMatrix[i]
		c.slidingWindow[i] = byte((pattern >> newOffset) & 0xFF)
		pattern &= uint64(maskForClearingWindow)
		pattern ^= uint64(maskForFlippingEarlyZone)
		allSurprisesORed |= pattern
		for pattern != 0 {
			col := bits.TrailingZeros64(pattern)
			pattern ^= 1 << col
			rowCol := (i << 6) | col
			isNovel, err := c.pairTable.maybeInsert(rowCol)
			if err != nil {
				return err
			}
			if !isNovel {
				return nil
			}
		}
	}
	c.windowOffset = newOffset
	c.fiCol = bits.TrailingZeros64(allSurprisesORed)
	if c.fiCol > newOffset {
		c.fiCol = newOffset
	}
	return nil
}

func (c *CpcSketch) refreshKXP(bitMatrix []uint64) {
	k := 1 << c.lgK
	byteSums := make([]float64, 8)
	for i := 0; i < k; i++ {
		row := bitMatrix[i]
		for j := 0; j < 8; j++ {
			byteIdx := int(row & 0xFF)
			byteSums[j] += kxpByteLookup[byteIdx]
			row >>= 8
		}
	}
	total := 0.0
	for j := 7; j >= 0; j-- {
		factor, _ := internal.InvPow2(8 * j)
		total += factor * byteSums[j]
	}
	c.kxp = total
}
