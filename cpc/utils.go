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
	loFieldPreInts = iota
	loFieldSerVer
	loFieldFamily
	loFieldLgK
	loFieldFiCol
	LoFieldFlags
	loFieldSeedHash
)

const (
	// Preamble hi field definitions
	// This defines the eight additional preamble fields located after the <i>LoField</i>.
	// Do not change the order.
	//
	// Note: NUM_SV has dual meanings: In sparse and hybrid flavors it is equivalent to
	// numCoupons so it isn't stored separately. In pinned and sliding flavors is is the
	// numSV of the PairTable, which stores only surprising values.

	hiFieldNumCoupons = iota
	hiFieldNumSV
	hiFieldKXP
	hiFieldHipAccum
	hiFieldSVLengthInts
	hiFieldWLengthInts
	hiFieldSVStream
	hiFieldWStream
)

var (
	// This defines the byte offset for each of the 8 <i>HiFields</i>
	// given the Format ordinal (1st dimension) and the HiField ordinal (2nd dimension).
	hiFieldOffset = [8][8]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{8, 0, 0, 0, 12, 0, 16, 0},
		{8, 0, 16, 24, 12, 0, 32, 0},
		{8, 0, 0, 0, 0, 12, 0, 16},
		{8, 0, 16, 24, 0, 12, 0, 32},
		{8, 12, 0, 0, 16, 20, 24, 24},   //the 2nd 24 is not used.
		{8, 12, 16, 24, 32, 36, 40, 40}, //the 2nd 40 is not used.
	}
)

const (
	serVer = 1

	// Flags bit masks, Byte 5
	bigEndianFlagMask  = 1  // Reserved.
	compressedFlagMask = 2  // Compressed Flag
	hipFlagMask        = 4  // HIP Flag
	supValFlagMask     = 8  // num Suprising Values > 0
	windowFlagMask     = 16 // window length > 0

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

func countBitsSetInMatrix(matrix []uint64) uint64 {
	count := uint64(0)
	for _, v := range matrix {
		count += uint64(bits.OnesCount64(v))
	}
	return count
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

func checkLoPreamble(bytes []byte) error {
	if err := checkBounds(0, 8, len(bytes)); err != nil {
		return err
	}
	if bytes[loFieldSerVer] != (serVer & 0xFF) {
		return fmt.Errorf("SerVer: %d, bytes[loFieldSerVer]: %d", serVer&0xFF, bytes[loFieldSerVer])
	}
	fmat := getFormat(bytes)
	preIntsDef := preIntsDefs[fmat] & 0xFF
	if bytes[loFieldPreInts] != preIntsDef {
		return fmt.Errorf("preIntsDef: %d, bytes[loFieldPreInts]: %d", preIntsDef, bytes[loFieldPreInts])
	}
	fam := getFamilyId(bytes)
	if fam != internal.FamilyEnum.CPC.Id {
		return fmt.Errorf("Family: %d, bytes[loFieldFamily]: %d", internal.FamilyEnum.CPC.Id, fam)
	}
	lgK := getLgK(bytes)
	if lgK < 4 || lgK > 26 {
		return fmt.Errorf("lgK: %d", lgK)
	}
	fiCol := bytes[loFieldFiCol] & 0xFF
	if fiCol > 63 {
		return fmt.Errorf("fiCol: %d", fiCol)
	}
	return nil
}

func checkBounds(reqOff, reqLen, allocSize int) error {
	if reqOff < 0 || reqLen < 0 || reqOff+reqLen < 0 || allocSize-(reqOff+reqLen) < 0 {
		return fmt.Errorf("bounds Violation: reqOffset: %d, reqLength: %d, (reqOff + reqLen): %d, allocSize: %d", reqOff, reqLen, reqOff+reqLen, allocSize)
	}
	return nil
}

func checkCapacity(memCap, expectedCap int) error {
	if memCap < expectedCap {
		return fmt.Errorf("Insufficient Image Bytes = %d, Expected = %d", memCap, expectedCap)
	}
	return nil
}

func isCompressed(bytes []byte) bool {
	return (getFlags(bytes) & compressedFlagMask) > 0
}

func getFamilyId(bytes []byte) int {
	return int(bytes[loFieldFamily] & 0xFF)
}

func getLgK(bytes []byte) int {
	return int(bytes[loFieldLgK] & 0xFF)
}

func getSeedHash(bytes []byte) int16 {
	return int16(bytes[loFieldSeedHash])
}

func getFormat(bytes []byte) CpcFormat {
	ord := getFormatOrdinal(bytes)
	return CpcFormat(ord)
}

func getFormatOrdinal(bytes []byte) int {
	flags := getFlags(bytes)
	return (flags >> 2) & 0x7
}

func getFlags(bytes []byte) int {
	return int(bytes[LoFieldFlags] & 0xFF)
}

func getNumCoupons(bytes []byte) uint64 {
	offset := getHiFieldOffset(getFormat(bytes), hiFieldNumCoupons)
	return uint64(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
}

func getSvLengthInts(bytes []byte) int {
	offset := getHiFieldOffset(getFormat(bytes), hiFieldSVLengthInts)
	return int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
}

func getSvStream(bytes []byte) []int {
	offset := getHiFieldOffset(getFormat(bytes), hiFieldSVStream)
	svLengthInts := getSvLengthInts(bytes)
	svStream := make([]int, svLengthInts)
	for i := 0; i < svLengthInts; i++ {
		svStream[i] = int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
		offset += 4
	}
	return svStream
}

// getHiFieldOffset returns the defined byte offset from the start of the preamble given the Format and the HiField.
// Note this can not be used to obtain the stream offsets.
func getHiFieldOffset(format CpcFormat, hiField int) int {
	return int(hiFieldOffset[format][hiField])
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
