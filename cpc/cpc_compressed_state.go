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
)

type CpcCompressedState struct {
	CsvIsValid    bool
	WindowIsValid bool
	LgK           int
	SeedHash      int16
	FiCol         int
	MergeFlag     bool // compliment of HIP Flag
	NumCoupons    uint64

	Kxp         float64
	HipEstAccum float64

	NumCsv        uint64
	CsvStream     []int // may be longer than required
	CsvLengthInts int
	CwStream      []int // may be longer than required
	CwLengthInts  int
}

var (
	// This defines the preamble space required by each of the formats in units of 4-byte integers.
	preIntsDefs = []byte{2, 2, 4, 8, 4, 8, 6, 10}
)

func NewCpcCompressedState(lgK int, seedHash int16) *CpcCompressedState {
	return &CpcCompressedState{
		LgK:      lgK,
		SeedHash: seedHash,
		Kxp:      float64(int(1) << lgK),
	}
}

func NewCpcCompressedStateFromSketch(sketch *CpcSketch) (*CpcCompressedState, error) {
	seedHash, err := internal.ComputeSeedHash(int64(sketch.seed))
	if err != nil {
		return nil, err
	}
	state := NewCpcCompressedState(sketch.lgK, seedHash)
	state.FiCol = sketch.fiCol
	state.MergeFlag = sketch.mergeFlag
	state.NumCoupons = sketch.numCoupons
	state.Kxp = sketch.kxp
	state.HipEstAccum = sketch.hipEstAccum
	state.CsvIsValid = sketch.pairTable != nil
	state.WindowIsValid = sketch.slidingWindow != nil

	err = state.compress(sketch)
	return state, err
}

func (c *CpcCompressedState) getRequiredSerializedBytes() int {
	preInts := getDefinedPreInts(c.getFormat())
	return 4 * (preInts + c.CsvLengthInts + c.CwLengthInts)
}

func (c *CpcCompressedState) getWindowOffset() int {
	return determineCorrectOffset(c.LgK, c.NumCoupons)
}

func (c *CpcCompressedState) getFormat() CpcFormat {
	ordinal := 0
	if c.CwLengthInts > 0 {
		ordinal |= 4
	}
	if c.NumCsv > 0 {
		ordinal |= 2
	}
	if c.MergeFlag {
		ordinal |= 1
	}
	return CpcFormat(ordinal)
}

func (c *CpcCompressedState) uncompress(src *CpcSketch) error {
	srcFlavor := src.GetFlavor()
	switch srcFlavor {
	case CpcFlavorEmpty:
		return nil
	case CpcFlavorSparse:
		panic("not implemented")
		//return c.uncompressSparseFlavor(target)
	case CpcFlavorHybrid:
		panic("not implemented")
		//return c.uncompressHybridFlavor(target)
	case CpcFlavorPinned:
		panic("not implemented")
		//return c.uncompressPinnedFlavor(target)
	case CpcFlavorSliding:
		panic("not implemented")
		//return c.uncompressSlidingFlavor(target)
	default:
		return fmt.Errorf("unable to uncompress flavor %v", srcFlavor)
	}
}

func (c *CpcCompressedState) compress(src *CpcSketch) error {
	srcFlavor := src.GetFlavor()
	switch srcFlavor {
	case CpcFlavorEmpty:
		return nil
	case CpcFlavorSparse:
		panic("not implemented")
		//return c.uncompressSparseFlavor(target)
	case CpcFlavorHybrid:
		panic("not implemented")
		//return c.uncompressHybridFlavor(target)
	case CpcFlavorPinned:
		panic("not implemented")
		//return c.uncompressPinnedFlavor(target)
	case CpcFlavorSliding:
		panic("not implemented")
		//return c.uncompressSlidingFlavor(target)
	default:
		return fmt.Errorf("unable to compress flavor %v", srcFlavor)
	}
}

func importFromMemory(bytes []byte) (*CpcCompressedState, error) {
	if err := checkLoPreamble(bytes); err != nil {
		return nil, err
	}
	if !isCompressed(bytes) {
		return nil, fmt.Errorf("not compressed")
	}
	lgK := getLgK(bytes)
	seedHash := getSeedHash(bytes)
	state := NewCpcCompressedState(lgK, seedHash)
	fmtOrd := getFormatOrdinal(bytes)
	format := CpcFormat(fmtOrd)
	state.MergeFlag = (fmtOrd & 1) == 0
	state.CsvIsValid = (fmtOrd & 2) > 0
	state.WindowIsValid = (fmtOrd & 4) > 0

	switch format {
	case CpcformatEmptyMerged, CpcFormatEmptyHip:
		if err := checkCapacity(len(bytes), 8); err != nil {
			return nil, err
		}
	case CpcFormatSparseHybridMerged:
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = state.NumCoupons
		state.CsvLengthInts = getSvLengthInts(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CsvStream = getSvStream(bytes)
	case CpcFormatSparceHybridHip:
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = state.NumCoupons
		state.CsvLengthInts = getSvLengthInts(bytes)
		state.Kxp = getKxP(bytes)
		state.HipEstAccum = getHipAccum(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CsvStream = getSvStream(bytes)
	case CpcFormatPinnedSlidingMergedNosv:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
	case CpcFormatPinnedSlidingHipNosv:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		state.Kxp = getKxP(bytes)
		state.HipEstAccum = getHipAccum(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
	case CpcFormatPinnedSlidingMerged:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = getNumSV(bytes)
		state.CsvLengthInts = getSvLengthInts(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
		state.CsvStream = getSvStream(bytes)
	case CpcFormatPinnedSlidingHip:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = getNumSV(bytes)
		state.CsvLengthInts = getSvLengthInts(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		state.Kxp = getKxP(bytes)
		state.HipEstAccum = getHipAccum(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
		state.CsvStream = getSvStream(bytes)
	default:
		panic("not implemented")
	}
	return state, nil
}

func getDefinedPreInts(format CpcFormat) int {
	return int(preIntsDefs[format])
}
