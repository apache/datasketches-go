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

func NewCpcSketch(lgK int, seed uint64) (*CpcSketch, error) {
	if err := checkLgK(lgK); err != nil {
		return nil, err
	}

	return &CpcSketch{
		lgK:  lgK,
		seed: seed,
		kxp:  float64(int64(1) << lgK),
	}, nil
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

func (c *CpcSketch) UpdateSlice(datum []byte) error {
	hashLo, hashHi := hash(datum, c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64Slice(datum []int64) error {
	hashLo, hashHi := internal.HashInt64SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateString(datum string) error {
	// get a slice to the string data (avoiding a copy to heap)
	return c.UpdateSlice(unsafe.Slice(unsafe.StringData(datum), len(datum)))
}

func (c *CpcSketch) hashUpdate(hash0, hash1 uint64) error {
	col := 64 - bits.LeadingZeros64(hash1)
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
	} else {
		// TODO(pierre)
		// return c.updateWindowed(rowCol)
	}
	return nil
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
		// TODO (pierre)
		//c.updateHIP(rowCol)
		c32post := c.numCoupons << 5
		if c32post >= (3 * k) {
			// TODO (pierre)
			// c.promoteSparseToWindowed() // C >= 3K/32
		}

	}
	return nil
}

func hash(bs []byte, seed uint64) (uint64, uint64) {
	return murmur3.SeedSum128(seed, seed, bs)
}

func (c *CpcSketch) getFormat() cpcFormat {
	ordinal := 0
	f := c.getFlavor()
	if f == flavor_hybrid || f == flavor_sparse {
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
	return cpcFormat(ordinal)
}

func (c *CpcSketch) getFlavor() cpcFlavor {
	return determineFlavor(c.lgK, c.numCoupons)
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
